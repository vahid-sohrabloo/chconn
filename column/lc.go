package column

import (
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

const (
	// Need to read additional keys.
	// Additional keys are stored before indexes as value N and N keys
	// after them.
	hasAdditionalKeysBit = 1 << 9
	// Need to update dictionary.
	// It means that previous granule has different dictionary.
	needUpdateDictionary = 1 << 10

	serializationType = hasAdditionalKeysBit | needUpdateDictionary
)

// LowCardinality use for LowCardinality ClickHouse DataTypes
type LowCardinality[T comparable] struct {
	column
	numRow         int
	dictColumn     Column[T]
	indices        indicesColumnI
	oldIndicesType int
	scratch        [8]byte
	readDict       []T
	dict           map[T]int
	keys           []int
	nullable       bool
	rtype          reflect.Type
}

// NewLowCardinality return new LC for LowCardinality ClickHouse DataTypes
func NewLowCardinality[T comparable](dictColumn Column[T]) *LowCardinality[T] {
	return NewLC(dictColumn)
}

// NewLC return new LC for LowCardinality ClickHouse DataTypes
func NewLC[T comparable](dictColumn Column[T]) *LowCardinality[T] {
	l := &LowCardinality[T]{
		dict:       make(map[T]int),
		dictColumn: dictColumn,
		rtype:      reflect.TypeOf((*T)(nil)).Elem(),
	}
	return l
}

// Data get all the data in current block as a slice.
//
// NOTE: the return slice only valid in current block, if you want to use it after, you should copy it. or use Read
func (c *LowCardinality[T]) Data() []T {
	result := make([]T, c.NumRow())
	for i, k := range c.keys {
		result[i] = c.readDict[k]
	}
	return result
}

// Read reads all the data in current block and append to the input.
func (c *LowCardinality[T]) Read(value []T) []T {
	for _, k := range c.keys {
		value = append(value, c.readDict[k])
	}
	return value
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *LowCardinality[T]) Row(row int) T {
	return c.readDict[c.keys[row]]
}

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *LowCardinality[T]) RowAny(row int) any {
	return c.Row(row)
}

func (c *LowCardinality[T]) Scan(row int, dest any) error {
	return c.dictColumn.Scan(c.keys[row], dest)
}

// Append value for insert
func (c *LowCardinality[T]) Append(v T) {
	c.preHookAppend()
	key, ok := c.dict[v]
	if !ok {
		key = len(c.dict)
		c.dict[v] = key
		c.dictColumn.Append(v)
	}
	c.keys = append(c.keys, key)
	c.numRow++
}

func (c *LowCardinality[T]) canAppend(value any) bool {
	if _, ok := value.(T); ok {
		return true
	}
	return false
}

func (c *LowCardinality[T]) AppendAny(value any) error {
	if v, ok := value.(T); ok {
		c.Append(v)
		return nil
	}

	return fmt.Errorf("could not convert %v of type %T to type %T", value, value, value)
}

// AppendMulti value for insert
func (c *LowCardinality[T]) AppendMulti(v ...T) {
	c.preHookAppendMulti(len(v))
	for _, v := range v {
		key, ok := c.dict[v]
		if !ok {
			key = len(c.dict)
			c.dict[v] = key
			c.dictColumn.Append(v)
		}
		c.keys = append(c.keys, key)
	}
	c.numRow += len(v)
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *LowCardinality[T]) Remove(n int) {
	if c.NumRow() == 0 || c.NumRow() <= n {
		return
	}
	c.keys = c.keys[:n]
	c.numRow = len(c.keys)
}

func (c *LowCardinality[T]) Delete(start int, end int) {
	if c.NumRow() == 0 || c.NumRow() <= start {
		return
	}
	if end > c.NumRow() {
		end = c.NumRow()
	}
	if start >= end {
		return
	}
	c.keys = slices.Delete(c.keys, start, end)
	c.numRow = len(c.keys)
}

func (c *LowCardinality[T]) DeleteFunc(del func(row int) bool) {
	if c.NumRow() == 0 {
		return
	}
	i := 0
	for j := 0; j < len(c.keys); j++ {
		if !del(j) {
			c.keys[i] = c.keys[j]
			i++
		}
	}
	clear(c.keys[i:]) // zero/nil out the obsolete elements, for GC
	c.keys = c.keys[:i]
	c.numRow = len(c.keys)
}

// Dicts get dictionary data
// each key is an index of the dictionary
func (c *LowCardinality[T]) Dicts() []T {
	return c.readDict
}

// Keys get keys of data
// each key is an index of the dictionary
func (c *LowCardinality[T]) Keys() []int {
	return c.keys
}

// NumRow return number of row for this block
func (c *LowCardinality[T]) NumRow() int {
	return c.numRow
}

// Array return a Array type for this column
func (c *LowCardinality[T]) Array() *Array[T] {
	return NewArray[T](c)
}

// Nullable return a Nullable type for this column
func (c *LowCardinality[T]) Nullable() *LowCardinalityNullable[T] {
	return NewLowCardinalityNullable(c.dictColumn)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *LowCardinality[T]) Reset() {
	c.dictColumn.Reset()
	clear(c.dict)
	c.keys = c.keys[:0]
	c.readDict = c.readDict[:0]
	c.numRow = 0
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *LowCardinality[T]) SetWriteBufferSize(row int) {
	if cap(c.keys) < row {
		c.keys = make([]int, 0, row)
	}
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *LowCardinality[T]) ReadRaw(num int) error {
	c.numRow = num
	if c.numRow == 0 {
		c.indices = newIndicesColumn[uint8](c.r)
		c.readDict = c.readDict[:0]
		c.keys = c.keys[:0]
		// to reset nullable dictionary
		return c.dictColumn.ReadRaw(0)
	}

	serializationType, err := c.r.Uint64()
	if err != nil {
		return fmt.Errorf("error reading serialization type: %w", err)
	}
	intType := int(serializationType & 0xf)

	dictionarySize, err := c.r.Uint64()
	if err != nil {
		return fmt.Errorf("error reading dictionary size: %w", err)
	}
	err = c.dictColumn.ReadRaw(int(dictionarySize))
	if err != nil {
		return fmt.Errorf("error reading dictionary: %w", err)
	}

	indicesSize, err := c.r.Uint64()
	c.numRow = int(indicesSize)
	if err != nil {
		return fmt.Errorf("error reading indices size: %w", err)
	}
	if c.indices == nil || c.oldIndicesType != intType {
		c.indices = getLCIndicate(intType, c.r)
		c.oldIndicesType = intType
	}
	err = c.indices.ReadRaw(c.numRow)
	if err != nil {
		return fmt.Errorf("error reading indices: %w", err)
	}
	c.readDict = c.readDict[:0]
	if cap(c.keys) < c.numRow {
		c.keys = make([]int, 0, c.numRow)
	} else {
		c.keys = c.keys[:0]
	}
	c.readDict = c.dictColumn.Read(c.readDict)
	c.indices.readInt(&c.keys)
	return nil
}

// ReadHeader writes header data to writer
// it uses internally
func (c *LowCardinality[T]) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	err := c.column.ReadHeader(r, serverInfo)
	if err != nil {
		return err
	}

	// ready KeysSerializationVersion.
	_, err = c.r.Uint64()
	if err != nil {
		return fmt.Errorf("error reading keys serialization version: %w", err)
	}

	return c.dictColumn.ReadHeader(r, serverInfo)
}

func (c *LowCardinality[T]) chconnType() string {
	if c.nullable {
		return "column.LowCardinalityNullable[" + reflect.TypeOf((*T)(nil)).Elem().String() + "]"
	}
	return "column.LowCardinality[" + reflect.TypeOf((*T)(nil)).Elem().String() + "]"
}

func (c *LowCardinality[T]) structType() string {
	if !c.nullable {
		return strings.ReplaceAll(helper.LowCardinalityTypeStr, "<type>", c.dictColumn.structType())
	}
	return strings.ReplaceAll(helper.LowCardinalityNullableTypeStr, "<type>", c.dictColumn.structType())
}

func (c *LowCardinality[T]) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	chType := helper.FilterSimpleAggregate(c.columnHeader.ChType)
	if !c.nullable {
		if !helper.IsLowCardinality(chType) {
			return &ErrInvalidType{
				chType:     string(c.columnHeader.ChType),
				chconnType: c.chconnType(),
				goToChType: c.structType(),
			}
		}
		if err := c.dictColumn.SetColumnHeader(ColumnHeader{
			ChType: chType[helper.LenLowCardinalityStr : len(chType)-1],
		}); err != nil {
			if !isInvalidType(err) {
				return err
			}
			return &ErrInvalidType{
				chType:     string(c.columnHeader.ChType),
				chconnType: c.chconnType(),
				goToChType: c.structType(),
			}
		}
	} else {
		if !helper.IsNullableLowCardinality(chType) {
			return &ErrInvalidType{
				chType:     string(c.columnHeader.ChType),
				chconnType: c.chconnType(),
				goToChType: c.structType(),
			}
		}
		if err := c.dictColumn.SetColumnHeader(ColumnHeader{
			ChType: chType[helper.LenLowCardinalityNullableStr : len(chType)-2],
		}); err != nil {
			if !isInvalidType(err) {
				return err
			}
			return &ErrInvalidType{
				chType:     string(c.columnHeader.ChType),
				chconnType: c.chconnType(),
				goToChType: c.structType(),
			}
		}
	}
	return nil
}

func (c *LowCardinality[T]) ValidateInsert() error {
	return c.dictColumn.ValidateInsert()
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *LowCardinality[T]) WriteTo(w io.Writer) (int64, error) {
	dictionarySize := c.dictColumn.NumRow()
	// Do not write anything for empty column.
	// May happen while writing empty arrays.
	if dictionarySize == 0 || (c.nullable && dictionarySize == 1) {
		return 0, nil
	}
	var n int64
	intType := int(math.Log2(float64(dictionarySize)) / 8)
	stype := serializationType | intType

	nw, err := c.writeUint64(w, uint64(stype))
	n += int64(nw)
	if err != nil {
		return n, fmt.Errorf("error writing stype: %w", err)
	}

	nw, err = c.writeUint64(w, uint64(dictionarySize))
	n += int64(nw)
	if err != nil {
		return n, fmt.Errorf("error writing dictionarySize: %w", err)
	}

	nwd, err := c.dictColumn.WriteTo(w)
	n += nwd
	if err != nil {
		return n, fmt.Errorf("error writing dictionary: %w", err)
	}

	nw, err = c.writeUint64(w, uint64(len(c.keys)))
	n += int64(nw)
	if err != nil {
		return n, fmt.Errorf("error writing keys len: %w", err)
	}
	if c.indices == nil || c.oldIndicesType != intType {
		c.indices = getLCIndicate(intType, nil)
		c.oldIndicesType = intType
	}
	c.indices.setKeys(c.keys)
	nwt, err := c.indices.WriteTo(w)
	if err != nil {
		return n, fmt.Errorf("error writing indices: %w", err)
	}
	return n + nwt, err
}

// HeaderWriter reader header data
// it uses internally
func (c *LowCardinality[T]) HeaderWriter(w *readerwriter.Writer) {
	// write KeysSerializationVersion. for more information see clickhouse docs
	w.Uint64(1)
}

func getLCIndicate(intType int, r *readerwriter.Reader) indicesColumnI {
	switch intType {
	case 0:
		return newIndicesColumn[uint8](r)
	case 1:
		return newIndicesColumn[uint16](r)
	case 2:
		return newIndicesColumn[uint32](r)
	case 3:
		panic("cannot handle this amount of data for lc")
	}
	// this should never happen unless something wrong with the code
	panic("cannot not find indicate type")
}

func (c *LowCardinality[T]) writeUint64(w io.Writer, v uint64) (int, error) {
	c.scratch[0] = byte(v)
	c.scratch[1] = byte(v >> 8)
	c.scratch[2] = byte(v >> 16)
	c.scratch[3] = byte(v >> 24)
	c.scratch[4] = byte(v >> 32)
	c.scratch[5] = byte(v >> 40)
	c.scratch[6] = byte(v >> 48)
	c.scratch[7] = byte(v >> 56)
	return w.Write(c.scratch[:8])
}

func (c *LowCardinality[T]) elem(arrayLevel int, nullable bool) ColumnCore {
	if nullable {
		return c.Nullable().elem(arrayLevel)
	}
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *LowCardinality[T]) FullType() string {
	if len(c.columnHeader.Name) == 0 {
		return "LowCardinality(" + c.dictColumn.FullType() + ")"
	}
	return string(c.columnHeader.Name) + " LowCardinality(" + c.dictColumn.FullType() + ")"
}

func (c *LowCardinality[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	return c.dictColumn.ToJSON(c.keys[row], ignoreDoubleQuotes, b)
}

func (c *LowCardinality[T]) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexLowCardinality))
	c.dictColumn.writeBinaryDataTo(w)
}
