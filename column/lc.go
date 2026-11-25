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
	numRow               int
	dictColumn           Column[T]
	indices              indicesColumnI
	oldIndicesType       int
	scratch              [8]byte
	readDict             []T
	dict                 map[T]uint32
	keys                 []uint32
	nullable             bool
	rtype                reflect.Type
	indexRemoveKeepIndex int
}

// NewLowCardinality return new LC for LowCardinality ClickHouse DataTypes
func NewLowCardinality[T comparable](dictColumn Column[T]) *LowCardinality[T] {
	return NewLC(dictColumn)
}

// NewLC return new LC for LowCardinality ClickHouse DataTypes
func NewLC[T comparable](dictColumn Column[T]) *LowCardinality[T] {
	l := &LowCardinality[T]{
		dict:       make(map[T]uint32),
		dictColumn: dictColumn,
		rtype:      reflect.TypeFor[T](),
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
	return c.dictColumn.Scan(int(c.keys[row]), dest)
}

// Append value for insert
func (c *LowCardinality[T]) Append(v T) {
	c.preHookAppend()
	key := c.dictKey(v)
	c.keys = append(c.keys, key)
	c.numRow++
}

// dictKey returns the dictionary key of v, adding v to the dictionary if needed.
// The dictionary column can store v in a normalized form (FixedString pads it),
// so keys are by the stored form and a value never gets two dictionary entries.
func (c *LowCardinality[T]) dictKey(v T) uint32 {
	if key, ok := c.dict[v]; ok {
		return key
	}
	c.dictColumn.Append(v)
	return c.addLastDictRow()
}

// dictKeyAny is dictKey for a value the dictionary column accepts through AppendAny,
// such as a [N]byte array for a FixedString dictionary.
func (c *LowCardinality[T]) dictKeyAny(value any) (uint32, error) {
	if err := c.dictColumn.AppendAny(value); err != nil {
		return 0, err
	}
	return c.addLastDictRow(), nil
}

// addLastDictRow returns the key of the value just appended to the dictionary column,
// and removes that row again if its stored form already has a key.
func (c *LowCardinality[T]) addLastDictRow() uint32 {
	last := c.dictColumn.NumRow() - 1
	// The map key is the stored value, not the input: the input can alias a caller buffer.
	stored := c.dictColumn.Row(last)
	if key, ok := c.dict[stored]; ok {
		c.dictColumn.Remove(last)
		return key
	}
	key := uint32(last)
	if c.nullable {
		key--
	}
	c.readDict = append(c.readDict, stored)
	c.dict[stored] = key
	return key
}

// Set value at given row
//
// NOTE: Row number start from zero
func (c *LowCardinality[T]) SetAt(row int, v T) {
	if row >= c.NumRow() {
		return
	}

	key := c.dictKey(v)
	c.keys[row] = key
}

func (c *LowCardinality[T]) canAppend(value any) bool {
	return c.dictColumn.canAppend(value)
}

func (c *LowCardinality[T]) AppendAny(value any) error {
	if v, ok := value.(T); ok {
		c.Append(v)
		return nil
	}
	if key, err := c.dictKeyAny(value); err == nil {
		c.preHookAppend()
		c.keys = append(c.keys, key)
		c.numRow++
		return nil
	}

	return fmt.Errorf("could not convert %v of type %T to %s", value, value, c.chconnType())
}

// AppendMulti value for insert
func (c *LowCardinality[T]) AppendMulti(v ...T) {
	c.preHookAppendMulti(len(v))
	for _, v := range v {
		key := c.dictKey(v)
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

func (c *LowCardinality[T]) Delete(start, end int) {
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

func (c *LowCardinality[T]) startBatchDelete() {
	c.indexRemoveKeepIndex = 0
}

func (c *LowCardinality[T]) batchDeleteKeep(start, end int) {
	for i := start; i < end; i++ {
		c.keys[c.indexRemoveKeepIndex] = c.keys[i]
		c.indexRemoveKeepIndex++
	}
}

func (c *LowCardinality[T]) endBatchDelete() {
	clear(c.keys[c.indexRemoveKeepIndex:]) // zero/nil out the obsolete elements, for GC
	c.keys = c.keys[:c.indexRemoveKeepIndex]
	c.numRow = len(c.keys)
}

// Dicts get dictionary data
// each key is an index of the dictionary
func (c *LowCardinality[T]) Dicts() []T {
	return c.readDict
}

// Keys get keys of data
// each key is an index of the dictionary
func (c *LowCardinality[T]) Keys() []uint32 {
	return c.keys
}

// NumRow return number of row for this block
func (c *LowCardinality[T]) NumRow() int {
	return c.numRow
}

// Array return a Array type for this column
func (c *LowCardinality[T]) Array[R interface{ *Array[T] }]() R {
	return R(NewArray[T](c))
}

// Nullable return a Nullable type for this column
func (c *LowCardinality[T]) Nullable[R interface{ *LowCardinalityNullable[T] }]() R {
	return R(NewLowCardinalityNullable(c.dictColumn))
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
		c.keys = make([]uint32, 0, row)
	}
	c.dictColumn.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *LowCardinality[T]) ReadRaw(num int) error {
	clear(c.dict)
	c.numRow = num
	if c.numRow == 0 {
		c.indices = newIndicesColumn[uint8](c.r)
		c.oldIndicesType = 0
		c.readDict = c.readDict[:0]
		c.keys = c.keys[:0]
		if err := c.dictColumn.ReadRaw(0); err != nil {
			return err
		}
		if c.nullable {
			var empty T
			c.dictColumn.Append(empty)
			c.readDict = append(c.readDict, empty)
		}
		return nil
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
	} else {
		// The indices column is cached across blocks, so it still holds the
		// reader it was built with. Reused across Selects on different pooled
		// connections it would otherwise decode indices from a stale reader and
		// return keys pointing past the freshly rebuilt dictionary, panicking in
		// Row. The dictColumn is immune because ReadHeader refreshes its reader
		// every query.
		c.indices.setReader(c.r)
	}
	err = c.indices.ReadRaw(c.numRow)
	if err != nil {
		return fmt.Errorf("error reading indices: %w", err)
	}
	c.readDict = c.readDict[:0]
	if cap(c.keys) < c.numRow {
		c.keys = make([]uint32, 0, c.numRow)
	} else {
		c.keys = c.keys[:0]
	}
	c.readDict = c.dictColumn.Read(c.readDict)
	for i, value := range c.readDict {
		if c.nullable {
			// Key 0 is NULL; non-null map keys are offset by Append.
			if i == 0 {
				continue
			}
			c.dict[value] = uint32(i - 1)
		} else {
			c.dict[value] = uint32(i)
		}
	}
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
		return "column.LowCardinalityNullable[" + reflect.TypeFor[T]().String() + "]"
	}
	return "column.LowCardinality[" + reflect.TypeFor[T]().String() + "]"
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
	if c.numRow == 0 {
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

func elemOfLowCardinality[T comparable](c *LowCardinality[T], arrayLevel int, nullable bool) ColumnCore {
	if nullable {
		return elemOfLowCardinalityNullable(c.Nullable(), arrayLevel)
	}
	if arrayLevel > 0 {
		return elemOfArray(c.Array(), arrayLevel-1)
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
	return c.dictColumn.ToJSON(int(c.keys[row]), ignoreDoubleQuotes, b)
}

func (c *LowCardinality[T]) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexLowCardinality))
	c.dictColumn.writeBinaryDataTo(w)
}
