package column

import (
	"fmt"
	"io"
	"math"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
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

//  LCDictColumn is a interface for column that can be LowCardinality
type LCDictColumn interface {
	Column
	getKeys() []int
}

// LC use for LowCardinality ClickHouse DataTypes
type LC struct {
	column
	DictColumn     LCDictColumn
	indices        indicesColumn
	oldIndicesType int
	scratch        [8]byte
}

var _ Column = &LC{}

// NewLowCardinality return new LC for LowCardinality ClickHouse DataTypes
func NewLowCardinality(dictColumn LCDictColumn) *LC {
	return NewLC(dictColumn)
}

// NewLC return new LC for LowCardinality ClickHouse DataTypes
func NewLC(dictColumn LCDictColumn) *LC {
	l := &LC{
		DictColumn: dictColumn,
	}
	dictColumn.setParent(l)
	return l
}

// ReadRaw read raw data from the reader. it runs automatically when you call `ReadColumns()`
func (c *LC) ReadRaw(num int, r *readerwriter.Reader) error {
	c.r = r
	c.numRow = num
	if c.numRow == 0 {
		c.indices = NewUint8(false)
		// to reset nullable dictionary
		return c.DictColumn.ReadRaw(0, r)
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
	nullable := c.DictColumn.IsNullable()
	// disable nullable for low cardinality dictionary
	c.DictColumn.setNullable(false)
	err = c.DictColumn.ReadRaw(int(dictionarySize), r)
	c.DictColumn.setNullable(nullable)
	if err != nil {
		return fmt.Errorf("error reading dictionary: %w", err)
	}

	indicesSize, err := r.Uint64()
	c.numRow = int(indicesSize)
	if err != nil {
		return err
	}
	if c.indices == nil || c.oldIndicesType != intType {
		c.indices = getLCIndicate(intType)
		c.oldIndicesType = intType
	}

	return c.indices.ReadRaw(c.numRow, c.r)
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value()
func (c *LC) Next() bool {
	return c.indices.Next()
}

// Value of current pointer
//
// Use with Next()
func (c *LC) Value() int {
	return c.indices.valueInt()
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *LC) Row(row int) int {
	return c.indices.rowInt(row)
}

// ReadAll read all keys in this block and append to the input slice
func (c *LC) ReadAll(value *[]int) {
	c.indices.readAllInt(value)
}

// Fill slice with keys and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *LC) Fill(value []int) {
	c.indices.fillInt(value)
}

// NumRow return number of keys for this block
func (c *LC) NumRow() int {
	return len(c.DictColumn.getKeys())
}

// HeaderReader writes header data to writer
// it uses internally
func (c *LC) HeaderReader(r *readerwriter.Reader, readColumn bool) error {
	err := c.column.HeaderReader(r, readColumn)
	if err != nil {
		return err
	}
	// write KeysSerializationVersion. for more information see clickhouse docs
	_, err = r.Uint64()
	if err != nil {
		err = fmt.Errorf("error reading keys serialization version: %w", err)
	}
	return err
}

// HeaderWriter reads header data from read
// it uses internally
func (c *LC) HeaderWriter(w *readerwriter.Writer) {
	// write KeysSerializationVersion. for more information see clickhouse docs
	w.Int64(1)
}

// WriteTo write data clickhouse
// it uses internally
func (c *LC) WriteTo(w io.Writer) (int64, error) {
	dictionarySize := c.DictColumn.NumRow()
	// Do not write anything for empty column.
	// May happen while writing empty arrays.
	if dictionarySize == 0 {
		return 0, nil
	}
	dictNullable := c.DictColumn.IsNullable()
	if dictNullable {
		dictionarySize++
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

	if dictNullable {
		nw, err = w.Write(c.DictColumn.GetEmpty())
		n += int64(nw)
		if err != nil {
			return n, fmt.Errorf("error writing null empty data: %w", err)
		}
	}
	c.DictColumn.setNullable(false)
	nwd, err := c.DictColumn.WriteTo(w)
	c.DictColumn.setNullable(dictNullable)
	n += nwd
	if err != nil {
		return n, fmt.Errorf("error writing dictionary: %w", err)
	}
	keys := c.DictColumn.getKeys()
	nw, err = c.writeUint64(w, uint64(len(keys)))
	n += int64(nw)
	if err != nil {
		return n, fmt.Errorf("error writing keys len: %w", err)
	}
	if c.indices == nil || c.oldIndicesType != intType {
		c.indices = getLCIndicate(intType)
		c.oldIndicesType = intType
	} else {
		c.indices.Reset()
	}
	c.indices = getLCIndicate(intType)
	c.indices.appendInts(keys)
	nwt, err := c.indices.WriteTo(w)
	if err != nil {
		return n, fmt.Errorf("error writing indices: %w", err)
	}
	return n + nwt, err
}

func getLCIndicate(intType int) indicesColumn {
	switch intType {
	case 0:
		return NewUint8(false)
	case 1:
		return NewUint16(false)
	case 2:
		return NewUint32(false)
	case 3:
		panic("cannot handle this amount of data fo lc")
	}
	// this should never happen unless something wrong with the code
	panic("cannot not find indicate type")
}

func (c *LC) writeUint64(w io.Writer, v uint64) (int, error) {
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

func (c *LC) IsNullable() bool {
	// low cardinality column cannot be nullable
	return false
}
func (c *LC) setNullable(nullable bool) {
	// low cardinality column cannot be nullable
}

// AppendEmpty append empty value for insert
//
// it does nothing for low cardinality column
func (c *LC) AppendEmpty() {
}

// GetEmpty return empty value for insert
// it does nothing for low cardinality column
func (c *LC) GetEmpty() []byte {
	return emptyByte[:c.size]
}

// Reset reset column
// it does nothing for low cardinality column
func (c *LC) Reset() {
}

func (c *LC) setParent(parent Column) {
	c.parent = parent
}
