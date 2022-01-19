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

type lcDictColumn interface {
	Column
	getKeys() []int
}

// LC use for LowCardinality ClickHouse DataTypes
type LC struct {
	column
	dictColumn lcDictColumn
	indices    indicesColumn
	scratch    [8]byte
}

var _ Column = &LC{}

// NewLowCardinality return new LC for LowCardinality ClickHouse DataTypes
func NewLowCardinality(dictColumn lcDictColumn) *LC {
	return NewLC(dictColumn)
}

// NewLC return new LC for LowCardinality ClickHouse DataTypes
func NewLC(dictColumn lcDictColumn) *LC {
	if dictColumn.isNullable() {
		dictColumn.AppendEmpty()
	}
	l := &LC{
		dictColumn: dictColumn,
	}
	dictColumn.setParent(l)
	return l
}

// ReadRaw read raw data from the reader. it runs automatically when you call `NextColumn()`
func (c *LC) ReadRaw(num int, r *readerwriter.Reader) error {
	c.r = r
	c.numRow = num
	if c.numRow == 0 {
		c.indices = NewUint8(false)
		// to reset nullable dictionary
		return c.dictColumn.ReadRaw(0, r)
	}

	serializationType, err := c.r.Uint64()
	if err != nil {
		return fmt.Errorf("error reading serialization type: %w", err)
	}
	intType := serializationType & 0xf

	dictionarySize, err := c.r.Uint64()
	if err != nil {
		return fmt.Errorf("error reading dictionary size: %w", err)
	}
	nullable := c.dictColumn.isNullable()
	// disable nullable for low cardinality dictionary
	c.dictColumn.setNullable(false)
	err = c.dictColumn.ReadRaw(int(dictionarySize), r)
	c.dictColumn.setNullable(nullable)
	if err != nil {
		return fmt.Errorf("error reading dictionary: %w", err)
	}

	indicesSize, err := r.Uint64()
	c.numRow = int(indicesSize)
	if err != nil {
		return err
	}

	switch intType {
	case 0:
		c.indices = NewUint8(false)
	case 1:
		c.indices = NewUint16(false)
	case 2:
		c.indices = NewUint32(false)
	case 3:
		panic("cannot handle this amount of data fo lc")
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
	return len(c.dictColumn.getKeys())
}

// HeaderReader writes header data to writer
// it uses internally
func (c *LC) HeaderReader(r *readerwriter.Reader) error {
	err := c.column.HeaderReader(r)
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
	dictionarySize := c.dictColumn.NumRow()
	// Do not write anything for empty column.
	// May happen while writing empty arrays.
	if dictionarySize == 0 || (c.dictColumn.isNullable() && dictionarySize <= 1) {
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
	keys := c.dictColumn.getKeys()
	nw, err = c.writeUint64(w, uint64(len(keys)))
	n += int64(nw)
	if err != nil {
		return n, fmt.Errorf("error writing keys len: %w", err)
	}

	switch intType {
	case 0:
		c.indices = NewUint8(false)
	case 1:
		c.indices = NewUint16(false)
	case 2:
		c.indices = NewUint32(false)
	case 3:
		panic("cannot handle this amount of data fo lc")
	}
	c.indices.appendInts(keys)
	nwt, err := c.indices.WriteTo(w)
	if err != nil {
		return n, fmt.Errorf("error writing indices: %w", err)
	}
	return n + nwt, err
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

func (c *LC) isNullable() bool {
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

// Reset reset column
// it does nothing for low cardinality column
func (c *LC) Reset() {
}

func (c *LC) setParent(parent Column) {
	c.parent = parent
}
