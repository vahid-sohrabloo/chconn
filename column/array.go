package column

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

// Array use for Array ClickHouse DataTypes
type Array struct {
	Uint64
	offset     int
	val        int
	dataColumn Column
}

// NewArray return new Array for Array ClickHouse DataTypes
func NewArray(dataColumn Column) *Array {
	a := &Array{
		dataColumn: dataColumn,
		Uint64: Uint64{
			column: column{
				size: ArraylenSize,
			},
		},
	}
	dataColumn.setParent(a)
	return a
}

// ReadRaw read raw data from the reader. it runs automatically when you call `ReadColumns()`
func (c *Array) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
	err := c.Uint64.ReadRaw(num, r)
	if err != nil {
		return err
	}
	return c.dataColumn.ReadRaw(c.TotalRows(), r)
}

// TotalRows return total rows on this block of array data
func (c *Array) TotalRows() int {
	return int(binary.LittleEndian.Uint64(c.b[c.totalByte-c.size : c.totalByte]))
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *Array) HeaderWriter(w *readerwriter.Writer) {
	c.dataColumn.HeaderWriter(w)
}

// HeaderReader reads header data from read
// it uses internally
func (c *Array) HeaderReader(r *readerwriter.Reader, readColumn bool) error {
	err := c.Uint64.HeaderReader(r, readColumn)
	if err != nil {
		return err
	}
	err = c.dataColumn.HeaderReader(r, readColumn)
	if err != nil {
		return err
	}
	c.dataColumn.SetType(c.Type()[len("Array(") : len(c.Type())-1])
	return err
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *Array) Reset() {
	c.Uint64.Reset()
	c.offset = 0
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value()
func (c *Array) Next() bool {
	ok := c.Uint64.Next()
	if !ok {
		return false
	}
	offset := int(c.Uint64.Value())
	c.val = offset - c.offset
	c.offset = offset
	return true
}

// Value of current pointer
//
// Use with Next()
func (c *Array) Value() int {
	return c.val
}

// Value of current pointer
// NOTE: Row number start from zero
func (c *Array) Row(i int) int {
	if i == 0 {
		return int(c.Uint64.Row(i))
	}
	return int(c.Uint64.Row(i) - c.Uint64.Row(i-1))
}

// ReadAll read all lens in this block and append to the input slice
func (c *Array) ReadAll(value *[]int) error {
	var offset uint64
	var prevOffset uint64
	for i := 0; i < c.totalByte; i += c.size {
		offset = binary.LittleEndian.Uint64(c.b[i : i+c.size])
		*value = append(*value, int(offset-prevOffset))
		prevOffset = offset
	}
	c.offset = int(offset)
	return nil
}

// AppendLen Append len for insert
func (c *Array) AppendLen(v int) {
	c.numRow++
	c.offset += v
	c.writerData = append(c.writerData,
		byte(c.offset),
		byte(c.offset>>8),
		byte(c.offset>>16),
		byte(c.offset>>24),
		byte(c.offset>>32),
		byte(c.offset>>40),
		byte(c.offset>>48),
		byte(c.offset>>56),
	)
}

// WriteTo write data clickhouse
// it uses internally
func (c *Array) WriteTo(w io.Writer) (int64, error) {
	nw, err := w.Write(c.writerData)
	if err != nil {
		return 0, fmt.Errorf("write len data: %w", err)
	}
	n, errdataColumn := c.dataColumn.WriteTo(w)

	return int64(nw) + n, errdataColumn
}

// DataColumn return data column
func (c *Array) DataColumn() Column {
	return c.dataColumn
}
