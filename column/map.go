package column

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

// Map use for Map ClickHouse DataTypes
type Map struct {
	Uint64
	offset      int
	val         int
	columnKey   Column
	columnValue Column
}

// NewMap return new Map for Map ClickHouse DataTypes
func NewMap(columnKey, columnValue Column) *Map {
	m := &Map{
		columnKey:   columnKey,
		columnValue: columnValue,
		Uint64: Uint64{
			column: column{
				size: MaplenSize,
			},
		},
	}
	columnKey.setParent(m)
	columnValue.setParent(m)
	return m
}

// ReadRaw read raw data from the reader. it runs automatically when you call `ReadColumns()`
func (c *Map) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
	err := c.Uint64.ReadRaw(num, r)
	if err != nil {
		return fmt.Errorf("read len data: %w", err)
	}
	err = c.columnKey.ReadRaw(c.TotalRows(), r)
	if err != nil {
		return fmt.Errorf("read key data: %w", err)
	}
	err = c.columnValue.ReadRaw(c.TotalRows(), r)
	if err != nil {
		return fmt.Errorf("read value data: %w", err)
	}
	return nil
}

// TotalRows return total rows on this block of array data
func (c *Map) TotalRows() int {
	return int(binary.LittleEndian.Uint64(c.b[c.totalByte-c.size : c.totalByte]))
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *Map) HeaderWriter(w *readerwriter.Writer) {
	c.columnKey.HeaderWriter(w)
	c.columnValue.HeaderWriter(w)
}

// HeaderReader reads header data from read
// it uses internally
func (c *Map) HeaderReader(r *readerwriter.Reader, readColumn bool) error {
	err := c.Uint64.HeaderReader(r, readColumn)
	if err != nil {
		return err
	}
	err = c.columnKey.HeaderReader(r, readColumn)
	if err != nil {
		return err
	}
	return c.columnValue.HeaderReader(r, readColumn)
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *Map) Reset() {
	c.Uint64.Reset()
	c.offset = 0
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value()
func (c *Map) Next() bool {
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
func (c *Map) Value() int {
	return c.val
}

// ReadAll read all lens in this block and append to the input slice
func (c *Map) ReadAll(value *[]int) error {
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
func (c *Map) AppendLen(v int) {
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
func (c *Map) WriteTo(w io.Writer) (int64, error) {
	var n int64
	nw, err := w.Write(c.writerData)
	n += int64(nw)
	if err != nil {
		return n, fmt.Errorf("write len data: %w", err)
	}
	nc, errSubColumn := c.columnKey.WriteTo(w)
	n += nc
	if errSubColumn != nil {
		return n, errSubColumn
	}
	nc, errSubColumn = c.columnValue.WriteTo(w)
	n += nc
	return n, errSubColumn
}
