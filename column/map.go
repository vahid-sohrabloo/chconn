package column

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

type Map struct {
	Uint64
	offset      int
	val         int
	columnKey   Column
	columnValue Column
}

func NewMap(columnKey, columnValue Column) *Map {
	return &Map{
		columnKey:   columnKey,
		columnValue: columnValue,
		Uint64: Uint64{
			column: column{
				size: MaplenSize,
			},
		},
	}
}

func (c *Map) ReadRaw(num int, r *readerwriter.Reader) error {
	c.reset()
	err := c.Uint64.ReadRaw(num, r)
	if err != nil {
		return err
	}
	err = c.columnKey.ReadRaw(c.TotalRows(), r)
	if err != nil {
		return err
	}
	return c.columnValue.ReadRaw(c.TotalRows(), r)
}

func (c *Map) TotalRows() int {
	return int(binary.LittleEndian.Uint64(c.b[c.totalByte-c.size : c.totalByte]))
}

func (c *Map) HeaderWriter(w *readerwriter.Writer) {
	c.columnKey.HeaderWriter(w)
	c.columnValue.HeaderWriter(w)
}
func (c *Map) HeaderReader(r *readerwriter.Reader) error {
	err := c.columnKey.HeaderReader(r)
	if err != nil {
		return err
	}
	return c.columnValue.HeaderReader(r)
}

func (c *Map) reset() {
	c.Uint64.Reset()
	c.offset = 0
}

func (c *Map) Next() bool {
	ok := c.Uint64.Next()
	if !ok {
		return false
	}
	c.val = int(c.Uint64.val) - c.offset
	c.offset = int(c.Uint64.val)
	return true
}

func (c *Map) Value() int {
	return c.val
}

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
