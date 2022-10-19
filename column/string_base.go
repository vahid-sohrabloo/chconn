package column

import (
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/v2/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
)

type stringPos struct {
	start int
	end   int
}

// StringBase is a column of String ClickHouse data type with generic type
type StringBase[T ~string] struct {
	column
	numRow     int
	writerData []byte
	vals       []byte
	pos        []stringPos
}

// NewString is a column of String ClickHouse data type with generic type
func NewStringBase[T ~string]() *StringBase[T] {
	return &StringBase[T]{}
}

// Data get all the data in current block as a slice.
func (c *StringBase[T]) Data() []T {
	val := make([]T, len(c.pos))
	for i, v := range c.pos {
		val[i] = T(c.vals[v.start:v.end])
	}
	return val
}

// Data get all the data in current block as a slice of []byte.
func (c *StringBase[T]) DataBytes() [][]byte {
	return c.ReadBytes(nil)
}

// Read reads all the data in current block and append to the input.
func (c *StringBase[T]) Read(value []T) []T {
	if cap(value)-len(value) >= len(c.pos) {
		value = (value)[:len(value)+len(c.pos)]
	} else {
		value = append(value, make([]T, len(c.pos))...)
	}
	val := (value)[len(value)-len(c.pos):]
	for i, v := range c.pos {
		val[i] = T(c.vals[v.start:v.end])
	}
	return value
}

// Read reads all the data as `[]byte` in current block and append to the input.
//
// data is valid only in the current block.
func (c *StringBase[T]) ReadBytes(value [][]byte) [][]byte {
	if cap(value)-len(value) >= len(c.pos) {
		value = (value)[:len(value)+len(c.pos)]
	} else {
		value = append(value, make([][]byte, len(c.pos))...)
	}

	val := (value)[len(value)-len(c.pos):]
	for i, v := range c.pos {
		val[i] = c.vals[v.start:v.end]
	}

	return value
}

// Row return the value of given row.
//
// NOTE: Row number start from zero
func (c *StringBase[T]) Row(row int) T {
	return T(c.RowBytes(row))
}

// Row return the value of given row.
//
// Data is valid only in the current block.
func (c *StringBase[T]) RowBytes(row int) []byte {
	pos := c.pos[row]
	return c.vals[pos.start:pos.end]
}

func (c *StringBase[T]) Each(f func(i int, b []byte) bool) {
	for i, p := range c.pos {
		if !f(i, c.vals[p.start:p.end]) {
			return
		}
	}
}

func (c *StringBase[T]) appendLen(x int) {
	i := 0
	for x >= 0x80 {
		c.writerData = append(c.writerData, byte(x)|0x80)
		x >>= 7
		i++
	}
	c.writerData = append(c.writerData, byte(x))
}

// Append value for insert
func (c *StringBase[T]) Append(v ...T) {
	for _, v := range v {
		c.appendLen(len(v))
		c.writerData = append(c.writerData, v...)
	}
	c.numRow += len(v)
}

// AppendBytes value of bytes for insert
func (c *StringBase[T]) AppendBytes(v ...[]byte) {
	for _, v := range v {
		c.appendLen(len(v))
		c.writerData = append(c.writerData, v...)
	}
	c.numRow += len(v)
}

// NumRow return number of row for this block
func (c *StringBase[T]) NumRow() int {
	return c.numRow
}

// Array return a Array type for this column
func (c *StringBase[T]) Array() *Array[T] {
	return NewArray[T](c)
}

// Nullable return a nullable type for this column
func (c *StringBase[T]) Nullable() *Nullable[T] {
	return NewNullable[T](c)
}

// LC return a low cardinality type for this column
func (c *StringBase[T]) LC() *LowCardinality[T] {
	return NewLC[T](c)
}

// LowCardinality return a low cardinality type for this column
func (c *StringBase[T]) LowCardinality() *LowCardinality[T] {
	return NewLC[T](c)
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *StringBase[T]) Reset() {
	c.numRow = 0
	c.vals = c.vals[:0]
	c.pos = c.pos[:0]
	c.writerData = c.writerData[:0]
}

// SetWriteBufferSize set write buffer (number of bytes)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *StringBase[T]) SetWriteBufferSize(b int) {
	if cap(c.writerData) < b {
		c.writerData = make([]byte, 0, b)
	}
}

// ReadRaw read raw data from the reader. it runs automatically when you call `ReadColumns()`
func (c *StringBase[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
	c.r = r
	c.numRow = num

	var p stringPos
	for i := 0; i < num; i++ {
		l, err := c.r.Uvarint()
		if err != nil {
			return fmt.Errorf("error read string len: %w", err)
		}

		p.start = p.end
		p.end += int(l)

		c.vals = append(c.vals, make([]byte, l)...)
		if _, err := c.r.Read(c.vals[p.start:p.end]); err != nil {
			return fmt.Errorf("error read string: %w", err)
		}
		c.pos = append(c.pos, p)
	}
	return nil
}

// HeaderReader reads header data from read
// it uses internally
func (c *StringBase[T]) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	c.r = r
	return c.readColumn(readColumn, revision)
}

func (c *StringBase[T]) Validate() error {
	chType := helper.FilterSimpleAggregate(c.chType)
	if !helper.IsString(chType) {
		return ErrInvalidType{
			column: c,
		}
	}
	return nil
}

func (c *StringBase[T]) ColumnType() string {
	return helper.StringStr
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *StringBase[T]) WriteTo(w io.Writer) (int64, error) {
	nw, err := w.Write(c.writerData)
	return int64(nw), err
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *StringBase[T]) HeaderWriter(w *readerwriter.Writer) {
}

func (c *StringBase[T]) appendEmpty() {
	var emptyValue T
	c.Append(emptyValue)
}

func (c *StringBase[T]) Elem(arrayLevel int, nullable, lc bool) ColumnBasic {
	if nullable {
		return c.Nullable().elem(arrayLevel, lc)
	}
	if lc {
		return c.LowCardinality().elem(arrayLevel)
	}
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}
