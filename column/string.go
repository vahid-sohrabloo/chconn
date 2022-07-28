package column

import (
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/v2/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
)

// String is a column of String ClickHouse data type
type String[T ~string] struct {
	column
	numRow     int
	writerData []byte
	vals       [][]byte
}

// NewString is a column of String ClickHouse data type
func NewString[T ~string]() *String[T] {
	return &String[T]{}
}

// Data get all the data in current block as a slice.
func (c *String[T]) Data() []T {
	val := make([]T, len(c.vals))
	for i, v := range c.vals {
		val[i] = T(v)
	}
	return val
}

// Data get all the data in current block as a slice of []byte.
func (c *String[T]) DataBytes() [][]byte {
	return c.vals
}

// Read reads all the data in current block and append to the input.
func (c *String[T]) Read(value []T) []T {
	if cap(value)-len(value) >= len(c.vals) {
		value = (value)[:len(value)+len(c.vals)]
	} else {
		value = append(value, make([]T, len(c.vals))...)
	}
	val := (value)[len(value)-len(c.vals):]
	for i, v := range c.vals {
		val[i] = T(v)
	}
	return value
}

// Read reads all the data as `[]byte` in current block and append to the input.
//
// data is valid only in the current block.
func (c *String[T]) ReadBytes(value [][]byte) [][]byte {
	return append(value, c.vals...)
}

// Row return the value of given row.
//
// NOTE: Row number start from zero
func (c *String[T]) Row(row int) T {
	return T(c.vals[row])
}

// Row return the value of given row.
//
// Data is valid only in the current block.
func (c *String[T]) RowBytes(row int) []byte {
	return c.vals[row]
}

func (c *String[T]) appendLen(x int) {
	i := 0
	for x >= 0x80 {
		c.writerData = append(c.writerData, byte(x)|0x80)
		x >>= 7
		i++
	}
	c.writerData = append(c.writerData, byte(x))
}

// Append value for insert
func (c *String[T]) Append(v T) {
	c.numRow++
	c.appendLen(len(v))
	c.writerData = append(c.writerData, v...)
}

// AppendSlice append slice of value for insert
func (c *String[T]) AppendSlice(v []T) {
	for _, vv := range v {
		c.Append(vv)
	}
}

// NumRow return number of row for this block
func (c *String[T]) NumRow() int {
	return c.numRow
}

// Array return a Array type for this column
func (c *String[T]) Array() *Array[T] {
	return NewArray[T](c)
}

// Nullable return a nullable type for this column
func (c *String[T]) Nullable() *Nullable[T] {
	return NewNullable[T](c)
}

// LC return a low cardinality type for this column
func (c *String[T]) LC() *LowCardinality[T] {
	return NewLC[T](c)
}

// LowCardinality return a low cardinality type for this column
func (c *String[T]) LowCardinality() *LowCardinality[T] {
	return NewLC[T](c)
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *String[T]) Reset() {
	c.numRow = 0
	c.vals = c.vals[:0]
	c.writerData = c.writerData[:0]
}

// SetWriteBufferSize set write buffer (number of bytes)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *String[T]) SetWriteBufferSize(b int) {
	if cap(c.writerData) < b {
		c.writerData = make([]byte, 0, b)
	}
}

// ReadRaw read raw data from the reader. it runs automatically when you call `ReadColumns()`
func (c *String[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
	c.r = r
	c.numRow = num
	if cap(c.vals) < num {
		c.vals = make([][]byte, num)
	} else {
		c.vals = c.vals[:num]
	}
	for i := 0; i < num; i++ {
		l, err := c.r.Uvarint()
		if err != nil {
			return fmt.Errorf("error read string len: %w", err)
		}
		if cap(c.vals[i]) < int(l) {
			c.vals[i] = make([]byte, l)
		} else {
			c.vals[i] = c.vals[i][:l]
		}
		_, err = c.r.Read(c.vals[i])
		if err != nil {
			return fmt.Errorf("error read string: %w", err)
		}
	}
	return nil
}

// HeaderReader reads header data from read
// it uses internally
func (c *String[T]) HeaderReader(r *readerwriter.Reader, readColumn bool) error {
	c.r = r
	return c.readColumn(readColumn)
}

func (c *String[T]) Validate() error {
	chType := helper.FilterSimpleAggregate(c.chType)
	if !helper.IsString(chType) {
		return ErrInvalidType{
			column: c,
		}
	}
	return nil
}

func (c *String[T]) columnType() string {
	return helper.StringStr
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *String[T]) WriteTo(w io.Writer) (int64, error) {
	nw, err := w.Write(c.writerData)
	return int64(nw), err
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *String[T]) HeaderWriter(w *readerwriter.Writer) {
}

func (c *String[T]) appendEmpty() {
	var emptyValue T
	c.Append(emptyValue)
}

func (c *String[T]) Elem(arrayLevel int, nullable, lc bool) ColumnBasic {
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
