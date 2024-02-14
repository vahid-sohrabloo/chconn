package column

import (
	"encoding"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

type marshalerUnmarshalerText interface {
	encoding.TextMarshaler
	encoding.TextUnmarshaler
	json.Marshaler
	json.Unmarshaler
}

type StringMarshaler[T marshalerUnmarshalerText] struct {
	column
	numRow     int
	writerData []byte
	vals       []byte
	pos        []stringPos
}

// NewString is a column of String ClickHouse data type
func NewStringMarshaler[T marshalerUnmarshalerText]() *StringMarshaler[T] {
	return &StringMarshaler[T]{}
}

// Data get all the data in current block as a slice.
func (c *StringMarshaler[T]) Data() []T {
	val := make([]T, len(c.pos))
	for i := range c.pos {
		val[i] = c.Row(i)
	}
	return val
}

// Data get all the data in current block as a slice of []byte.
func (c *StringMarshaler[T]) DataBytes() [][]byte {
	return c.ReadBytes(nil)
}

// Read reads all the data in current block and append to the input.
func (c *StringMarshaler[T]) Read(value []T) []T {
	return append(value, c.Data()...)
}

// Read reads all the data as `[]byte` in current block and append to the input.
//
// data is valid only in the current block.
//
//nolint:dupl
func (c *StringMarshaler[T]) ReadBytes(value [][]byte) [][]byte {
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
func (c *StringMarshaler[T]) Row(row int) T {
	var t T
	//nolint:errcheck
	t.UnmarshalText(c.RowBytes(row))
	return t
}

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *StringMarshaler[T]) RowAny(row int) any {
	return c.Row(row)
}

func (c *StringMarshaler[T]) Scan(row int, value any) error {
	// TODO: implement
	// switch d := value.(type) {
	// case *string:
	// 	*d = string(c.RowBytes(row))
	// case *[]byte:
	// 	*d = c.RowBytes(row)
	// default:
	// 	return fmt.Errorf("unsupported type %T", value)
	// }
	return nil
}

func (c *StringMarshaler[T]) ScanValue(row int, value reflect.Value) error {
	// TODO: implement
	// switch d := value.(type) {
	// case *string:
	// 	*d = string(c.RowBytes(row))
	// case *[]byte:
	// 	*d = c.RowBytes(row)
	// default:
	// 	return fmt.Errorf("unsupported type %T", value)
	// }
	return nil
}

// Row return the value of given row.
//
// Data is valid only in the current block.
func (c *StringMarshaler[T]) RowBytes(row int) []byte {
	pos := c.pos[row]
	return c.vals[pos.start:pos.end]
}

func (c *StringMarshaler[T]) Each(f func(i int, b []byte) bool) {
	for i, p := range c.pos {
		if !f(i, c.vals[p.start:p.end]) {
			return
		}
	}
}

func (c *StringMarshaler[T]) appendLen(x int) {
	c.preHookAppend()
	i := 0
	p := stringPos{
		start: len(c.writerData),
	}
	for x >= 0x80 {
		c.writerData = append(c.writerData, byte(x)|0x80)
		x >>= 7
		i++
	}
	c.writerData = append(c.writerData, byte(x))
	// we don't need the end position of the string. we use position only for remove
	c.pos = append(c.pos, p)
}

// Append value for insert
func (c *StringMarshaler[T]) Append(v T) {
	d, _ := v.MarshalText()
	c.appendLen(len(d))
	c.writerData = append(c.writerData, d...)
	c.numRow++
}

func (c *StringMarshaler[T]) AppendAny(value any) error {
	// TODO: implement this
	return nil
}

// AppendMulti value for insert
func (c *StringMarshaler[T]) AppendMulti(v ...T) {
	for _, v := range v {
		d, _ := v.MarshalText()
		c.appendLen(len(d))
		c.writerData = append(c.writerData, d...)
	}
	c.numRow += len(v)
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *StringMarshaler[T]) Remove(n int) {
	if len(c.writerData) == 0 || c.NumRow() <= n {
		return
	}
	pos := c.pos[n]
	c.writerData = c.writerData[:pos.start]
	c.pos = c.pos[:n]
	c.numRow = len(c.pos)
}

// AppendBytes value of bytes for insert
func (c *StringMarshaler[T]) AppendBytes(v ...[]byte) {
	for _, v := range v {
		c.appendLen(len(v))
		c.writerData = append(c.writerData, v...)
	}
	c.numRow += len(v)
}

// NumRow return number of row for this block
func (c *StringMarshaler[T]) NumRow() int {
	return c.numRow
}

// Array return a Array type for this column
func (c *StringMarshaler[T]) Array() *Array[T] {
	return NewArray[T](c)
}

// Nullable return a nullable type for this column
// TODO must complete for release
// func (c *StringMarshaler[T]) Nullable() *StringNullable[T] {
// 	return NewNullable[T](c)
// }

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *StringMarshaler[T]) Reset() {
	c.numRow = 0
	c.vals = c.vals[:0]
	c.pos = c.pos[:0]
	c.writerData = c.writerData[:0]
}

// SetWriteBufferSize set write buffer (number of bytes)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *StringMarshaler[T]) SetWriteBufferSize(b int) {
	if cap(c.writerData) < b {
		c.writerData = make([]byte, 0, b)
	}
}

// ReadRaw read raw data from the reader. it runs automatically when you call `ReadColumns()`
func (c *StringMarshaler[T]) ReadRaw(num int, r *readerwriter.Reader) error {
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
func (c *StringMarshaler[T]) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	c.r = r
	return c.readColumn(readColumn, revision)
}

func (c *StringMarshaler[T]) Validate(forInsert bool) error {
	chType := helper.FilterSimpleAggregate(c.chType)
	if !helper.IsString(chType) {
		return ErrInvalidType{
			chType:     string(c.chType),
			structType: c.structType(),
		}
	}
	return nil
}

func (c *StringMarshaler[T]) structType() string {
	return helper.StringStr
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *StringMarshaler[T]) WriteTo(w io.Writer) (int64, error) {
	nw, err := w.Write(c.writerData)
	return int64(nw), err
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *StringMarshaler[T]) HeaderWriter(w *readerwriter.Writer) {
}

func (c *StringMarshaler[T]) Elem(arrayLevel int, nullable bool) ColumnBasic {
	if nullable {
		// TODO must complete for release
		return nil
	}
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *StringMarshaler[T]) FullType() string {
	if len(c.name) == 0 {
		return "String"
	}
	return string(c.name) + " String"
}

// ToJSON
func (c *StringMarshaler[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	d := c.Row(row)
	t, _ := d.MarshalJSON()
	return append(b, t...)
}
