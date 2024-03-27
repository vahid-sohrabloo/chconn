package column

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
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
//
//nolint:dupl
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

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *StringBase[T]) RowAny(row int) any {
	return c.Row(row)
}

func (c *StringBase[T]) Scan(row int, dest any) error {
	switch d := dest.(type) {
	case *T:
		*d = c.Row(row)
		return nil
	case **T:
		*d = new(T)
		**d = c.Row(row)
		return nil
	case *[]byte:
		b := c.RowBytes(row)
		if len(*d) < len(b) {
			*d = make([]byte, len(b))
		}
		copy(*d, b)
		return nil
	case **[]byte:
		b := make([]byte, len(c.RowBytes(row)))
		copy(b, c.RowBytes(row))
		*d = &b
		return nil
	case *any:
		*d = c.Row(row)
		return nil
	case sql.Scanner:
		return d.Scan(c.Row(row))
	}

	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "*" + reflect.TypeOf(c.Row(row)).String(),
	}
}

func (c *StringBase[T]) ScanValue(row int, value reflect.Value) error {
	if value.Kind() != reflect.Ptr {
		return fmt.Errorf("scan dest should be a pointer")
	}

	val := value.Elem()

	rowVal := reflect.ValueOf(c.Row(row))
	if val.Type().AssignableTo(rowVal.Type()) {
		val.Set(rowVal)
		return nil
	}
	switch val.Kind() {
	case reflect.String:
		val.SetString(string(c.Row(row)))
		return nil
	case reflect.Slice:
		if val.Type().Elem().Kind() == reflect.Uint8 {
			val.SetBytes(c.RowBytes(row))
			return nil
		}
	case reflect.Interface:
		if value.NumMethod() == 0 {
			val.Set(reflect.ValueOf(c.Row(row)))
			return nil
		}
	}
	return fmt.Errorf("cannot scan text into %s", val.Type().String())
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
	c.preHookAppend()
	i := 0
	for x >= 0x80 {
		c.writerData = append(c.writerData, byte(x)|0x80)
		x >>= 7
		i++
	}
	c.writerData = append(c.writerData, byte(x))
}

// Append value for insert
func (c *StringBase[T]) Append(v T) {
	c.appendLen(len(v))
	c.writerData = append(c.writerData, v...)
	c.numRow++
}

func (c *StringBase[T]) AppendAny(value any) error {
	switch v := value.(type) {
	case T:
		c.Append(v)
	//nolint:gocritic // to ignore caseOrder
	case string:
		c.Append(T(v))
	case []byte:
		c.AppendBytes(v)
	case *string:
		c.Append(T(*v))
	case *[]byte:
		c.AppendBytes(*v)
	default:
		return fmt.Errorf("cannot convert %T to string", value)
	}
	return nil
}

// AppendMulti value for insert
func (c *StringBase[T]) AppendMulti(v ...T) {
	for _, v := range v {
		c.appendLen(len(v))
		c.writerData = append(c.writerData, v...)
	}
	c.numRow += len(v)
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *StringBase[T]) Remove(n int) {
	if c.NumRow() == 0 || c.NumRow() <= n {
		return
	}
	skip := 0
	for i := 0; i < n; i++ {
		strLen, l := binary.Uvarint(c.writerData[skip:])
		skip += l + int(strLen)
	}
	c.numRow = n
	c.writerData = c.writerData[:skip]
}

// AppendBytes value of bytes for insert
func (c *StringBase[T]) AppendBytes(v []byte) {
	c.appendLen(len(v))
	c.writerData = append(c.writerData, v...)
	c.numRow++
}

// AppendBytesMulti value of bytes for insert
func (c *StringBase[T]) AppendBytesMulti(v ...[]byte) {
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
func (c *StringBase[T]) Nullable() *StringNullable[T] {
	return NewStringNullable(c)
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
		if l > 0 {
			c.vals = append(c.vals, make([]byte, l)...)
			if _, err := c.r.Read(c.vals[p.start:p.end]); err != nil {
				return fmt.Errorf("error read string: %w", err)
			}
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

func (c *StringBase[T]) Validate(forInsert bool) error {
	chType := helper.FilterSimpleAggregate(c.chType)
	if !helper.IsString(chType) {
		return &ErrInvalidType{
			chType:     string(c.chType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	return nil
}

func (c *StringBase[T]) chconnType() string {
	return "column.StringBase[" + reflect.TypeOf((*T)(nil)).Elem().String() + "]"
}

func (c *StringBase[T]) structType() string {
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
	c.writerData = append(c.writerData, 0)
	c.numRow++
}

func (c *StringBase[T]) Elem(arrayLevel int, nullable, lc bool) ColumnBasic {
	if lc {
		return c.LowCardinality().elem(arrayLevel, nullable)
	}
	if nullable {
		return c.Nullable().elem(arrayLevel)
	}
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *StringBase[T]) FullType() string {
	if len(c.name) == 0 {
		return "String"
	}
	return string(c.name) + " String"
}

// ToJSON
func (c *StringBase[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	return helper.AppendJSONSting(b, ignoreDoubleQuotes, c.RowBytes(row))
}
