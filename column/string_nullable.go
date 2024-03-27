package column

import (
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

// StringNullable is a column of Nullable(T) ClickHouse data type
type StringNullable[T ~string] struct {
	column
	numRow     int
	dataColumn *StringBase[T]
	writerData []byte
	b          []byte
}

// NewStringNullable return new StringNullable for StringNullable(T) ClickHouse DataType
func NewStringNullable[T ~string](dataColumn *StringBase[T]) *StringNullable[T] {
	return &StringNullable[T]{
		dataColumn: dataColumn,
	}
}

// Data get all the data in current block as a slice.
//
// NOTE: the return slice only valid in current block, if you want to use it after, you should copy it. or use Read
func (c *StringNullable[T]) Data() []T {
	return c.dataColumn.Data()
}

// Data get all the nullable  data in current block as a slice of pointer.
//
// As an alternative (for better performance).
// You can use `Data` and one of `RowIsNil` and `ReadNil` and `DataNil`  to detect if value is null or not.
func (c *StringNullable[T]) DataP() []*T {
	val := make([]*T, c.numRow)
	for i, d := range c.dataColumn.Data() {
		if c.RowIsNil(i) {
			val[i] = nil
		} else {
			// make a copy of the value
			v := d
			val[i] = &v
		}
	}
	return val
}

// Read reads all the data in current block and append to the input.
func (c *StringNullable[T]) Read(value []T) []T {
	return c.dataColumn.Read(value)
}

// ReadP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), You can use `Read` and one of `RowIsNil` and `ReadNil` and `DataNil`
// to detect if value is null or not.
func (c *StringNullable[T]) ReadP(value []*T) []*T {
	for i := 0; i < c.numRow; i++ {
		value = append(value, c.RowP(i))
	}
	return value
}

// Row return the value of given row
func (c *StringNullable[T]) Row(i int) T {
	return c.dataColumn.Row(i)
}

// Row return the value of given row as bytes
func (c *StringNullable[T]) RowBytes(i int) []byte {
	return c.dataColumn.RowBytes(i)
}

// RowAny return the value of given row
func (c *StringNullable[T]) RowAny(i int) any {
	return c.RowP(i)
}

func (c *StringNullable[T]) Scan(row int, dest any) error {
	switch d := dest.(type) {
	case *T:
		*d = c.Row(row)
		return nil
	case **T:
		*d = c.RowP(row)
		return nil
	case *[]byte:
		b := c.RowBytes(row)
		if len(*d) < len(b) {
			*d = make([]byte, len(b))
		}
		copy(*d, b)
		return nil
	case **[]byte:
		if c.b[row] == 1 {
			*d = nil
			return nil
		}
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
		columnType: "**" + reflect.TypeOf(c.Row(row)).String() + " or **[]byte",
	}
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `RowIsNil()` to check if it is null.
func (c *StringNullable[T]) RowP(row int) *T {
	if c.b[row] == 1 {
		return nil
	}
	val := c.dataColumn.Row(row)
	return &val
}

// ReadAll read all nils state in this block and append to the input
func (c *StringNullable[T]) ReadNil(value []bool) []bool {
	return append(value, *(*[]bool)(unsafe.Pointer(&c.b))...)
}

// DataNil get all nil state in this block
func (c *StringNullable[T]) DataNil() []bool {
	return *(*[]bool)(unsafe.Pointer(&c.b))
}

// RowIsNil return true if the row is null
func (c *StringNullable[T]) RowIsNil(row int) bool {
	return c.b[row] == 1
}

// Append value for insert
func (c *StringNullable[T]) Append(v T) {
	c.preHookAppend()
	c.writerData = append(c.writerData, 0)
	c.dataColumn.Append(v)
}

func (c *StringNullable[T]) AppendAny(value any) error {
	switch v := value.(type) {
	case T:
		c.Append(v)
	//nolint:gocritic // to ignore caseOrder
	case string:
		c.Append(T(v))
	case []byte:
		c.AppendBytes(v)
	case *string:
		var tmp = T(*v)
		c.AppendP(&tmp)
	case *[]byte:
		c.AppendBytes(*v)
	case nil:
		c.AppendNil()
	default:
		return fmt.Errorf("cannot convert %T to string", value)
	}
	return nil
}

// Append value for insert
func (c *StringNullable[T]) AppendMulti(v ...T) {
	c.preHookAppendMulti(len(v))
	c.writerData = append(c.writerData, make([]uint8, len(v))...)
	c.dataColumn.AppendMulti(v...)
}

// Append value for insert
func (c *StringNullable[T]) AppendBytes(v []byte) {
	c.preHookAppend()
	c.writerData = append(c.writerData, 0)
	c.dataColumn.AppendBytes(v)
}

// Append value for insert
func (c *StringNullable[T]) AppendBytesMulti(v ...[]byte) {
	c.preHookAppendMulti(len(v))
	c.writerData = append(c.writerData, make([]uint8, len(v))...)
	c.dataColumn.AppendBytesMulti(v...)
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *StringNullable[T]) Remove(n int) {
	if c.NumRow() == 0 || c.NumRow() <= n {
		return
	}
	c.writerData = c.writerData[:n]
	c.dataColumn.Remove(n)
}

// AppendP nullable value for insert
//
// as an alternative (for better performance), you can use `Append` and `AppendNil` to insert a value
func (c *StringNullable[T]) AppendP(v *T) {
	if v == nil {
		c.AppendNil()
		return
	}
	c.Append(*v)
}

// AppendMultiP nullable value for insert
//
// as an alternative (for better performance), you can use `Append` and `AppendNil` to insert a value
func (c *StringNullable[T]) AppendMultiP(v ...*T) {
	for _, v := range v {
		if v == nil {
			c.AppendNil()
			continue
		}
		c.Append(*v)
	}
}

// Append nil value for insert
func (c *StringNullable[T]) AppendNil() {
	c.preHookAppend()
	c.writerData = append(c.writerData, 1)
	c.dataColumn.appendEmpty()
}

// NumRow return number of row for this block
func (c *StringNullable[T]) NumRow() int {
	return c.dataColumn.NumRow()
}

// Array return a Array type for this column
func (c *StringNullable[T]) Array() *ArrayNullable[T] {
	return NewArrayNullable[T](c)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *StringNullable[T]) Reset() {
	c.b = c.b[:0]
	c.numRow = 0
	c.writerData = c.writerData[:0]
	c.dataColumn.Reset()
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *StringNullable[T]) SetWriteBufferSize(row int) {
	if cap(c.writerData) < row {
		c.writerData = make([]byte, 0, row)
	}
	c.dataColumn.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *StringNullable[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
	c.r = r
	c.numRow = num

	err := c.readBuffer()
	if err != nil {
		return fmt.Errorf("read nullable data: %w", err)
	}
	return c.dataColumn.ReadRaw(num, r)
}

func (c *StringNullable[T]) readBuffer() error {
	if cap(c.b) < c.numRow {
		c.b = make([]byte, c.numRow)
	} else {
		c.b = c.b[:c.numRow]
	}
	_, err := c.r.Read(c.b)
	if err != nil {
		return fmt.Errorf("read nullable data: %w", err)
	}
	return nil
}

// HeaderReader reads header data from reader
// it uses internally
func (c *StringNullable[T]) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	c.r = r
	err := c.readColumn(readColumn, revision)
	if err != nil {
		return err
	}
	return c.dataColumn.HeaderReader(r, false, revision)
}

func (c *StringNullable[T]) Validate(forInsert bool) error {
	chType := helper.FilterSimpleAggregate(c.chType)
	if !helper.IsNullable(chType) {
		return &ErrInvalidType{
			chType:     string(c.chType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	c.dataColumn.SetType(chType[helper.LenNullableStr : len(chType)-1])
	if err := c.dataColumn.Validate(forInsert); err != nil {
		if !isInvalidType(err) {
			return err
		}
		return &ErrInvalidType{
			chType:     string(c.chType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	return nil
}

func (c *StringNullable[T]) chconnType() string {
	return "StringNullable[" + reflect.TypeOf((*T)(nil)).Elem().String() + "]"
}

func (c *StringNullable[T]) structType() string {
	return strings.ReplaceAll(helper.NullableTypeStr, "<type>", c.dataColumn.structType())
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *StringNullable[T]) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(c.writerData)
	if err != nil {
		return int64(n), fmt.Errorf("write nullable data: %w", err)
	}

	nw, err := c.dataColumn.WriteTo(w)
	return nw + int64(n), err
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *StringNullable[T]) HeaderWriter(w *readerwriter.Writer) {
}

func (c *StringNullable[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *StringNullable[T]) FullType() string {
	if len(c.name) == 0 {
		return "Nullable(" + c.dataColumn.FullType() + ")"
	}
	return string(c.name) + " Nullable(" + c.dataColumn.FullType() + ")"
}

func (c *StringNullable[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	if c.RowIsNil(row) {
		return append(b, "null"...)
	}
	return c.dataColumn.ToJSON(row, ignoreDoubleQuotes, b)
}
