package column

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

// DateNullable is a column of Nullable(T) ClickHouse data type
type DateNullable[T DateType[T]] struct {
	column
	numRow     int
	dataColumn *Date[T]
	writerData []byte
	b          []byte
}

// NewDateNullable return new DateNullable for DateNullable(T) ClickHouse DataType
func NewDateNullable[T DateType[T]](dataColumn *Date[T]) *DateNullable[T] {
	return &DateNullable[T]{
		dataColumn: dataColumn,
	}
}

// Data get all the data in current block as a slice.
//
// NOTE: the return slice only valid in current block, if you want to use it after, you should copy it. or use Read
func (c *DateNullable[T]) Data() []time.Time {
	return c.dataColumn.Data()
}

// Data get all the nullable  data in current block as a slice of pointer.
//
// As an alternative (for better performance).
// You can use `Data` and one of `RowIsNil` and `ReadNil` and `DataNil`  to detect if value is null or not.
func (c *DateNullable[T]) DataP() []*time.Time {
	val := make([]*time.Time, c.numRow)
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
func (c *DateNullable[T]) Read(value []time.Time) []time.Time {
	return c.dataColumn.Read(value)
}

// ReadP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), You can use `Read` and one of `RowIsNil` and `ReadNil` and `DataNil`
// to detect if value is null or not.
func (c *DateNullable[T]) ReadP(value []*time.Time) []*time.Time {
	for i := 0; i < c.numRow; i++ {
		value = append(value, c.RowP(i))
	}
	return value
}

// Row return the value of given row
func (c *DateNullable[T]) Row(i int) time.Time {
	return c.dataColumn.Row(i)
}

// RowAny return the value of given row
func (c *DateNullable[T]) RowAny(i int) any {
	return c.RowP(i)
}

func (c *DateNullable[T]) Scan(row int, dest any) error {
	if c.RowIsNil(row) {
		return nil
	}
	return c.dataColumn.Scan(row, dest)
}

func (c *DateNullable[T]) ScanValue(row int, dest reflect.Value) error {
	if c.RowIsNil(row) {
		return nil
	}
	return c.dataColumn.ScanValue(row, dest)
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `RowIsNil()` to check if it is null.
func (c *DateNullable[T]) RowP(row int) *time.Time {
	if c.b[row] == 1 {
		return nil
	}
	val := c.dataColumn.Row(row)
	return &val
}

// ReadAll read all nils state in this block and append to the input
func (c *DateNullable[T]) ReadNil(value []bool) []bool {
	return append(value, *(*[]bool)(unsafe.Pointer(&c.b))...)
}

// DataNil get all nil state in this block
func (c *DateNullable[T]) DataNil() []bool {
	return *(*[]bool)(unsafe.Pointer(&c.b))
}

// RowIsNil return true if the row is null
func (c *DateNullable[T]) RowIsNil(row int) bool {
	return c.b[row] == 1
}

// Append value for insert
func (c *DateNullable[T]) Append(v time.Time) {
	c.preHookAppend()
	c.writerData = append(c.writerData, 0)
	c.dataColumn.Append(v)
}

// AppendMulti value for insert
func (c *DateNullable[T]) AppendMulti(v ...time.Time) {
	c.preHookAppend()
	c.writerData = append(c.writerData, make([]uint8, len(v))...)
	c.dataColumn.AppendMulti(v...)
}

// AppendP nullable value for insert
//
// as an alternative (for better performance), you can use `Append` and `AppendNil` to insert a value
func (c *DateNullable[T]) AppendP(v *time.Time) {
	if v == nil {
		c.AppendNil()
		return
	}
	c.Append(*v)
}

// AppendMultiP nullable value for insert
//
// as an alternative (for better performance), you can use `Append` and `AppendNil` to insert a value
func (c *DateNullable[T]) AppendMultiP(v ...*time.Time) {
	for _, v := range v {
		if v == nil {
			c.AppendNil()
			continue
		}
		c.Append(*v)
	}
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *DateNullable[T]) Remove(n int) {
	if c.NumRow() == 0 || c.NumRow() <= n {
		return
	}
	c.writerData = c.writerData[:n]
	c.dataColumn.Remove(n)
	c.numRow = len(c.writerData)
}

// Append nil value for insert
func (c *DateNullable[T]) AppendNil() {
	c.preHookAppend()
	c.writerData = append(c.writerData, 1)
	c.dataColumn.appendEmpty()
}

// NumRow return number of row for this block
func (c *DateNullable[T]) NumRow() int {
	return c.dataColumn.NumRow()
}

// Array return a Array type for this column
func (c *DateNullable[T]) Array() *ArrayNullable[time.Time] {
	return NewArrayNullable[time.Time](c)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *DateNullable[T]) Reset() {
	c.b = c.b[:0]
	c.numRow = 0
	c.writerData = c.writerData[:0]
	c.dataColumn.Reset()
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *DateNullable[T]) SetWriteBufferSize(row int) {
	if cap(c.writerData) < row {
		c.writerData = make([]byte, 0, row)
	}
	c.dataColumn.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *DateNullable[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
	c.r = r
	c.numRow = num

	err := c.readBuffer()
	if err != nil {
		return fmt.Errorf("read nullable data: %w", err)
	}
	return c.dataColumn.ReadRaw(num, r)
}

func (c *DateNullable[T]) readBuffer() error {
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
func (c *DateNullable[T]) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	c.r = r
	err := c.readColumn(readColumn, revision)
	if err != nil {
		return err
	}
	return c.dataColumn.HeaderReader(r, false, revision)
}

func (c *DateNullable[T]) Validate(forInsert bool) error {
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

func (c *DateNullable[T]) chconnType() string {
	return "DateNullable[" + reflect.TypeOf((*T)(nil)).Elem().String() + "]"
}

func (c *DateNullable[T]) structType() string {
	return strings.ReplaceAll(helper.NullableTypeStr, "<type>", c.dataColumn.structType())
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *DateNullable[T]) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(c.writerData)
	if err != nil {
		return int64(n), fmt.Errorf("write nullable data: %w", err)
	}

	nw, err := c.dataColumn.WriteTo(w)
	return nw + int64(n), err
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *DateNullable[T]) HeaderWriter(w *readerwriter.Writer) {
}

func (c *DateNullable[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *DateNullable[T]) FullType() string {
	if len(c.name) == 0 {
		return "Nullable(" + c.dataColumn.FullType() + ")"
	}
	return string(c.name) + " Nullable(" + c.dataColumn.FullType() + ")"
}

func (c DateNullable[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	if c.RowIsNil(row) {
		return append(b, "null"...)
	}
	return c.dataColumn.ToJSON(row, ignoreDoubleQuotes, b)
}
