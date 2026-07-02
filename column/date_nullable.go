package column

import (
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

// DateNullable is a column of Nullable(T) ClickHouse data type
type DateNullable[T DateType[T]] struct {
	column
	numRow               int
	dataColumn           *Date[T]
	values               []byte
	indexRemoveKeepIndex int
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
	switch dest := dest.(type) {
	case *T:
		*dest = c.dataColumn.Base.Row(row)
		return nil
	case **T:
		if c.values[row] == 1 {
			*dest = nil
			return nil
		}
		val := c.dataColumn.Base.Row(row)
		*dest = &val
		return nil
	case *time.Time:
		*dest = c.Row(row)
		return nil
	case **time.Time:
		*dest = c.RowP(row)
		return nil
	case *any:
		*dest = c.RowP(row)
		return nil
	case sql.Scanner:
		return dest.Scan(c.RowP(row))
	}

	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "**" + c.dataColumn.rtype.String() + " or **time.Time",
	}
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `RowIsNil()` to check if it is null.
func (c *DateNullable[T]) RowP(row int) *time.Time {
	if c.values[row] == 1 {
		return nil
	}
	val := c.dataColumn.Row(row)
	return &val
}

// ReadAll read all nils state in this block and append to the input
func (c *DateNullable[T]) ReadNil(value []bool) []bool {
	return append(value, *(*[]bool)(unsafe.Pointer(&c.values))...)
}

// DataNil get all nil state in this block
func (c *DateNullable[T]) DataNil() []bool {
	return *(*[]bool)(unsafe.Pointer(&c.values))
}

// RowIsNil return true if the row is null
func (c *DateNullable[T]) RowIsNil(row int) bool {
	return c.values[row] == 1
}

// Append value for insert
func (c *DateNullable[T]) Append(v time.Time) {
	c.preHookAppend()
	c.values = append(c.values, 0)
	c.dataColumn.Append(v)
}

func (c *DateNullable[T]) canAppend(value any) bool {
	switch value.(type) {
	case nil:
		return true
	case T:
		return true
	case *T:
		return true
	case time.Time:
		return true
	case *time.Time:
		return true
	case int64:
		return true
	case *int64:
		return true
	default:
		return false
	}
}

func (c *DateNullable[T]) AppendAny(value any) error {
	switch v := value.(type) {
	case T:
		c.Append(v.ToTime(c.dataColumn.loc, c.dataColumn.precision))

		return nil
	case *T:
		if v == nil {
			c.AppendNil()

			return nil
		}
		t := (*v).ToTime(c.dataColumn.loc, c.dataColumn.precision)
		c.AppendP(&t)
	case nil:
		c.AppendNil()

		return nil
	case time.Time:
		c.Append(v)

		return nil
	case *time.Time:
		c.AppendP(v)

		return nil
	case *int64:
		if v == nil {
			c.AppendNil()

			return nil
		}
		c.Append(time.Unix(*v, 0))
	case int64:
		c.Append(time.Unix(v, 0))

		return nil
	default:
		return fmt.Errorf("invalid type %T", value)
	}
	return nil
}

// AppendMulti value for insert
func (c *DateNullable[T]) AppendMulti(v ...time.Time) {
	c.preHookAppend()
	c.values = append(c.values, make([]uint8, len(v))...)
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
	c.values = c.values[:n]
	c.dataColumn.Remove(n)
	c.numRow = len(c.values)
}

func (c *DateNullable[T]) Delete(start, end int) {
	if c.NumRow() == 0 || c.NumRow() <= start {
		return
	}
	if end > c.NumRow() {
		end = c.NumRow()
	}
	c.values = slices.Delete(c.values, start, end)
	c.dataColumn.Delete(start, end)
	c.numRow = len(c.values)
}

func (c *DateNullable[T]) DeleteFunc(del func(row int) bool) {
	if c.NumRow() == 0 {
		return
	}
	i := 0
	for j := 0; j < len(c.values); j++ {
		if !del(j) {
			c.values[i] = c.values[j]
			i++
		}
	}
	clear(c.values[i:]) // zero/nil out the obsolete elements, for GC
	c.values = c.values[:i]
	c.numRow = len(c.values)
	c.dataColumn.DeleteFunc(del)
}

func (c *DateNullable[T]) startBatchDelete() {
	c.indexRemoveKeepIndex = 0
	c.dataColumn.startBatchDelete()
}

func (c *DateNullable[T]) batchDeleteKeep(start, end int) {
	for i := start; i < end; i++ {
		c.values[c.indexRemoveKeepIndex] = c.values[i]
		c.indexRemoveKeepIndex++
	}
	c.dataColumn.batchDeleteKeep(start, end)
}

func (c *DateNullable[T]) endBatchDelete() {
	if c.indexRemoveKeepIndex == 0 {
		return
	}
	clear(c.values[c.indexRemoveKeepIndex:]) // zero/nil out the obsolete elements, for GC
	c.values = c.values[:c.indexRemoveKeepIndex]
	c.numRow = len(c.values)
	c.dataColumn.endBatchDelete()
}

// Append nil value for insert
func (c *DateNullable[T]) AppendNil() {
	c.preHookAppend()
	c.values = append(c.values, 1)
	var emptyValue T
	c.dataColumn.Base.Append(emptyValue)
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
	c.numRow = 0
	c.values = c.values[:0]
	c.dataColumn.Reset()
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *DateNullable[T]) SetWriteBufferSize(row int) {
	if cap(c.values) < row {
		c.values = make([]byte, 0, row)
	}
	c.dataColumn.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *DateNullable[T]) ReadRaw(num int) error {
	c.Reset()
	c.numRow = num

	err := c.readBuffer()
	if err != nil {
		return fmt.Errorf("read nullable data: %w", err)
	}
	return c.dataColumn.ReadRaw(num)
}

func (c *DateNullable[T]) readBuffer() error {
	c.values = helper.ResetSlice(c.values, c.numRow, false)
	_, err := c.r.Read(c.values)
	if err != nil {
		return fmt.Errorf("read nullable data: %w", err)
	}
	return nil
}

// ReadHeader reads header data from reader
// it uses internally
func (c *DateNullable[T]) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	err := c.column.ReadHeader(r, serverInfo)
	if err != nil {
		return err
	}

	return c.dataColumn.ReadHeader(r, serverInfo)
}

func (c *DateNullable[T]) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	chType := helper.FilterSimpleAggregate(c.columnHeader.ChType)
	if !helper.IsNullable(chType) {
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}

	if err := c.dataColumn.SetColumnHeader(ColumnHeader{
		ChType: chType[helper.LenNullableStr : len(chType)-1],
	}); err != nil {
		if !isInvalidType(err) {
			return err
		}
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			goToChType: c.structType(),
			chconnType: c.chconnType(),
		}
	}
	return nil
}

func (c *DateNullable[T]) ValidateInsert() error {
	return c.dataColumn.ValidateInsert()
}

func (c *DateNullable[T]) chconnType() string {
	return "DateNullable[" + reflect.TypeFor[T]().String() + "]"
}

func (c *DateNullable[T]) structType() string {
	return strings.ReplaceAll(helper.NullableTypeStr, "<type>", c.dataColumn.structType())
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *DateNullable[T]) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(c.values)
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

func (c *DateNullable[T]) elem(arrayLevel int) ColumnCore {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *DateNullable[T]) FullType() string {
	if len(c.columnHeader.Name) == 0 {
		return "Nullable(" + c.dataColumn.FullType() + ")"
	}
	return string(c.columnHeader.Name) + " Nullable(" + c.dataColumn.FullType() + ")"
}

func (c DateNullable[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	if c.RowIsNil(row) {
		return append(b, "null"...)
	}
	return c.dataColumn.ToJSON(row, ignoreDoubleQuotes, b)
}

func (c *DateNullable[T]) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexNullable))
	c.dataColumn.writeBinaryDataTo(w)
}
