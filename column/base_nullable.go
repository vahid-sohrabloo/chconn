package column

import (
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

// BaseNullable is a column of Nullable(T) ClickHouse data type
type BaseNullable[T BaseType] struct {
	column
	numRow               int
	dataColumn           *Base[T]
	values               []byte
	indexRemoveKeepIndex int
}

// NewBaseNullable return new BaseNullable for BaseNullable(T) ClickHouse DataType
func NewBaseNullable[T BaseType](dataColumn *Base[T]) *BaseNullable[T] {
	return &BaseNullable[T]{
		dataColumn: dataColumn,
	}
}

// Data get all the data in current block as a slice.
//
// NOTE: the return slice only valid in current block, if you want to use it after, you should copy it. or use Read
func (c *BaseNullable[T]) Data() []T {
	return c.dataColumn.Data()
}

// Data get all the nullable  data in current block as a slice of pointer.
//
// As an alternative (for better performance).
// You can use `Data` and one of `RowIsNil` and `ReadNil` and `DataNil`  to detect if value is null or not.
func (c *BaseNullable[T]) DataP() []*T {
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
func (c *BaseNullable[T]) Read(value []T) []T {
	return c.dataColumn.Read(value)
}

// ReadP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), You can use `Read` and one of `RowIsNil` and `ReadNil` and `DataNil`
// to detect if value is null or not.
func (c *BaseNullable[T]) ReadP(value []*T) []*T {
	for i := 0; i < c.numRow; i++ {
		value = append(value, c.RowP(i))
	}
	return value
}

// Append value for insert
func (c *BaseNullable[T]) Row(i int) T {
	return c.dataColumn.Row(i)
}

// Append value for insert
func (c *BaseNullable[T]) RowAny(i int) any {
	return c.RowP(i)
}

func (c *BaseNullable[T]) Scan(row int, dest any) error {
	switch d := dest.(type) {
	case *T:
		*d = c.Row(row)
		return nil
	case **T:
		*d = c.RowP(row)
		return nil
	case *any:
		*d = c.Row(row)
		return nil
	case sql.Scanner:
		return d.Scan(c.Row(row))
	}

	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "**" + c.dataColumn.rtype.String(),
	}
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `RowIsNil()` to check if it is null.
func (c *BaseNullable[T]) RowP(row int) *T {
	if c.values[row] == 1 {
		return nil
	}
	val := c.dataColumn.Row(row)
	return &val
}

// ReadAll read all nils state in this block and append to the input
func (c *BaseNullable[T]) ReadNil(value []bool) []bool {
	return append(value, *(*[]bool)(unsafe.Pointer(&c.values))...)
}

// DataNil get all nil state in this block
func (c *BaseNullable[T]) DataNil() []bool {
	return *(*[]bool)(unsafe.Pointer(&c.values))
}

// RowIsNil return true if the row is null
func (c *BaseNullable[T]) RowIsNil(row int) bool {
	return c.values[row] == 1
}

// Append value for insert
func (c *BaseNullable[T]) Append(v T) {
	c.preHookAppend()
	c.values = append(c.values, 0)
	c.dataColumn.Append(v)
}

func (c *BaseNullable[T]) canAppend(value any) bool {
	switch value.(type) {
	case nil:
		return true
	case T:
		return true
	case *T:
		return true
	}
	return false
}

func (c *BaseNullable[T]) AppendAny(value any) error {
	switch v := value.(type) {
	case nil:
		c.AppendNil()

		return nil
	case T:
		c.Append(v)

		return nil
	case *T:
		c.AppendP(v)

		return nil
	}

	val := reflect.ValueOf(value)
	valueKind := val.Kind()
	if valueKind == reflect.Ptr {
		value = reflect.ValueOf(value).Elem().Interface()
	}

	if value == nil {
		c.AppendNil()

		return nil
	}

	return c.dataColumn.AppendAny(value)
}

// AppendMulti value for insert
func (c *BaseNullable[T]) AppendMulti(v ...T) {
	c.preHookAppendMulti(len(v))
	c.values = append(c.values, make([]uint8, len(v))...)
	c.dataColumn.AppendMulti(v...)
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *BaseNullable[T]) Remove(n int) {
	if c.NumRow() == 0 || c.NumRow() <= n {
		return
	}
	c.values = c.values[:n]
	c.dataColumn.Remove(n)
}

func (c *BaseNullable[T]) Delete(start, end int) {
	if c.NumRow() == 0 || c.NumRow() <= start {
		return
	}
	if end > c.NumRow() {
		end = c.NumRow()
	}
	c.values = slices.Delete(c.values, start, end)
	c.dataColumn.Delete(start, end)
}

func (c *BaseNullable[T]) DeleteFunc(del func(row int) bool) {
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

func (c *BaseNullable[T]) startBatchDelete() {
	c.indexRemoveKeepIndex = 0
	c.dataColumn.startBatchDelete()
}

func (c *BaseNullable[T]) batchDeleteKeep(start, end int) {
	for i := start; i < end; i++ {
		c.values[c.indexRemoveKeepIndex] = c.values[i]
		c.indexRemoveKeepIndex++
	}
	c.dataColumn.batchDeleteKeep(start, end)
}

func (c *BaseNullable[T]) endBatchDelete() {
	if c.indexRemoveKeepIndex == 0 {
		return
	}
	clear(c.values[c.indexRemoveKeepIndex:]) // zero/nil out the obsolete elements, for GC
	c.values = c.values[:c.indexRemoveKeepIndex]
	c.numRow = len(c.values)
	c.dataColumn.endBatchDelete()
}

// AppendP nullable value for insert
//
// as an alternative (for better performance), you can use `Append` and `AppendNil` to insert a value
func (c *BaseNullable[T]) AppendP(v *T) {
	if v == nil {
		c.AppendNil()
		return
	}
	c.Append(*v)
}

// AppendMultiP nullable value for insert
//
// as an alternative (for better performance), you can use `Append` and `AppendNil` to insert a value
func (c *BaseNullable[T]) AppendMultiP(v ...*T) {
	for _, v := range v {
		if v == nil {
			c.AppendNil()
			continue
		}
		c.Append(*v)
	}
}

// Append nil value for insert
func (c *BaseNullable[T]) AppendNil() {
	c.preHookAppend()
	c.values = append(c.values, 1)
	var emptyValue T
	c.dataColumn.Append(emptyValue)
}

func (c *BaseNullable[T]) SetAt(row int, v T) {
	c.values[row] = 0
	c.dataColumn.SetAt(row, v)
}

func (c *BaseNullable[T]) SetNilAt(row int) {
	c.values[row] = 1
	var emptyValue T
	c.dataColumn.SetAt(row, emptyValue)
}

func (c *BaseNullable[T]) SetAtP(row int, v *T) {
	if v == nil {
		c.values[row] = 1
		var emptyValue T
		c.dataColumn.SetAt(row, emptyValue)
		return
	}
	c.values[row] = 0
	c.dataColumn.SetAt(row, *v)
}

// NumRow return number of row for this block
func (c *BaseNullable[T]) NumRow() int {
	return c.dataColumn.NumRow()
}

// Array return a Array type for this column
func (c *BaseNullable[T]) Array() *ArrayNullable[T] {
	return NewArrayNullable[T](c)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *BaseNullable[T]) Reset() {
	c.values = c.values[:0]
	c.numRow = 0
	c.dataColumn.Reset()
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *BaseNullable[T]) SetWriteBufferSize(row int) {
	if cap(c.values) < row {
		c.values = make([]byte, 0, row)
	}
	c.dataColumn.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *BaseNullable[T]) ReadRaw(num int) error {
	c.Reset()
	c.numRow = num

	err := c.readBuffer()
	if err != nil {
		return fmt.Errorf("read nullable data: %w", err)
	}
	return c.dataColumn.ReadRaw(num)
}

func (c *BaseNullable[T]) readBuffer() error {
	c.values = helper.ResetSlice(c.values, c.numRow, false)
	_, err := c.r.Read(c.values)
	if err != nil {
		return fmt.Errorf("read nullable data: %w", err)
	}
	return nil
}

// ReadHeader reads header data from reader
// it uses internally
func (c *BaseNullable[T]) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	c.r = r
	err := c.column.ReadHeader(r, serverInfo)
	if err != nil {
		return err
	}

	return c.dataColumn.ReadHeader(r, serverInfo)
}

func (c *BaseNullable[T]) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	chType := helper.FilterSimpleAggregate(c.columnHeader.ChType)
	if !helper.IsNullable(chType) {
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			goToChType: c.structType(),
			chconnType: c.chconnType(),
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

func (c *BaseNullable[T]) ValidateInsert() error {
	return c.dataColumn.ValidateInsert()
}

func (c *BaseNullable[T]) chconnType() string {
	return "column.BaseNullable[" + c.dataColumn.rtype.String() + "]"
}

func (c *BaseNullable[T]) structType() string {
	return strings.ReplaceAll(helper.NullableTypeStr, "<type>", c.dataColumn.structType())
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *BaseNullable[T]) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(c.values)
	if err != nil {
		return int64(n), fmt.Errorf("write nullable data: %w", err)
	}

	nw, err := c.dataColumn.WriteTo(w)
	return nw + int64(n), err
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *BaseNullable[T]) HeaderWriter(w *readerwriter.Writer) {
}

func (c *BaseNullable[T]) elem(arrayLevel int) ColumnCore {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *BaseNullable[T]) FullType() string {
	if len(c.columnHeader.Name) == 0 {
		return "Nullable(" + c.dataColumn.FullType() + ")"
	}
	return string(c.columnHeader.Name) + " Nullable(" + c.dataColumn.FullType() + ")"
}

func (c *BaseNullable[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	if c.RowIsNil(row) {
		return append(b, "null"...)
	}
	return c.dataColumn.ToJSON(row, ignoreDoubleQuotes, b)
}

func (c *BaseNullable[T]) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexNullable))
	c.dataColumn.writeBinaryDataTo(w)
}
