package column

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

// BaseNullable is a column of Nullable(T) ClickHouse data type
type BaseNullable[T BaseType] struct {
	column
	numRow     int
	dataColumn *Base[T]
	writerData []byte
	b          []byte
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
	if c.RowIsNil(row) {
		return nil
	}
	return c.dataColumn.Scan(row, dest)
}

func (c *BaseNullable[T]) ScanValue(row int, dest reflect.Value) error {
	if c.RowIsNil(row) {
		return nil
	}
	return c.dataColumn.ScanValue(row, dest)
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `RowIsNil()` to check if it is null.
func (c *BaseNullable[T]) RowP(row int) *T {
	if c.b[row] == 1 {
		return nil
	}
	val := c.dataColumn.Row(row)
	return &val
}

// ReadAll read all nils state in this block and append to the input
func (c *BaseNullable[T]) ReadNil(value []bool) []bool {
	return append(value, *(*[]bool)(unsafe.Pointer(&c.b))...)
}

// DataNil get all nil state in this block
func (c *BaseNullable[T]) DataNil() []bool {
	return *(*[]bool)(unsafe.Pointer(&c.b))
}

// RowIsNil return true if the row is null
func (c *BaseNullable[T]) RowIsNil(row int) bool {
	return c.b[row] == 1
}

// Append value for insert
func (c *BaseNullable[T]) Append(v T) {
	c.preHookAppend()
	c.writerData = append(c.writerData, 0)
	c.dataColumn.Append(v)
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
	//nolint:gocritic // to ignore caseOrder
	case bool:
		if c.dataColumn.kind == reflect.Int8 || c.dataColumn.kind == reflect.Uint8 {
			var tmp T
			if v {
				tmp = *(*T)(unsafe.Pointer(&[]byte{1}[0]))
			} else {
				tmp = *(*T)(unsafe.Pointer(&[]byte{0}[0]))
			}
			c.Append(tmp)
			return nil
		}
	case *bool:
		if c.dataColumn.kind == reflect.Int8 || c.dataColumn.kind == reflect.Uint8 {
			if v == nil {
				c.AppendNil()
				return nil
			}
			var tmp T
			if *v {
				tmp = *(*T)(unsafe.Pointer(&[]byte{1}[0]))
			} else {
				tmp = *(*T)(unsafe.Pointer(&[]byte{0}[0]))
			}
			c.Append(tmp)
			return nil
		}
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
	c.writerData = append(c.writerData, make([]uint8, len(v))...)
	c.dataColumn.AppendMulti(v...)
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *BaseNullable[T]) Remove(n int) {
	if c.NumRow() == 0 || c.NumRow() <= n {
		return
	}
	c.writerData = c.writerData[:n]
	c.dataColumn.Remove(n)
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
	c.writerData = append(c.writerData, 1)
	c.dataColumn.appendEmpty()
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
	c.b = c.b[:0]
	c.numRow = 0
	c.writerData = c.writerData[:0]
	c.dataColumn.Reset()
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *BaseNullable[T]) SetWriteBufferSize(row int) {
	if cap(c.writerData) < row {
		c.writerData = make([]byte, 0, row)
	}
	c.dataColumn.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *BaseNullable[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
	c.r = r
	c.numRow = num

	err := c.readBuffer()
	if err != nil {
		return fmt.Errorf("read nullable data: %w", err)
	}
	return c.dataColumn.ReadRaw(num, r)
}

func (c *BaseNullable[T]) readBuffer() error {
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
func (c *BaseNullable[T]) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	c.r = r
	err := c.readColumn(readColumn, revision)
	if err != nil {
		return err
	}
	return c.dataColumn.HeaderReader(r, false, revision)
}

func (c *BaseNullable[T]) Validate(forInsert bool) error {
	chType := helper.FilterSimpleAggregate(c.chType)
	if !helper.IsNullable(chType) {
		return &ErrInvalidType{
			chType:     string(c.chType),
			goToChType: c.structType(),
			chconnType: c.chconnType(),
		}
	}
	c.dataColumn.SetType(chType[helper.LenNullableStr : len(chType)-1])
	if err := c.dataColumn.Validate(forInsert); err != nil {
		if !isInvalidType(err) {
			return err
		}
		return &ErrInvalidType{
			chType:     string(c.chType),
			goToChType: c.structType(),
			chconnType: c.chconnType(),
		}
	}
	return nil
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
	n, err := w.Write(c.writerData)
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

func (c *BaseNullable[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *BaseNullable[T]) FullType() string {
	if len(c.name) == 0 {
		return "Nullable(" + c.dataColumn.FullType() + ")"
	}
	return string(c.name) + " Nullable(" + c.dataColumn.FullType() + ")"
}

func (c *BaseNullable[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	if c.RowIsNil(row) {
		return append(b, "null"...)
	}
	return c.dataColumn.ToJSON(row, ignoreDoubleQuotes, b)
}
