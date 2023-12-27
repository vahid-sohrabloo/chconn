package column

import (
	"fmt"
	"io"
	"strings"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

// NothingNullable is a column of Nullable(Nothing) ClickHouse data type
type NothingNullable struct {
	column
	numRow     int
	dataColumn *Nothing
	writerData []byte
	b          []byte
}

// NewNothingNullable return new NothingNullable for Nullable(Nothing) ClickHouse DataType
func NewNothingNullable(dataColumn *Nothing) *NothingNullable {
	return &NothingNullable{
		dataColumn: dataColumn,
	}
}

// Data get all the data in current block as a slice.
//
// NOTE: the return slice only valid in current block, if you want to use it after, you should copy it. or use Read
func (c *NothingNullable) Data() []int8 {
	return c.dataColumn.Data()
}

// Data get all the nullable  data in current block as a slice of pointer.
//
// As an alternative (for better performance).
// You can use `Data` and one of `RowIsNil` and `ReadNil` and `DataNil`  to detect if value is null or not.
func (c *NothingNullable) DataP() []*int8 {
	return make([]*int8, c.numRow)
}

// Read reads all the data in current block and append to the input.
func (c *NothingNullable) Read(value []int8) []int8 {
	return c.dataColumn.Read(value)
}

// ReadP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), You can use `Read` and one of `RowIsNil` and `ReadNil` and `DataNil`
// to detect if value is null or not.
func (c *NothingNullable) ReadP(value []*int8) []*int8 {
	return value
}

// Append value for insert
func (c *NothingNullable) Row(i int) int8 {
	return c.dataColumn.Row(i)
}

// Append value for insert
func (c *NothingNullable) RowI(i int) any {
	return c.RowP(i)
}

func (c *NothingNullable) Scan(row int, dest any) error {
	if c.RowIsNil(row) {
		return nil
	}
	return c.dataColumn.Scan(row, dest)
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `RowIsNil()` to check if it is null.
func (c *NothingNullable) RowP(row int) *int8 {
	return nil
}

// ReadAll read all nils state in this block and append to the input
func (c *NothingNullable) ReadNil(value []bool) []bool {
	return append(value, *(*[]bool)(unsafe.Pointer(&c.b))...)
}

// DataNil get all nil state in this block
func (c *NothingNullable) DataNil() []bool {
	return *(*[]bool)(unsafe.Pointer(&c.b))
}

// RowIsNil return true if the row is null
func (c *NothingNullable) RowIsNil(row int) bool {
	return c.b[row] == 1
}

// Append value for insert
//
// Should not use this method. NothingNullable column is only for select query
func (c *NothingNullable) Append(v int8) {
}

// AppendMulti value for insert
//
// Should not use this method. NothingNullable column is only for select query
func (c *NothingNullable) AppendMulti(v ...int8) {
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *NothingNullable) Remove(n int) {
}

// AppendP nullable value for insert
//
// as an alternative (for better performance), you can use `Append` and `AppendNil` to insert a value
//
// Should not use this method. NothingNullable column is only for select query
func (c *NothingNullable) AppendP(v *int8) {
}

// AppendMultiP nullable value for insert
//
// as an alternative (for better performance), you can use `Append` and `AppendNil` to insert a value
//
// Should not use this method. NothingNullable column is only for select query
func (c *NothingNullable) AppendMultiP(v ...*int8) {
}

// Append nil value for insert
//
// Should not use this method. NothingNullable column is only for select query
func (c *NothingNullable) AppendNil() {
	c.writerData = append(c.writerData, 1)
	c.dataColumn.appendEmpty()
}

// NumRow return number of row for this block
func (c *NothingNullable) NumRow() int {
	return c.dataColumn.NumRow()
}

// Array return a Array type for this column
func (c *NothingNullable) Array() *ArrayNullable[int8] {
	return NewArrayNullable[int8](c)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *NothingNullable) Reset() {
	c.b = c.b[:0]
	c.numRow = 0
	c.writerData = c.writerData[:0]
	c.dataColumn.Reset()
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *NothingNullable) SetWriteBufferSize(row int) {
	if cap(c.writerData) < row {
		c.writerData = make([]byte, 0, row)
	}
	c.dataColumn.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *NothingNullable) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
	c.r = r
	c.numRow = num

	err := c.readBuffer()
	if err != nil {
		return fmt.Errorf("read nullable data: %w", err)
	}
	return c.dataColumn.ReadRaw(num, r)
}

func (c *NothingNullable) readBuffer() error {
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
func (c *NothingNullable) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	c.r = r
	err := c.readColumn(readColumn, revision)
	if err != nil {
		return err
	}
	return c.dataColumn.HeaderReader(r, false, revision)
}

func (c *NothingNullable) Validate() error {
	chType := helper.FilterSimpleAggregate(c.chType)
	if !helper.IsNullable(chType) {
		return ErrInvalidType{
			column: c,
		}
	}
	c.dataColumn.SetType(chType[helper.LenNullableStr : len(chType)-1])
	if c.dataColumn.Validate() != nil {
		return ErrInvalidType{
			column: c,
		}
	}
	return nil
}

func (c *NothingNullable) ColumnType() string {
	return strings.ReplaceAll(helper.NullableTypeStr, "<type>", c.dataColumn.ColumnType())
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *NothingNullable) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(c.writerData)
	if err != nil {
		return int64(n), fmt.Errorf("write nullable data: %w", err)
	}

	nw, err := c.dataColumn.WriteTo(w)
	return nw + int64(n), err
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *NothingNullable) HeaderWriter(w *readerwriter.Writer) {
}

func (c *NothingNullable) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *NothingNullable) FullType() string {
	return "Nullable(" + c.dataColumn.FullType() + ")"
}
