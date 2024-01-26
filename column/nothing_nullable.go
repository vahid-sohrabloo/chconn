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
// NOTE: it always return slice of zero value of NothingData
func (c *NothingNullable) Data() []NothingData {
	return c.dataColumn.Data()
}

// Data get all the nullable  data in current block as a slice of pointer.
//
// NOTE: it always return slice of nil
func (c *NothingNullable) DataP() []*NothingData {
	return make([]*NothingData, c.numRow)
}

// Read reads all the data in current block and append to the input.
//
// NOTE: it always append zero value of NothingData
func (c *NothingNullable) Read(value []NothingData) []NothingData {
	return c.dataColumn.Read(value)
}

// ReadP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), You can use `Read` and one of `RowIsNil` and `ReadNil` and `DataNil`
// to detect if value is null or not.
func (c *NothingNullable) ReadP(value []*NothingData) []*NothingData {
	return append(value, make([]*NothingData, c.numRow)...)
}

// Append value for insert
func (c *NothingNullable) Row(i int) NothingData {
	return c.dataColumn.Row(i)
}

// Append value for insert
func (c *NothingNullable) RowAny(i int) any {
	return c.RowP(i)
}

func (c *NothingNullable) Scan(row int, dest any) error {
	return nil
}

func (c *NothingNullable) ScanValue(row int, dest reflect.Value) error {
	return nil
}

// RowP return the value of given row for nullable data
//
// NOTE: Row number start from zero
//
// NOTE: it always return nil
func (c *NothingNullable) RowP(row int) *NothingData {
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
func (c *NothingNullable) Append(v NothingData) {
}

// AppendMulti value for insert
//
// Should not use this method. NothingNullable column is only for select query
func (c *NothingNullable) AppendMulti(v ...NothingData) {
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
func (c *NothingNullable) AppendP(v *NothingData) {
}

// AppendMultiP nullable value for insert
//
// as an alternative (for better performance), you can use `Append` and `AppendNil` to insert a value
//
// Should not use this method. NothingNullable column is only for select query
func (c *NothingNullable) AppendMultiP(v ...*NothingData) {
}

// Append nil value for insert
//
// Should not use this method. NothingNullable column is only for select query
func (c *NothingNullable) AppendNil() {
}

// NumRow return number of row for this block
func (c *NothingNullable) NumRow() int {
	return c.dataColumn.NumRow()
}

// Array return a Array type for this column
func (c *NothingNullable) Array() *ArrayNullable[NothingData] {
	return NewArrayNullable[NothingData](c)
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
//
// NOTE: Should not use this method. NothingNullable column is only for select query
func (c *NothingNullable) SetWriteBufferSize(row int) {
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
			chType: string(c.chType),
		}
	}
	c.dataColumn.SetType(chType[helper.LenNullableStr : len(chType)-1])
	if c.dataColumn.Validate() != nil {
		return ErrInvalidType{
			structType: c.structType(),
		}
	}
	return nil
}

func (c *NothingNullable) structType() string {
	return strings.ReplaceAll(helper.NullableTypeStr, "<type>", c.dataColumn.structType())
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *NothingNullable) WriteTo(w io.Writer) (int64, error) {
	return 0, fmt.Errorf("NothingNullable column is only for select query")
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
	if len(c.name) == 0 {
		return "Nullable(" + c.dataColumn.FullType() + ")"
	}
	return string(c.name) + " Nullable(" + c.dataColumn.FullType() + ")"

}
