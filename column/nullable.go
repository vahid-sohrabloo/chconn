package column

import (
	"fmt"
	"io"
	"strings"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v2/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
)

type appendEmptyInterface interface {
	appendEmpty()
}

// Nullable is a column of Nullable(T) ClickHouse data type
type Nullable[T comparable] struct {
	column
	numRow     int
	dataColumn Column[T]
	writerData []byte
	b          []byte
}

// NewNullable return new Nullable for Nullable(T) ClickHouse DataType
func NewNullable[T comparable](dataColumn Column[T]) *Nullable[T] {
	return &Nullable[T]{
		dataColumn: dataColumn,
	}
}

// Data get all the data in current block as a slice.
//
// NOTE: the return slice only valid in current block, if you want to use it after, you should copy it. or use Read
func (c *Nullable[T]) Data() []T {
	return c.dataColumn.Data()
}

// Data get all the nullable  data in current block as a slice of pointer.
//
// As an alternative (for better performance).
// You can use `Data` and one of `RowIsNil` and `ReadNil` and `DataNil`  to detect if value is null or not.
func (c *Nullable[T]) DataP() []*T {
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
func (c *Nullable[T]) Read(value []T) []T {
	return c.dataColumn.Read(value)
}

// ReadP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), You can use `Read` and one of `RowIsNil` and `ReadNil` and `DataNil`
// to detect if value is null or not.
func (c *Nullable[T]) ReadP(value []*T) []*T {
	for i := 0; i < c.numRow; i++ {
		value = append(value, c.RowP(i))
	}
	return value
}

// Append value for insert
func (c *Nullable[T]) Row(i int) T {
	return c.dataColumn.Row(i)
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `RowIsNil()` to check if it is null.
func (c *Nullable[T]) RowP(row int) *T {
	if c.b[row] == 1 {
		return nil
	}
	val := c.dataColumn.Row(row)
	return &val
}

// ReadAll read all nils state in this block and append to the input
func (c *Nullable[T]) ReadNil(value []bool) []bool {
	return append(value, *(*[]bool)(unsafe.Pointer(&c.b))...)
}

// DataNil get all nil state in this block
func (c *Nullable[T]) DataNil() []bool {
	return *(*[]bool)(unsafe.Pointer(&c.b))
}

// RowIsNil return true if the row is null
func (c *Nullable[T]) RowIsNil(row int) bool {
	return c.b[row] == 1
}

// Append value for insert
func (c *Nullable[T]) Append(v ...T) {
	c.writerData = append(c.writerData, make([]uint8, len(v))...)
	c.dataColumn.Append(v...)
}

// Append nullable value for insert
//
// as an alternative (for better performance), you can use `Append` and `AppendNil` to insert a value
func (c *Nullable[T]) AppendP(v ...*T) {
	for _, v := range v {
		if v == nil {
			c.AppendNil()
			continue
		}
		c.Append(*v)
	}
}

// Append nil value for insert
func (c *Nullable[T]) AppendNil() {
	c.writerData = append(c.writerData, 1)
	c.dataColumn.(appendEmptyInterface).appendEmpty()
}

// NumRow return number of row for this block
func (c *Nullable[T]) NumRow() int {
	return c.dataColumn.NumRow()
}

// Array return a Array type for this column
func (c *Nullable[T]) Array() *ArrayNullable[T] {
	return NewArrayNullable[T](c)
}

// LC return a low cardinality type for this column
func (c *Nullable[T]) LC() *LowCardinalityNullable[T] {
	return NewLowCardinalityNullable(c.dataColumn)
}

// LowCardinality return a low cardinality type for this column
func (c *Nullable[T]) LowCardinality() *LowCardinalityNullable[T] {
	return NewLowCardinalityNullable(c.dataColumn)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *Nullable[T]) Reset() {
	c.b = c.b[:0]
	c.numRow = 0
	c.writerData = c.writerData[:0]
	c.dataColumn.Reset()
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *Nullable[T]) SetWriteBufferSize(row int) {
	if cap(c.writerData) < row {
		c.writerData = make([]byte, 0, row)
	}
	c.dataColumn.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *Nullable[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
	c.r = r
	c.numRow = num

	err := c.readBuffer()
	if err != nil {
		return fmt.Errorf("read nullable data: %w", err)
	}
	return c.dataColumn.ReadRaw(num, r)
}

func (c *Nullable[T]) readBuffer() error {
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
func (c *Nullable[T]) HeaderReader(r *readerwriter.Reader, readColumn bool) error {
	c.r = r
	err := c.readColumn(readColumn)
	if err != nil {
		return err
	}
	return c.dataColumn.HeaderReader(r, false)
}

func (c *Nullable[T]) Validate() error {
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

func (c *Nullable[T]) columnType() string {
	return strings.Replace(helper.NullableTypeStr, "<type>", c.dataColumn.columnType(), -1)
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *Nullable[T]) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(c.writerData)
	if err != nil {
		return int64(n), fmt.Errorf("write nullable data: %w", err)
	}

	nw, err := c.dataColumn.WriteTo(w)
	return nw + int64(n), err
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *Nullable[T]) HeaderWriter(w *readerwriter.Writer) {
}

func (c *Nullable[T]) elem(arrayLevel int, lc bool) ColumnBasic {
	if lc {
		return c.LowCardinality().elem(arrayLevel)
	}
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}
