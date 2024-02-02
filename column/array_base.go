package column

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

// ArrayBase is a column of Array(T) ClickHouse data type
//
// ArrayBase is a base class for other arrays or use for none generic use
type ArrayBase struct {
	column
	offsetColumn *Base[uint64]
	dataColumn   ColumnBasic
	offset       uint64
	resetHook    func()
}

// NewArray create a new array column of Array(T) ClickHouse data type
func NewArrayBase(dataColumn ColumnBasic) *ArrayBase {
	a := &ArrayBase{
		dataColumn:   dataColumn,
		offsetColumn: New[uint64](),
	}
	return a
}

// AppendLen Append len of array for insert
func (c *ArrayBase) AppendLen(v int) {
	c.offset += uint64(v)
	c.offsetColumn.Append(c.offset)
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *ArrayBase) Remove(n int) {
	if c.NumRow() == 0 || c.NumRow() <= n {
		return
	}
	var offset uint64
	if n != 0 {
		offset = c.offsetColumn.values[n-1]
	}
	c.offsetColumn.Remove(n)
	c.dataColumn.Remove(int(offset))
}

func (c *ArrayBase) RowAny(row int) any {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	var val []any
	endOffset := c.offsetColumn.Row(row)
	for i := lastOffset; i < endOffset; i++ {
		val = append(val, c.dataColumn.RowAny(int(i)))
	}
	return val
}

func (c *ArrayBase) Scan(row int, dest any) error {
	return c.ScanValue(row, reflect.ValueOf(dest))
}

func (c *ArrayBase) ScanValue(row int, dest reflect.Value) error {
	destValue := reflect.Indirect(dest)
	if destValue.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}
	rowVal := reflect.ValueOf(c.RowAny(row))
	if destValue.Type().AssignableTo(rowVal.Type()) {
		destValue.Set(rowVal)
		return nil
	}

	var lastOffset int
	if row != 0 {
		lastOffset = int(c.offsetColumn.Row(row - 1))
	}
	offset := int(c.offsetColumn.Row(row))

	rSlice := reflect.MakeSlice(destValue.Type(), offset-lastOffset, offset-lastOffset)
	for i, b := lastOffset, 0; i < offset; i, b = i+1, b+1 {
		err := c.dataColumn.Scan(i, rSlice.Index(b).Addr().Interface())
		if err != nil {
			return fmt.Errorf("cannot scan array item %d: %w", i, err)
		}
	}
	destValue.Set(rSlice)

	return nil
}

// NumRow return number of row for this block
func (c *ArrayBase) NumRow() int {
	return c.offsetColumn.NumRow()
}

// Array return a Array type for this column
func (c *ArrayBase) Array() *ArrayBase {
	return NewArrayBase(c)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *ArrayBase) Reset() {
	c.offsetColumn.Reset()
	c.dataColumn.Reset()
	c.offset = 0
}

// Offsets return all the offsets in current block
// Note: Only available in the current block
func (c *ArrayBase) Offsets() []uint64 {
	return c.offsetColumn.Data()
}

// TotalRows return total rows on this block of array data
func (c *ArrayBase) TotalRows() int {
	if c.offsetColumn.totalByte == 0 {
		return 0
	}
	return int(binary.LittleEndian.Uint64(c.offsetColumn.b[c.offsetColumn.totalByte-8 : c.offsetColumn.totalByte]))
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *ArrayBase) SetWriteBufferSize(row int) {
	c.offsetColumn.SetWriteBufferSize(row)
	c.dataColumn.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *ArrayBase) ReadRaw(num int, r *readerwriter.Reader) error {
	c.offsetColumn.Reset()
	err := c.offsetColumn.ReadRaw(num, r)
	if err != nil {
		return fmt.Errorf("array: read offset column: %w", err)
	}
	err = c.dataColumn.ReadRaw(c.TotalRows(), r)
	if err != nil {
		return fmt.Errorf("array: read data column: %w", err)
	}

	if c.resetHook != nil {
		c.resetHook()
	}
	return nil
}

// HeaderReader reads header data from reader
// it uses internally
func (c *ArrayBase) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	c.r = r
	err := c.readColumn(readColumn, revision)
	if err != nil {
		return err
	}

	// never return error
	//nolint:errcheck
	c.offsetColumn.HeaderReader(r, false, revision)

	return c.dataColumn.HeaderReader(r, false, revision)
}

// Column returns the sub column
func (c *ArrayBase) Column() ColumnBasic {
	return c.dataColumn
}

func (c *ArrayBase) Validate() error {
	chType := helper.FilterSimpleAggregate(c.chType)
	switch {
	case helper.IsRing(chType):
		chType = helper.RingMainTypeStr
	case helper.IsPolygon(chType):
		chType = helper.PolygonMainTypeStr
	case helper.IsMultiPolygon(chType):
		chType = helper.MultiPolygonMainTypeStr
	}

	chType = helper.NestedToArrayType(chType)

	if !helper.IsArray(chType) {
		return ErrInvalidType{
			chType:     string(c.chType),
			structType: c.structType(),
		}
	}
	c.dataColumn.SetType(chType[helper.LenArrayStr : len(chType)-1])
	if c.dataColumn.Validate() != nil {
		return ErrInvalidType{
			chType:     string(c.chType),
			structType: c.structType(),
		}
	}
	return nil
}

func (c *ArrayBase) structType() string {
	return strings.ReplaceAll(helper.ArrayTypeStr, "<type>", c.dataColumn.structType())
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *ArrayBase) WriteTo(w io.Writer) (int64, error) {
	nw, err := c.offsetColumn.WriteTo(w)
	if err != nil {
		return 0, fmt.Errorf("write len data: %w", err)
	}
	n, errDataColumn := c.dataColumn.WriteTo(w)

	return nw + n, errDataColumn
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *ArrayBase) HeaderWriter(w *readerwriter.Writer) {
	c.dataColumn.HeaderWriter(w)
}

func (c *ArrayBase) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *ArrayBase) FullType() string {
	if len(c.name) == 0 {
		return "Array(" + c.dataColumn.FullType() + ")"
	}
	return string(c.name) + " Array(" + c.dataColumn.FullType() + ")"
}

func (c *ArrayBase) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	b = append(b, '[')

	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}

	offset := c.offsetColumn.Row(row)
	for i := lastOffset; i < offset; i++ {
		if i != lastOffset {
			b = append(b, ',')
		}
		b = c.dataColumn.ToJSON(int(i), ignoreDoubleQuotes, b)
	}
	return append(b, ']')
}
