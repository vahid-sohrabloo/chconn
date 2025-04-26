package column

import (
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/kelindar/bitmap"
	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

// ArrayBase is a column of Array(T) ClickHouse data type
//
// ArrayBase is a base class for other arrays or use for none generic use
type ArrayBase struct {
	column
	offsetColumn     *Base[uint64]
	dataColumn       ColumnCore
	offset           uint64
	arrayChconnType  string
	resetHook        func()
	bitmapDeleteKeep bitmap.Bitmap
}

// NewArray create a new array column of Array(T) ClickHouse data type
func NewArrayBase(dataColumn ColumnCore) *ArrayBase {
	a := &ArrayBase{
		dataColumn:      dataColumn,
		offsetColumn:    New[uint64](),
		arrayChconnType: "column.ArrayBase",
	}
	return a
}

// AppendLen Append len of array for insert
func (c *ArrayBase) AppendLen(v int) {
	c.offset += uint64(v)
	c.offsetColumn.Append(c.offset)
	c.preHookAppend()
}

func (c *ArrayBase) canAppend(value any) bool {
	sliceVal := reflect.ValueOf(value)
	if sliceVal.Kind() != reflect.Slice {
		return false
	}
	for i := 0; i < sliceVal.Len(); i++ {
		item := sliceVal.Index(i).Interface()
		if !c.dataColumn.canAppend(item) {
			return false
		}
	}
	return true
}

func (c *ArrayBase) AppendAny(value any) error {
	sliceVal := reflect.ValueOf(value)
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("value is not a slice")
	}

	for i := 0; i < sliceVal.Len(); i++ {
		item := sliceVal.Index(i).Interface()
		err := c.dataColumn.AppendAny(item)
		if err != nil {
			return fmt.Errorf("cannot append array item %v: %w", item, err)
		}
	}

	c.AppendLen(sliceVal.Len())

	return nil
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

// Delete removes rows in the range [start, end)
func (c *ArrayBase) Delete(start int, end int) {
	if c.NumRow() == 0 || c.NumRow() <= start || start >= end {
		return
	}

	if end > c.NumRow() {
		end = c.NumRow()
	}

	// Calculate offsets for the data elements
	startOffset := uint64(0)
	if start > 0 {
		startOffset = c.offsetColumn.values[start-1]
	}
	endOffset := c.offsetColumn.values[end-1]

	// Calculate how many data elements we're removing
	elementsToRemove := endOffset - startOffset

	// Remove the data elements
	c.dataColumn.Delete(int(startOffset), int(endOffset))

	// Remove the offsets for the deleted rows
	c.offsetColumn.Delete(start, end)

	// Adjust all remaining offsets after the deleted range
	for i := start; i < c.offsetColumn.NumRow(); i++ {
		c.offsetColumn.values[i] -= elementsToRemove
	}
}

func (c *ArrayBase) DeleteFunc(del func(row int) bool) {
	if c.NumRow() == 0 {
		return
	}

	c.startBatchDelete()
	for i := 0; i < c.NumRow(); i++ {
		if !del(i) {
			// Keep this row
			c.bitmapDeleteKeep.Set(uint32(i))
		}
	}
	c.endBatchDelete()
}

func (c *ArrayBase) startBatchDelete() {
	lenNeeded := int(uint32(c.NumRow()) >> 6)
	if cap(c.bitmapDeleteKeep) < lenNeeded {
		c.bitmapDeleteKeep = make(bitmap.Bitmap, lenNeeded)
	} else {
		clear(c.bitmapDeleteKeep)
	}
}

func (c *ArrayBase) batchDeleteKeep(start, end int) {
	for i := start; i < end; i++ {
		c.bitmapDeleteKeep.Set(uint32(i))
	}
}

func (c *ArrayBase) endBatchDelete() {
	prevOffset := uint64(0)
	keepIndex := 0
	c.offset = 0
	c.dataColumn.startBatchDelete()
	for i := 0; i < c.NumRow(); i++ {
		if c.bitmapDeleteKeep.Contains(uint32(i)) {
			if i != 0 {
				prevOffset = c.offsetColumn.Row(i - 1)
			}
			currentOffset := c.offsetColumn.Row(i)
			c.dataColumn.batchDeleteKeep(int(prevOffset), int(currentOffset))
			c.offset += currentOffset - prevOffset
			c.offsetColumn.values[keepIndex] = c.offset
			keepIndex++
		}
	}

	c.offsetColumn.values = c.offsetColumn.values[:keepIndex]
	c.offsetColumn.numRow = keepIndex
	c.dataColumn.endBatchDelete()
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
	switch v := dest.(type) {
	case *any:
		*v = c.RowAny(row)
		return nil
	case *[]any:
		*v = c.RowAny(row).([]any)
		return nil
	case sql.Scanner:
		return v.Scan(c.RowAny(row))
	}

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
	if len(c.offsetColumn.values) == 0 {
		return 0
	}
	return int(c.offsetColumn.values[len(c.offsetColumn.values)-1])
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *ArrayBase) SetWriteBufferSize(row int) {
	c.offsetColumn.SetWriteBufferSize(row)
	c.dataColumn.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *ArrayBase) ReadRaw(num int) error {
	c.offsetColumn.Reset()
	err := c.offsetColumn.ReadRaw(num)
	if err != nil {
		return fmt.Errorf("array: read offset column: %w", err)
	}
	if num > 0 {
		c.offset = c.offsetColumn.Row(num - 1)
	}
	err = c.dataColumn.ReadRaw(c.TotalRows())
	if err != nil {
		return fmt.Errorf("array: read data column: %w", err)
	}

	if c.resetHook != nil {
		c.resetHook()
	}
	return nil
}

// ReadHeader reads header data from reader
// it uses internally
func (c *ArrayBase) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	err := c.column.ReadHeader(r, serverInfo)
	if err != nil {
		return err
	}

	c.offsetColumn.r = r

	return c.dataColumn.ReadHeader(r, serverInfo)
}

// Column returns the sub column
func (c *ArrayBase) Column() ColumnCore {
	return c.dataColumn
}

func (c *ArrayBase) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	chType := helper.FilterSimpleAggregate(c.columnHeader.ChType)
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
		return &ErrInvalidType{
			chType:     string(chType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}

	if err := c.dataColumn.SetColumnHeader(ColumnHeader{
		ChType: chType[helper.LenArrayStr : len(chType)-1],
	}); err != nil {
		if !isInvalidType(err) {
			return err
		}
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	return nil
}

func (c *ArrayBase) ValidateInsert() error {
	var offset uint64
	if len(c.offsetColumn.values) > 0 {
		offset = c.offsetColumn.values[len(c.offsetColumn.values)-1]
	}
	if offset != uint64(c.dataColumn.NumRow()) {
		return fmt.Errorf("array length is not equal to data length: %d != %d %s",
			offset,
			c.dataColumn.NumRow(), c.FullType())
	}
	return nil
}

func (c *ArrayBase) chconnType() string {
	return c.arrayChconnType
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

func (c *ArrayBase) elem(arrayLevel int) ColumnCore {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *ArrayBase) FullType() string {
	if len(c.columnHeader.Name) == 0 {
		return "Array(" + c.dataColumn.FullType() + ")"
	}
	return string(c.columnHeader.Name) + " Array(" + c.dataColumn.FullType() + ")"
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

func (c *ArrayBase) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexArray))
	c.dataColumn.writeBinaryDataTo(w)
}
