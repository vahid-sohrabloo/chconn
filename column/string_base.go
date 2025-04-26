package column

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"slices"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

type stringPos struct {
	start int
	end   int
}

// StringBase is a column of String ClickHouse data type with generic type
type StringBase[T ~string] struct {
	column
	numRow               int
	vals                 []byte
	pos                  []stringPos
	sparseDataPos        []stringPos
	indexRemoveKeepIndex int
}

// NewString is a column of String ClickHouse data type with generic type
func NewStringBase[T ~string]() *StringBase[T] {
	return &StringBase[T]{}
}

// Data get all the data in current block as a slice.
func (c *StringBase[T]) Data() []T {
	if len(c.pos) == 0 {
		return nil
	}
	val := make([]T, len(c.pos))
	for i, v := range c.pos {
		val[i] = T(c.vals[v.start:v.end])
	}
	return val
}

// Data get all the data in current block as a slice of []byte.
func (c *StringBase[T]) DataBytes() [][]byte {
	return c.ReadBytes(nil)
}

// Read reads all the data in current block and append to the input.
func (c *StringBase[T]) Read(value []T) []T {
	if len(c.pos) == 0 {
		return value
	}
	valueLen := len(value)
	if cap(value)-valueLen >= len(c.pos) {
		value = (value)[:valueLen+len(c.pos)]
	} else {
		value = append(value, make([]T, len(c.pos))...)
	}
	val := value[valueLen:]
	for i, v := range c.pos {
		val[i] = T(c.vals[v.start:v.end])
	}
	return value
}

// Read reads all the data as `[]byte` in current block and append to the input.
//
// data is valid only in the current block.
//
//nolint:dupl
func (c *StringBase[T]) ReadBytes(value [][]byte) [][]byte {
	if len(c.pos) == 0 {
		return value
	}
	valueLen := len(value)

	if cap(value)-valueLen >= len(c.pos) {
		value = (value)[:valueLen+len(c.pos)]
	} else {
		value = append(value, make([][]byte, len(c.pos))...)
	}

	val := (value)[valueLen:]
	for i, v := range c.pos {
		val[i] = make([]byte, v.end-v.start)
		copy(val[i], c.vals[v.start:v.end])
	}

	return value
}

// Row return the value of given row.
//
// NOTE: Row number start from zero
func (c *StringBase[T]) Row(row int) T {
	return T(c.rowBytes(row))
}

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *StringBase[T]) RowAny(row int) any {
	return c.Row(row)
}

func (c *StringBase[T]) Scan(row int, dest any) error {
	switch d := dest.(type) {
	case *T:
		*d = c.Row(row)
		return nil
	case **T:
		*d = new(T)
		**d = c.Row(row)
		return nil
	case *[]byte:
		b := c.rowBytes(row)
		if len(*d) < len(b) {
			*d = make([]byte, len(b))
		}
		copy(*d, b)
		return nil
	case **[]byte:
		rowBytes := c.rowBytes(row)
		b := make([]byte, len(rowBytes))
		copy(b, rowBytes)
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
		columnType: "*" + reflect.TypeOf(c.Row(row)).String(),
	}
}

func (c *StringBase[T]) ScanValue(row int, value reflect.Value) error {
	if value.Kind() != reflect.Ptr {
		return fmt.Errorf("scan dest should be a pointer")
	}

	val := value.Elem()

	rowVal := reflect.ValueOf(c.Row(row))
	if val.Type().AssignableTo(rowVal.Type()) {
		val.Set(rowVal)
		return nil
	}
	switch val.Kind() {
	case reflect.String:
		val.SetString(string(c.Row(row)))
		return nil
	case reflect.Slice:
		if val.Type().Elem().Kind() == reflect.Uint8 {
			val.SetBytes(c.rowBytes(row))
			return nil
		}
	case reflect.Interface:
		if value.NumMethod() == 0 {
			val.Set(reflect.ValueOf(c.Row(row)))
			return nil
		}
	}
	return fmt.Errorf("cannot scan text into %s", val.Type().String())
}

// Row return the value of given row.
//
// Data is valid only in the current block.
func (c *StringBase[T]) RowBytes(row int) []byte {
	r := c.rowBytes(row)
	if len(r) == 0 {
		return nil
	}
	val := make([]byte, len(r))
	copy(val, r)
	return val
}

func (c *StringBase[T]) rowBytes(row int) []byte {
	pos := c.pos[row]
	return c.vals[pos.start:pos.end]
}

func (c *StringBase[T]) Each(f func(i int, b []byte) bool) {
	for i, p := range c.pos {
		if !f(i, c.vals[p.start:p.end]) {
			return
		}
	}
}

func (c *StringBase[T]) appendLen(x int) {
	c.preHookAppend()
	c.vals = binary.AppendUvarint(c.vals, uint64(x))
	c.pos = append(c.pos, stringPos{start: len(c.vals), end: len(c.vals) + x})
}

// Append value for insert
func (c *StringBase[T]) Append(v T) {
	c.appendLen(len(v))
	c.vals = append(c.vals, v...)
	c.numRow++
}

func (c *StringBase[T]) canAppend(value any) bool {
	switch value.(type) {
	case string, T, []byte, *string, *[]byte:
		return true
	}
	return false
}

func (c *StringBase[T]) AppendAny(value any) error {
	switch v := value.(type) {
	case T:
		c.Append(v)
	//nolint:gocritic // to ignore caseOrder
	case string:
		c.Append(T(v))
	case []byte:
		c.AppendBytes(v)
	case *string:
		c.Append(T(*v))
	case *[]byte:
		c.AppendBytes(*v)
	default:
		return fmt.Errorf("cannot convert %T to string", value)
	}
	return nil
}

// AppendMulti value for insert
func (c *StringBase[T]) AppendMulti(v ...T) {
	for _, v := range v {
		c.appendLen(len(v))
		c.vals = append(c.vals, v...)
	}
	c.numRow += len(v)
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *StringBase[T]) Remove(n int) {
	if n < 0 {
		n = 0 // Cannot keep negative elements
	}
	if n >= c.numRow {
		return // Keep all or more elements than exist; do nothing
	}

	if n == 0 {
		// Keep zero elements
		c.vals = c.vals[:0]
		c.pos = c.pos[:0]
		c.numRow = 0
		return
	}

	// The first n elements in c.pos are the ones to keep.
	// Find the end byte position of the *data* for the last kept element (n-1).
	// This position marks the end of the valid data in c.vals that corresponds
	// to the elements being kept.
	endBytePos := c.pos[n-1].end

	// Truncate pos slice first.
	c.pos = c.pos[:n]

	// Truncate vals slice right after the data of the last kept element.
	// This ensures vals only contains complete prefixes and data for the
	// elements referenced by the truncated pos slice.
	c.vals = c.vals[:endBytePos]

	// Update the number of rows.
	c.numRow = n
}

func (c *StringBase[T]) Delete(start int, end int) {
	// Current validation checks...

	// Calculate byte range to remove
	startByteRemove := c.pos[start].start
	endByteRemove := c.pos[end-1].end

	// Adjust for the length prefix at start position
	startByteRemove -= helper.NumberByteForUvarint(uint64(c.pos[start].end - c.pos[start].start))

	// Calculate bytes being removed
	bytesRemoved := endByteRemove - startByteRemove

	// Delete from position slice and byte slice
	c.pos = slices.Delete(c.pos, start, end)
	c.vals = slices.Delete(c.vals, startByteRemove, endByteRemove)

	// Adjust remaining positions
	for i := start; i < len(c.pos); i++ {
		c.pos[i].start -= bytesRemoved
		c.pos[i].end -= bytesRemoved
	}

	c.numRow = len(c.pos)
}

func (c *StringBase[T]) DeleteFunc(del func(row int) bool) {
	if c.NumRow() == 0 {
		return
	}

	newPos := c.pos[:0]
	newVals := c.vals[:0]

	for i := 0; i < c.numRow; i++ {
		if del(i) {
			continue // Skip this row
		}

		// Get current position
		oldPos := c.pos[i]

		// Calculate string length
		strLen := oldPos.end - oldPos.start

		// Add the length prefix to new values
		newVals = binary.AppendUvarint(newVals, uint64(strLen))

		// Set new position start (after the length prefix)
		var newPos0 stringPos
		newPos0.start = len(newVals)

		// Copy string data
		newVals = append(newVals, c.vals[oldPos.start:oldPos.end]...)
		newPos0.end = len(newVals)

		// Add to position list
		newPos = append(newPos, newPos0)
	}

	// Update the column with the new data
	c.vals = newVals
	c.pos = newPos
	c.numRow = len(newPos)
}

func (c *StringBase[T]) startBatchDelete() {
	c.indexRemoveKeepIndex = 0
}

func (c *StringBase[T]) batchDeleteKeep(start, end int) {
	for i := start; i < end; i++ {
		c.pos[c.indexRemoveKeepIndex] = c.pos[i]
		c.indexRemoveKeepIndex++
	}
}

func (c *StringBase[T]) endBatchDelete() {
	keep := c.indexRemoveKeepIndex

	// nothing kept → clear everything
	if keep == 0 {
		c.vals = c.vals[:0]
		c.pos = c.pos[:0]
		c.numRow = 0
		return
	}
	// nothing deleted → no work
	if keep == len(c.pos) {
		return
	}

	// compact in-place
	writeOff := 0
	for i := 0; i < keep; i++ {
		old := c.pos[i]
		strLen := old.end - old.start
		prefixLen := helper.NumberByteForUvarint(uint64(strLen))
		segStart := old.start - prefixLen
		segLen := prefixLen + strLen

		// slide the [prefix + data] block down to writeOff
		copy(c.vals[writeOff:writeOff+segLen], c.vals[segStart:segStart+segLen])

		// update position to new locations
		c.pos[i].start = writeOff + prefixLen
		c.pos[i].end = c.pos[i].start + strLen

		writeOff += segLen
	}

	c.vals = c.vals[:writeOff]
	c.pos = c.pos[:keep]
	c.numRow = keep
}

// AppendBytes value of bytes for insert
func (c *StringBase[T]) AppendBytes(v []byte) {
	c.appendLen(len(v))
	c.vals = append(c.vals, v...)
	c.numRow++
}

// AppendBytesMulti value of bytes for insert
func (c *StringBase[T]) AppendBytesMulti(v ...[]byte) {
	for _, v := range v {
		c.appendLen(len(v))
		c.vals = append(c.vals, v...)
	}
	c.numRow += len(v)
}

// NumRow return number of row for this block
func (c *StringBase[T]) NumRow() int {
	return c.numRow
}

// Array return a Array type for this column
func (c *StringBase[T]) Array() *Array[T] {
	return NewArray[T](c)
}

// Nullable return a nullable type for this column
func (c *StringBase[T]) Nullable() *StringNullable[T] {
	return NewStringNullable(c)
}

// LC return a low cardinality type for this column
func (c *StringBase[T]) LC() *LowCardinality[T] {
	return NewLC[T](c)
}

// LowCardinality return a low cardinality type for this column
func (c *StringBase[T]) LowCardinality() *LowCardinality[T] {
	return NewLC[T](c)
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *StringBase[T]) Reset() {
	c.numRow = 0
	c.vals = c.vals[:0]
	c.pos = c.pos[:0]
	c.vals = c.vals[:0]
}

// SetWriteBufferSize set write buffer (number of bytes)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *StringBase[T]) SetWriteBufferSize(b int) {
	if cap(c.vals) < b {
		c.vals = make([]byte, 0, b)
	}
}

// ReadRaw read raw data from the reader.
//
// NOTE: its for internal use only
func (c *StringBase[T]) ReadRaw(num int) error {
	c.Reset()
	c.numRow = num

	if c.columnHeader.IsSparse {
		totalRowsRead, err := c.readSparse()
		if err != nil {
			return fmt.Errorf("read sparse: %w", err)
		}
		c.numRow = totalRowsRead
	}

	var p stringPos
	for i := 0; i < c.numRow; i++ {
		l, err := c.r.Uvarint()
		if err != nil {
			return fmt.Errorf("error read string len: %w", err)
		}
		// append length of string to make sure has the same data for write
		c.vals = binary.AppendUvarint(c.vals, uint64(l))
		p.start = len(c.vals)
		p.end = len(c.vals) + int(l)
		if l > 0 {
			c.vals = append(c.vals, make([]byte, l)...)
			if _, err := c.r.Read(c.vals[p.start:p.end]); err != nil {
				return fmt.Errorf("error read string: %w", err)
			}
		}
		c.pos = append(c.pos, p)
	}

	if c.columnHeader.IsSparse {
		c.itemsTotalSparse -= 1
		items := c.pos
		c.sparseDataPos = helper.ResetSlice(c.sparseDataPos, int(c.itemsTotalSparse), true)
		for i, itemNumber := range c.sparseIndexes {
			c.sparseDataPos[itemNumber-1] = items[i]
		}

		c.numRow = int(c.itemsTotalSparse)

		bSize := len(c.sparseDataPos)
		c.pos = helper.ResetSlice(c.pos, bSize, false)
		copy(c.pos, c.sparseDataPos)
	}
	return nil
}

func (c *StringBase[T]) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	chType := helper.FilterSimpleAggregate(c.columnHeader.ChType)
	if !helper.IsString(chType) {
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	return nil
}

func (c *StringBase[T]) ValidateInsert() error {
	return nil
}

func (c *StringBase[T]) chconnType() string {
	return "column.StringBase[" + reflect.TypeOf((*T)(nil)).Elem().String() + "]"
}

func (c *StringBase[T]) structType() string {
	return helper.StringStr
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *StringBase[T]) WriteTo(w io.Writer) (int64, error) {
	nw, err := w.Write(c.vals)
	return int64(nw), err
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *StringBase[T]) HeaderWriter(w *readerwriter.Writer) {
}

func (c *StringBase[T]) appendEmpty() {
	c.vals = append(c.vals, 0)
	c.pos = append(c.pos, stringPos{start: len(c.vals), end: len(c.vals)})
	c.numRow++
}

func (c *StringBase[T]) Elem(arrayLevel int, nullable, lc bool) ColumnCore {
	if lc {
		return c.LowCardinality().elem(arrayLevel, nullable)
	}
	if nullable {
		return c.Nullable().elem(arrayLevel)
	}
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *StringBase[T]) FullType() string {
	if len(c.columnHeader.Name) == 0 {
		return "String"
	}
	return string(c.columnHeader.Name) + " String"
}

// ToJSON
func (c *StringBase[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	return helper.AppendJSONSting(b, ignoreDoubleQuotes, c.rowBytes(row))
}

func (c *StringBase[T]) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexString))
}
