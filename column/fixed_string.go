package column

import (
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

// FixedString is a column of FixedString(N) ClickHouse data type.
//
// Values are N bytes long. Shorter strings are padded with zero bytes, as ClickHouse does.
// A longer value makes ValidateInsert, and so the insert, fail until the next Reset.
//
// ColumnByType returns this type for FixedString(N). For a typed column of a known size,
// use New[[N]byte]().
type FixedString struct {
	column
	size                 int
	numRow               int
	vals                 []byte
	sparseData           []byte
	indexRemoveKeepIndex int
	// aliased is set by ReadFromBytes: vals is the caller's buffer and must not be written.
	aliased bool
}

// NewFixedString returns a new column for FixedString(size). It panics if size < 1.
func NewFixedString(size int) *FixedString {
	if size < 1 {
		panic(fmt.Sprintf("column: FixedString size must be >= 1, got %d", size))
	}
	return &FixedString{size: size}
}

// Size returns N of FixedString(N).
func (c *FixedString) Size() int {
	return c.size
}

// Data returns all the data in the current block.
func (c *FixedString) Data() []string {
	if c.numRow == 0 {
		return nil
	}
	val := make([]string, c.numRow)
	for i := range val {
		val[i] = string(c.rowBytes(i))
	}
	return val
}

// Read reads all the data in the current block and appends it to value.
func (c *FixedString) Read(value []string) []string {
	for i := range c.numRow {
		value = append(value, string(c.rowBytes(i)))
	}
	return value
}

// Row returns the value of the given row, N bytes long.
func (c *FixedString) Row(row int) string {
	return string(c.rowBytes(row))
}

// RowBytes returns a copy of the given row.
func (c *FixedString) RowBytes(row int) []byte {
	return slices.Clone(c.rowBytes(row))
}

func (c *FixedString) rowBytes(row int) []byte {
	return c.vals[row*c.size : (row+1)*c.size]
}

// RowAny returns the value of the given row as a string.
func (c *FixedString) RowAny(row int) any {
	return c.Row(row)
}

// Scan copies the given row into dest: a *string, **string, *[]byte, **[]byte, *any,
// sql.Scanner, or a pointer to a [N]byte array of the column's size.
func (c *FixedString) Scan(row int, dest any) error {
	switch d := dest.(type) {
	case *string:
		*d = c.Row(row)
		return nil
	case **string:
		v := c.Row(row)
		*d = &v
		return nil
	case *[]byte:
		b := c.rowBytes(row)
		if len(*d) < len(b) {
			*d = make([]byte, len(b))
		}
		*d = (*d)[:len(b)]
		copy(*d, b)
		return nil
	case **[]byte:
		b := c.RowBytes(row)
		*d = &b
		return nil
	case *any:
		*d = c.Row(row)
		return nil
	case sql.Scanner:
		return d.Scan(c.Row(row))
	}
	if err := c.ScanValue(row, reflect.ValueOf(dest)); err == nil {
		return nil
	}
	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "*string, *[]byte or *[" + strconv.Itoa(c.size) + "]byte",
	}
}

// ScanValue copies the given row into the value value points to.
func (c *FixedString) ScanValue(row int, value reflect.Value) error {
	if value.Kind() != reflect.Pointer {
		return fmt.Errorf("scan dest should be a pointer")
	}
	val := value.Elem()
	switch val.Kind() {
	case reflect.String:
		val.SetString(c.Row(row))
		return nil
	case reflect.Slice:
		if val.Type().Elem().Kind() == reflect.Uint8 {
			val.SetBytes(c.RowBytes(row))
			return nil
		}
	case reflect.Array:
		if val.Type().Elem().Kind() == reflect.Uint8 && val.Len() == c.size {
			reflect.Copy(val, reflect.ValueOf(c.rowBytes(row)))
			return nil
		}
	case reflect.Interface:
		if val.NumMethod() == 0 {
			val.Set(reflect.ValueOf(c.Row(row)))
			return nil
		}
	}
	return fmt.Errorf("cannot scan FixedString(%d) into %s", c.size, val.Type().String())
}

// Append appends a value for insert.
func (c *FixedString) Append(v string) {
	c.preHookAppend()
	c.appendString(v)
}

func (c *FixedString) appendString(v string) {
	c.unalias()
	if len(v) > c.size {
		c.setTooLong(len(v))
		v = v[:c.size]
	}
	c.vals = append(c.vals, v...)
	c.pad(len(v))
	c.numRow++
}

// AppendBytes appends a value for insert.
func (c *FixedString) AppendBytes(v []byte) {
	c.preHookAppend()
	c.appendBytes(v)
}

func (c *FixedString) appendBytes(v []byte) {
	c.unalias()
	if len(v) > c.size {
		c.setTooLong(len(v))
		v = v[:c.size]
	}
	c.vals = append(c.vals, v...)
	c.pad(len(v))
	c.numRow++
}

// AppendMulti appends values for insert.
func (c *FixedString) AppendMulti(v ...string) {
	c.preHookAppendMulti(len(v))
	for _, s := range v {
		c.appendString(s)
	}
}

// AppendBytesMulti appends values for insert.
func (c *FixedString) AppendBytesMulti(v ...[]byte) {
	c.preHookAppendMulti(len(v))
	for _, b := range v {
		c.appendBytes(b)
	}
}

// unalias copies vals before a change when it is the caller's ReadFromBytes buffer.
func (c *FixedString) unalias() {
	if c.aliased {
		c.vals = slices.Clone(c.vals)
		c.aliased = false
	}
}

func (c *FixedString) pad(n int) {
	for ; n < c.size; n++ {
		c.vals = append(c.vals, 0)
	}
}

// setTooLong keeps the first error, so ValidateInsert reports the first bad row.
func (c *FixedString) setTooLong(n int) {
	if c.appendErr == nil {
		c.appendErr = fmt.Errorf("value of %d bytes does not fit FixedString(%d) at row %d", n, c.size, c.numRow)
	}
}

// canAppend accepts only a [N]byte of the column's size, like Base[[N]byte] did, so a
// Variant still routes Go strings to its String member.
func (c *FixedString) canAppend(value any) bool {
	v := reflect.ValueOf(value)
	return c.isSizedArray(v)
}

func (c *FixedString) isSizedArray(v reflect.Value) bool {
	return v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8 && v.Len() == c.size
}

// AppendAny appends a string, []byte, a [N]byte array of the column's size, or a non-nil
// pointer to one of them.
func (c *FixedString) AppendAny(value any) error {
	if isNilPointer(value) {
		return fmt.Errorf("cannot append a nil %T to FixedString(%d)", value, c.size)
	}
	switch v := value.(type) {
	case string:
		c.Append(v)
	case []byte:
		c.AppendBytes(v)
	case *string:
		c.Append(*v)
	case *[]byte:
		c.AppendBytes(*v)
	default:
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Pointer {
			rv = rv.Elem()
		}
		if !c.isSizedArray(rv) {
			return fmt.Errorf("cannot convert %T to FixedString(%d)", value, c.size)
		}
		b := make([]byte, c.size)
		reflect.Copy(reflect.ValueOf(b), rv)
		c.AppendBytes(b)
	}
	return nil
}

// Remove keeps the first n rows; it is equal to data = data[:n].
func (c *FixedString) Remove(n int) {
	c.unalias()
	if n < 0 {
		n = 0
	}
	if n >= c.numRow {
		return
	}
	c.vals = c.vals[:n*c.size]
	c.numRow = n
}

// Delete removes the rows [start, end).
func (c *FixedString) Delete(start, end int) {
	c.unalias()
	if c.numRow == 0 || start >= c.numRow {
		return
	}
	if end > c.numRow {
		end = c.numRow
	}
	c.vals = slices.Delete(c.vals, start*c.size, end*c.size)
	c.numRow -= end - start
}

// DeleteFunc removes every row for which del returns true.
func (c *FixedString) DeleteFunc(del func(row int) bool) {
	c.unalias()
	keep := 0
	for i := range c.numRow {
		if del(i) {
			continue
		}
		if keep != i {
			copy(c.vals[keep*c.size:(keep+1)*c.size], c.rowBytes(i))
		}
		keep++
	}
	c.vals = c.vals[:keep*c.size]
	c.numRow = keep
}

func (c *FixedString) startBatchDelete() {
	c.unalias()
	c.indexRemoveKeepIndex = 0
}

func (c *FixedString) batchDeleteKeep(start, end int) {
	for i := start; i < end; i++ {
		if c.indexRemoveKeepIndex != i {
			copy(c.vals[c.indexRemoveKeepIndex*c.size:(c.indexRemoveKeepIndex+1)*c.size], c.rowBytes(i))
		}
		c.indexRemoveKeepIndex++
	}
}

func (c *FixedString) endBatchDelete() {
	c.vals = c.vals[:c.indexRemoveKeepIndex*c.size]
	c.numRow = c.indexRemoveKeepIndex
}

// NumRow returns the number of rows in this block.
func (c *FixedString) NumRow() int {
	return c.numRow
}

// Array returns an Array(FixedString(N)) column.
func (c *FixedString) Array() *Array[string] {
	return NewArray[string](c)
}

// Nullable returns a Nullable(FixedString(N)) column.
func (c *FixedString) Nullable() *FixedStringNullable {
	return NewFixedStringNullable(c)
}

// LC returns a LowCardinality(FixedString(N)) column.
func (c *FixedString) LC() *LowCardinality[string] {
	return NewLC[string](c)
}

// LowCardinality returns a LowCardinality(FixedString(N)) column.
func (c *FixedString) LowCardinality() *LowCardinality[string] {
	return NewLC[string](c)
}

func elemOfFixedString(c *FixedString, arrayLevel int, nullable, lc bool) ColumnCore {
	if lc {
		return elemOfLowCardinality(c.LowCardinality(), arrayLevel, nullable)
	}
	if nullable {
		return elemOfFixedStringNullable(c.Nullable(), arrayLevel)
	}
	if arrayLevel > 0 {
		return elemOfArray(c.Array(), arrayLevel-1)
	}
	return c
}

// Reset clears the data and any append error.
func (c *FixedString) Reset() {
	c.numRow = 0
	if c.aliased {
		c.vals = nil
		c.aliased = false
	} else {
		c.vals = c.vals[:0]
	}
	c.appendErr = nil
}

// SetWriteBufferSize sets the write buffer, in rows.
func (c *FixedString) SetWriteBufferSize(row int) {
	c.unalias()
	if cap(c.vals) < row*c.size {
		c.vals = make([]byte, 0, row*c.size)
	}
}

// ReadRaw reads raw data from the reader. It runs automatically.
func (c *FixedString) ReadRaw(num int) error {
	c.Reset()
	c.numRow = num

	if c.columnHeader.IsSparse {
		totalRowsRead, err := c.readSparse()
		if err != nil {
			return fmt.Errorf("read sparse: %w", err)
		}
		c.numRow = totalRowsRead
	}

	c.vals = helper.ResetSlice(c.vals, c.numRow*c.size, false)
	if c.numRow > 0 {
		if _, err := c.r.Read(c.vals); err != nil {
			return fmt.Errorf("read data: %w", err)
		}
	}

	if c.columnHeader.IsSparse {
		c.itemsTotalSparse -= 1
		c.sparseData = helper.ResetSlice(c.sparseData, int(c.itemsTotalSparse)*c.size, true)
		for i, itemNumber := range c.sparseIndexes {
			copy(c.sparseData[int(itemNumber-1)*c.size:], c.vals[i*c.size:(i+1)*c.size])
		}
		c.numRow = int(c.itemsTotalSparse)
		c.vals = c.sparseData[:c.numRow*c.size]
	}
	return nil
}

// SetColumnHeader sets the column header and checks it is FixedString(N) of this size.
func (c *FixedString) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	chType := helper.FilterSimpleAggregate(c.columnHeader.ChType)
	if !helper.IsFixedString(chType) {
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	size, err := strconv.Atoi(string(chType[helper.FixedStringStrLen : len(chType)-1]))
	if err != nil {
		return fmt.Errorf("invalid size: %w", err)
	}
	if size != c.size {
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	return nil
}

// ValidateInsert returns an error if a value longer than N was appended.
func (c *FixedString) ValidateInsert() error {
	return c.appendErr
}

func (c *FixedString) chconnType() string {
	return "column.FixedString"
}

func (c *FixedString) structType() string {
	return "FixedString(" + strconv.Itoa(c.size) + ")"
}

// WriteTo writes the data to ClickHouse. It is used internally.
func (c *FixedString) WriteTo(w io.Writer) (int64, error) {
	nw, err := w.Write(c.vals)
	return int64(nw), err
}

// HeaderWriter writes header data to the writer. It is used internally.
func (c *FixedString) HeaderWriter(w *readerwriter.Writer) {
}

func (c *FixedString) appendEmpty() {
	c.unalias()
	c.pad(0)
	c.numRow++
}

// FullType returns the ClickHouse type, with the column name if it is set.
func (c *FixedString) FullType() string {
	chType := string(c.columnHeader.ChType)
	if chType == "" {
		chType = c.structType()
	}
	if len(c.columnHeader.Name) == 0 {
		return chType
	}
	return string(c.columnHeader.Name) + " " + chType
}

// ToJSON appends the JSON value of the given row to b.
func (c *FixedString) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	return helper.AppendJSONSting(b, ignoreDoubleQuotes, c.rowBytes(row))
}

func (c *FixedString) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexFixedString))
	w.Uvarint(uint64(c.size))
}

// ReadFromBytes implements ZeroCopyColumn by aliasing data. The column becomes
// read-only; data must outlive it.
func (c *FixedString) ReadFromBytes(num int, data []byte) (int, error) {
	if num < 0 {
		return 0, fmt.Errorf("fixed string column %q: ReadFromBytes: num must be >= 0, got %d", c.columnHeader.Name, num)
	}
	if num > len(data)/c.size {
		return 0, fmt.Errorf("fixed string column %q: %d rows of %d bytes do not fit %d bytes", c.columnHeader.Name, num, c.size, len(data))
	}
	need := num * c.size
	c.Reset()
	c.vals = data[:need:need]
	c.aliased = true
	c.numRow = num
	return need, nil
}
