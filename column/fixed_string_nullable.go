package column

import (
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

// FixedStringNullable is a column of Nullable(FixedString(N)) ClickHouse data type.
type FixedStringNullable struct {
	column
	dataColumn           *FixedString
	values               []byte
	indexRemoveKeepIndex int
}

// NewFixedStringNullable returns a new column for Nullable(FixedString(N)).
func NewFixedStringNullable(dataColumn *FixedString) *FixedStringNullable {
	return &FixedStringNullable{
		dataColumn: dataColumn,
	}
}

// Data returns all the data in the current block; null rows are zero bytes.
func (c *FixedStringNullable) Data() []string {
	return c.dataColumn.Data()
}

// DataP returns all the data in the current block, nil for null rows.
func (c *FixedStringNullable) DataP() []*string {
	val := make([]*string, c.NumRow())
	for i := range val {
		val[i] = c.RowP(i)
	}
	return val
}

// Read reads all the data in the current block and appends it to value.
func (c *FixedStringNullable) Read(value []string) []string {
	return c.dataColumn.Read(value)
}

// ReadP reads all the data in the current block and appends it to value, nil for null rows.
func (c *FixedStringNullable) ReadP(value []*string) []*string {
	for i := 0; i < c.NumRow(); i++ {
		value = append(value, c.RowP(i))
	}
	return value
}

// Row returns the value of the given row; a null row is zero bytes.
func (c *FixedStringNullable) Row(i int) string {
	return c.dataColumn.Row(i)
}

// RowBytes returns a copy of the given row.
func (c *FixedStringNullable) RowBytes(i int) []byte {
	return c.dataColumn.RowBytes(i)
}

// RowAny returns the value of the given row as a *string, nil for a null row.
func (c *FixedStringNullable) RowAny(i int) any {
	return c.RowP(i)
}

// Scan copies the given row into dest. A pointer to a pointer, such as **string or
// **[N]byte, receives nil for a null row.
func (c *FixedStringNullable) Scan(row int, dest any) error {
	switch d := dest.(type) {
	case **string:
		*d = c.RowP(row)
		return nil
	case **[]byte:
		if c.RowIsNil(row) {
			*d = nil
			return nil
		}
		b := c.RowBytes(row)
		*d = &b
		return nil
	case *any:
		*d = c.Row(row)
		return nil
	case sql.Scanner:
		if c.RowIsNil(row) {
			return d.Scan(nil)
		}
		return d.Scan(c.Row(row))
	}
	if ok, err := scanIntoPointer(dest, c.RowIsNil(row), func(d any) error { return c.dataColumn.Scan(row, d) }); ok {
		return err
	}
	if err := c.dataColumn.Scan(row, dest); err == nil {
		return nil
	}
	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "**string, **[]byte or **[" + strconv.Itoa(c.dataColumn.size) + "]byte",
	}
}

// RowP returns the value of the given row, nil for a null row.
func (c *FixedStringNullable) RowP(row int) *string {
	if c.values[row] == 1 {
		return nil
	}
	val := c.dataColumn.Row(row)
	return &val
}

// ReadNil reads the null flags of the current block and appends them to value.
func (c *FixedStringNullable) ReadNil(value []bool) []bool {
	return append(value, *(*[]bool)(unsafe.Pointer(&c.values))...)
}

// DataNil returns the null flags of the current block.
func (c *FixedStringNullable) DataNil() []bool {
	return *(*[]bool)(unsafe.Pointer(&c.values))
}

// RowIsNil returns true if the row is null.
func (c *FixedStringNullable) RowIsNil(row int) bool {
	return c.values[row] == 1
}

// Append appends a value for insert.
func (c *FixedStringNullable) Append(v string) {
	c.preHookAppend()
	c.values = append(c.values, 0)
	c.dataColumn.appendString(v)
}

// canAppend accepts nil, a [N]byte of the column's size, or a pointer to one.
func (c *FixedStringNullable) canAppend(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Pointer && v.Type().Elem().Kind() == reflect.Array {
		return v.IsNil() || c.dataColumn.isSizedArray(v.Elem())
	}
	return c.dataColumn.isSizedArray(v)
}

// AppendAny appends a value accepted by FixedString.AppendAny; nil or a nil pointer is a null row.
func (c *FixedStringNullable) AppendAny(value any) error {
	if isNilPointer(value) {
		c.AppendNil()
		return nil
	}
	switch v := value.(type) {
	case *string:
		c.AppendP(v)
		return nil
	case *[]byte:
		c.AppendBytes(*v)
		return nil
	}
	if err := c.dataColumn.AppendAny(value); err != nil {
		return fmt.Errorf("cannot append to Nullable(FixedString(%d)): %w", c.dataColumn.size, err)
	}
	c.preHookAppend()
	c.values = append(c.values, 0)
	return nil
}

// AppendMulti appends values for insert.
func (c *FixedStringNullable) AppendMulti(v ...string) {
	c.preHookAppendMulti(len(v))
	c.values = append(c.values, make([]uint8, len(v))...)
	for _, s := range v {
		c.dataColumn.appendString(s)
	}
}

// AppendBytes appends a value for insert.
func (c *FixedStringNullable) AppendBytes(v []byte) {
	c.preHookAppend()
	c.values = append(c.values, 0)
	c.dataColumn.appendBytes(v)
}

// AppendBytesMulti appends values for insert.
func (c *FixedStringNullable) AppendBytesMulti(v ...[]byte) {
	c.preHookAppendMulti(len(v))
	c.values = append(c.values, make([]uint8, len(v))...)
	for _, b := range v {
		c.dataColumn.appendBytes(b)
	}
}

// Remove keeps the first n rows; it is equal to data = data[:n].
func (c *FixedStringNullable) Remove(n int) {
	if c.NumRow() == 0 || c.NumRow() <= n {
		return
	}
	c.values = c.values[:n]
	c.dataColumn.Remove(n)
}

// Delete removes the rows [start, end).
func (c *FixedStringNullable) Delete(start, end int) {
	if c.NumRow() == 0 || c.NumRow() <= start {
		return
	}
	if end > c.NumRow() {
		end = c.NumRow()
	}
	c.values = append(c.values[:start], c.values[end:]...)
	c.dataColumn.Delete(start, end)
}

// DeleteFunc removes every row for which del returns true.
func (c *FixedStringNullable) DeleteFunc(del func(row int) bool) {
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
	c.values = c.values[:i]
	c.dataColumn.DeleteFunc(del)
}

func (c *FixedStringNullable) startBatchDelete() {
	c.indexRemoveKeepIndex = 0
	c.dataColumn.startBatchDelete()
}

func (c *FixedStringNullable) batchDeleteKeep(start, end int) {
	for i := start; i < end; i++ {
		c.values[c.indexRemoveKeepIndex] = c.values[i]
		c.indexRemoveKeepIndex++
	}
	c.dataColumn.batchDeleteKeep(start, end)
}

func (c *FixedStringNullable) endBatchDelete() {
	c.values = c.values[:c.indexRemoveKeepIndex]
	c.dataColumn.endBatchDelete()
}

// AppendP appends a value for insert, or a null row if v is nil.
func (c *FixedStringNullable) AppendP(v *string) {
	if v == nil {
		c.AppendNil()
		return
	}
	c.Append(*v)
}

// AppendMultiP appends values for insert; a nil value is a null row.
func (c *FixedStringNullable) AppendMultiP(v ...*string) {
	for _, s := range v {
		c.AppendP(s)
	}
}

// AppendNil appends a null row.
func (c *FixedStringNullable) AppendNil() {
	c.preHookAppend()
	c.values = append(c.values, 1)
	c.dataColumn.appendEmpty()
}

// NumRow returns the number of rows in this block.
func (c *FixedStringNullable) NumRow() int {
	return c.dataColumn.NumRow()
}

// Array returns an Array(Nullable(FixedString(N))) column.
func (c *FixedStringNullable) Array() *ArrayNullable[string] {
	return NewArrayNullable[string](c)
}

func elemOfFixedStringNullable(c *FixedStringNullable, arrayLevel int) ColumnCore {
	if arrayLevel > 0 {
		return elemOfArrayNullable(c.Array(), arrayLevel-1)
	}
	return c
}

// Reset clears the data.
func (c *FixedStringNullable) Reset() {
	c.values = c.values[:0]
	c.dataColumn.Reset()
}

// SetWriteBufferSize sets the write buffer, in rows.
func (c *FixedStringNullable) SetWriteBufferSize(row int) {
	if cap(c.values) < row {
		c.values = make([]byte, 0, row)
	}
	c.dataColumn.SetWriteBufferSize(row)
}

// ReadRaw reads raw data from the reader. It runs automatically.
func (c *FixedStringNullable) ReadRaw(num int) error {
	c.Reset()
	c.values = helper.ResetSlice(c.values, num, false)
	if _, err := c.r.Read(c.values); err != nil {
		return fmt.Errorf("read nullable data: %w", err)
	}
	return c.dataColumn.ReadRaw(num)
}

// ReadHeader reads header data from the reader. It is used internally.
func (c *FixedStringNullable) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	if err := c.column.ReadHeader(r, serverInfo); err != nil {
		return err
	}
	return c.dataColumn.ReadHeader(r, serverInfo)
}

// SetColumnHeader sets the column header and checks it is Nullable(FixedString(N)) of this size.
func (c *FixedStringNullable) SetColumnHeader(ch ColumnHeader) error {
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
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	return nil
}

// ValidateInsert returns an error if a value longer than N was appended.
func (c *FixedStringNullable) ValidateInsert() error {
	return c.dataColumn.ValidateInsert()
}

func (c *FixedStringNullable) chconnType() string {
	return "column.FixedStringNullable"
}

func (c *FixedStringNullable) structType() string {
	return strings.ReplaceAll(helper.NullableTypeStr, "<type>", c.dataColumn.structType())
}

// WriteTo writes the data to ClickHouse. It is used internally.
func (c *FixedStringNullable) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(c.values)
	if err != nil {
		return int64(n), fmt.Errorf("write nullable data: %w", err)
	}
	nw, err := c.dataColumn.WriteTo(w)
	return nw + int64(n), err
}

// HeaderWriter writes header data to the writer. It is used internally.
func (c *FixedStringNullable) HeaderWriter(w *readerwriter.Writer) {
}

// FullType returns the ClickHouse type, with the column name if it is set.
func (c *FixedStringNullable) FullType() string {
	if len(c.columnHeader.Name) == 0 {
		return "Nullable(" + c.dataColumn.FullType() + ")"
	}
	return string(c.columnHeader.Name) + " Nullable(" + c.dataColumn.FullType() + ")"
}

// ToJSON appends the JSON value of the given row to b.
func (c *FixedStringNullable) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	if c.RowIsNil(row) {
		return append(b, "null"...)
	}
	return c.dataColumn.ToJSON(row, ignoreDoubleQuotes, b)
}

func (c *FixedStringNullable) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexNullable))
	c.dataColumn.writeBinaryDataTo(w)
}
