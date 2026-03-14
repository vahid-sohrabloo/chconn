package column

import (
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

type TupleStruct[T any] interface {
	Column[T]
	Array() *Array[T]
}

// Tuple is a column of Tuple(T1,T2,.....,Tn) ClickHouse data type
//
// this is actually a group of columns. it doesn't have any method for read or write data
type Tuple struct {
	column
	isNamed bool
	isEmpty bool
	columns []ColumnCore
}

// NewTuple create a new tuple of Tuple(T1,T2,.....,Tn) ClickHouse data type
//
// this is actually a group of columns. it doesn't have any method for read or write data
func NewTuple(columns ...ColumnCore) *Tuple {
	if len(columns) < 1 {
		panic("tuple must have at least one column")
	}
	return &Tuple{
		columns: columns,
	}
}

// NumRow return number of row for this block
func (c *Tuple) NumRow() int {
	return c.columns[0].NumRow()
}

// Array return a Array type for this column
func (c *Tuple) Array() *ArrayBase {
	return NewArrayBase(c)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *Tuple) Reset() {
	for _, col := range c.columns {
		col.Reset()
	}
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *Tuple) SetWriteBufferSize(row int) {
	for _, col := range c.columns {
		col.SetWriteBufferSize(row)
	}
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *Tuple) ReadRaw(num int) error {
	for i, col := range c.columns {
		err := col.ReadRaw(num)
		if err != nil {
			return fmt.Errorf("tuple: read column index %d: %w", i, err)
		}
	}
	return nil
}

// ReadHeader reads header data from reader.
// it uses internally
func (c *Tuple) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	err := c.column.ReadHeader(r, serverInfo)
	if err != nil {
		return err
	}

	for i, col := range c.columns {
		err := col.ReadHeader(r, serverInfo)
		if err != nil {
			return fmt.Errorf("tuple: read column header index %d: %w", i, err)
		}
	}

	return nil
}

func (c *Tuple) Row(row int) any {
	if c.isEmpty {
		return nil
	}
	if c.isNamed {
		ret := make(map[string]any, len(c.columns))
		for _, col := range c.columns {
			ret[string(col.Name())] = col.RowAny(row)
		}
	}
	ret := make([]any, len(c.columns))
	for i, col := range c.columns {
		ret[i] = col.RowAny(row)
	}
	return ret
}

func (c *Tuple) RowAny(row int) any {
	return c.Row(row)
}

func (c *Tuple) Scan(row int, dest any) error {
	return c.ScanValue(row, reflect.ValueOf(dest))
}

func (c *Tuple) ScanValue(row int, dest reflect.Value) error {
	if dest.Kind() != reflect.Ptr {
		return fmt.Errorf("scan dest should be a pointer")
	}
	if c.isEmpty {
		return nil
	}
	switch dest.Elem().Kind() {
	case reflect.Struct:
		return c.scanStruct(row, dest)
	case reflect.Map:
		return c.scanMap(row, dest)
	case reflect.Slice:
		return c.scanSlice(row, dest)
	case reflect.Interface:
		if dest.NumMethod() == 0 {
			dest.Elem().Set(reflect.ValueOf(c.RowAny(row)))
			return nil
		}
		return fmt.Errorf("tuple: scan: unsupported type %s", dest.Elem().Kind())
	default:
		return fmt.Errorf("tuple: scan: unsupported type %s", dest.Elem().Kind())
	}
}

func (c *Tuple) scanMap(row int, val reflect.Value) error {
	if val.Elem().Type().Key().Kind() != reflect.String {
		return fmt.Errorf("tuple: scan: map key should be string")
	}
	if val.Elem().IsNil() {
		val.Elem().Set(reflect.MakeMapWithSize(val.Elem().Type(), len(c.columns)))
	}
	for i, col := range c.columns {
		colName := string(col.Name())
		if colName == "" {
			colName = strconv.Itoa(i)
		}
		index := reflect.ValueOf(colName)

		// Check if the key exists in the map
		mapIndexValue := val.Elem().MapIndex(index)

		// If the key does not exist, create a new value of the appropriate type
		if !mapIndexValue.IsValid() {
			mapIndexValue = reflect.New(val.Elem().Type().Elem()).Elem()
		}
		if colTuple, ok := col.(*Tuple); ok {
			err := colTuple.ScanValue(row, mapIndexValue.Addr())
			if err != nil {
				return fmt.Errorf("tuple: scan %s: %w", colName, err)
			}
		} else {
			err := col.Scan(row, mapIndexValue.Addr().Interface())
			if err != nil {
				return fmt.Errorf("tuple: scan %s: %w", colName, err)
			}
		}

		// Set the new or existing value in the map
		val.Elem().SetMapIndex(index, mapIndexValue)
	}
	return nil
}

func (c *Tuple) scanStruct(row int, val reflect.Value) error {
	for i, col := range c.columns {
		colName := string(col.Name())
		if colName == "" {
			colName = strconv.Itoa(i)
		}
		sField, ok := getStructFieldValue(val.Elem(), colName)
		if !ok {
			continue
		}

		err := col.Scan(row, sField.Addr().Interface())
		if err != nil {
			return fmt.Errorf("tuple: scan %s: %w", colName, err)
		}
	}
	return nil
}

func (c *Tuple) scanSlice(row int, val reflect.Value) error {
	for _, col := range c.columns {
		val.Elem().Set(reflect.Append(val.Elem(), reflect.ValueOf(col.RowAny(row))))
	}
	return nil
}

func getStructFieldValue(field reflect.Value, name string) (reflect.Value, bool) {
	tField := field.Type()
	for i := 0; i < tField.NumField(); i++ {
		if tag := tField.Field(i).Tag.Get("db"); tag == name {
			return field.Field(i), true
		}
		if tag := tField.Field(i).Tag.Get("json"); tag == name {
			return field.Field(i), true
		}
	}
	sField := field.FieldByName(name)
	return sField, sField.IsValid()
}

// Column returns the all sub columns
func (c *Tuple) Columns() []ColumnCore {
	return c.columns
}

var emptyTypeColumns = []helper.ColumnData{
	{ChType: []byte("Nothing")},
}

func (c *Tuple) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	chType := helper.FilterSimpleAggregate(c.columnHeader.ChType)
	if helper.IsPoint(chType) {
		chType = helper.PointMainTypeStr
	}

	if !helper.IsTuple(chType) {
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}

	columnsTuple, err := helper.TypesInParentheses(chType[helper.LenTupleStr : len(chType)-1])
	if err != nil {
		return fmt.Errorf("tuple invalid types %w", err)
	}
	if len(columnsTuple) == 0 {
		columnsTuple = emptyTypeColumns
		c.isEmpty = true
	}
	if len(columnsTuple) != len(c.columns) {
		//nolint:goerr113
		return fmt.Errorf("columns number for %s (%s) is not equal to tuple columns number: %d != %d",
			string(c.columnHeader.Name),
			string(c.Type()),
			len(columnsTuple),
			len(c.columns),
		)
	}

	for i, col := range c.columns {
		if len(columnsTuple[i].Name) != 0 {
			c.isNamed = true
		}
		if err := col.SetColumnHeader(ColumnHeader{
			ChType: columnsTuple[i].ChType,
			Name:   columnsTuple[i].Name,
		}); err != nil {
			if !isInvalidType(err) {
				return fmt.Errorf("tuple: set column header index %d: %w", i, err)
			}
			return &ErrInvalidType{
				chType:     string(c.columnHeader.ChType),
				chconnType: c.chconnType(),
				goToChType: c.structType(),
			}
		}
	}
	return nil
}

func (c *Tuple) ValidateInsert() error {
	for _, col := range c.columns {
		if err := col.ValidateInsert(); err != nil {
			if !isInvalidType(err) {
				return err
			}
			return &ErrInvalidType{
				chType:     string(c.columnHeader.ChType),
				chconnType: c.chconnType(),
				goToChType: c.structType(),
			}
		}
	}
	return nil
}

func (c *Tuple) structType() string {
	str := helper.TupleStr
	for _, col := range c.columns {
		str += col.structType() + ","
	}
	return str[:len(str)-1] + ")"
}

func (c *Tuple) chconnType() string {
	chConn := "column.Tuple("
	for _, col := range c.columns {
		chConn += col.chconnType() + ", "
	}
	return chConn[:len(chConn)-2] + ")"
}

func (c *Tuple) canAppend(value any) bool {
	sliceVal := reflect.ValueOf(value)
	if sliceVal.Kind() != reflect.Slice {
		return false
	}

	if sliceVal.Len() != len(c.columns) {
		return false
	}

	for i := 0; i < sliceVal.Len(); i++ {
		item := sliceVal.Index(i).Interface()
		if !c.columns[i].canAppend(item) {
			return false
		}
	}

	return true
}

func (c *Tuple) AppendAny(value any) error {
	sliceVal := reflect.ValueOf(value)
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("value is not a slice")
	}

	if sliceVal.Len() != len(c.columns) {
		return fmt.Errorf("value length is not equal to columns length")
	}

	for i := 0; i < sliceVal.Len(); i++ {
		item := sliceVal.Index(i).Interface()
		err := c.columns[i].AppendAny(item)
		if err != nil {
			return fmt.Errorf("cannot append %dth item to tuple %v: %w", i, item, err)
		}
	}

	return nil
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *Tuple) WriteTo(w io.Writer) (int64, error) {
	var n int64
	for i, col := range c.columns {
		nw, err := col.WriteTo(w)
		if err != nil {
			return n, fmt.Errorf("tuple: write column index %d: %w", i, err)
		}
		n += nw
	}
	return n, nil
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *Tuple) HeaderWriter(w *readerwriter.Writer) {
	for _, col := range c.columns {
		col.HeaderWriter(w)
	}
}

func (c *Tuple) Elem(arrayLevel int) ColumnCore {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *Tuple) Remove(n int) {
	for _, col := range c.columns {
		col.Remove(n)
	}
}

func (c *Tuple) Delete(start, end int) {
	for _, col := range c.columns {
		col.Delete(start, end)
	}
}

func (c *Tuple) DeleteFunc(del func(row int) bool) {
	for _, col := range c.columns {
		col.DeleteFunc(del)
	}
}

func (c *Tuple) startBatchDelete() {
	for _, col := range c.columns {
		col.startBatchDelete()
	}
}

func (c *Tuple) batchDeleteKeep(start, end int) {
	for _, col := range c.columns {
		col.batchDeleteKeep(start, end)
	}
}

func (c *Tuple) endBatchDelete() {
	for _, col := range c.columns {
		col.endBatchDelete()
	}
}

func (c *Tuple) FullType() string {
	var chType string
	if len(c.columnHeader.Name) == 0 {
		chType = "Tuple("
	} else {
		chType = string(c.columnHeader.Name) + " Tuple("
	}
	for _, col := range c.columns {
		chType += col.FullType() + ", "
	}
	return chType[:len(chType)-2] + ")"
}

func (c *Tuple) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	if c.isNamed {
		b = append(b, '{')
	} else {
		b = append(b, '[')
	}

	for i, col := range c.columns {
		if c.isNamed {
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			b = append(b, col.Name()...)
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			b = append(b, ':')
		}
		b = col.ToJSON(row, ignoreDoubleQuotes, b)
		if i < len(c.columns)-1 {
			b = append(b, ',')
		}
	}

	if c.isNamed {
		b = append(b, '}')
	} else {
		b = append(b, ']')
	}
	return b
}

func (c *Tuple) writeBinaryDataTo(w *readerwriter.Writer) {
	if c.isNamed {
		w.Uint8(uint8(helper.BinaryTypeIndexNamedTuple))
	} else {
		w.Uint8(uint8(helper.BinaryTypeIndexUnnamedTuple))
	}
	w.Uvarint(uint64(len(c.columns)))
	for _, col := range c.columns {
		if c.isNamed {
			w.ByteString(col.Name())
		}
		col.writeBinaryDataTo(w)
	}
}
