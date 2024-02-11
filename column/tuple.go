package column

import (
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
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
	isJSON  bool
	isNamed bool
	columns []ColumnBasic
}

// NewTuple create a new tuple of Tuple(T1,T2,.....,Tn) ClickHouse data type
//
// this is actually a group of columns. it doesn't have any method for read or write data
func NewTuple(columns ...ColumnBasic) *Tuple {
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
func (c *Tuple) ReadRaw(num int, r *readerwriter.Reader) error {
	for i, col := range c.columns {
		err := col.ReadRaw(num, r)
		if err != nil {
			return fmt.Errorf("tuple: read column index %d: %w", i, err)
		}
	}
	return nil
}

// HeaderReader reads header data from reader.
// it uses internally
func (c *Tuple) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	c.r = r
	err := c.readColumn(readColumn, revision)
	if err != nil {
		return err
	}

	for i, col := range c.columns {
		err = col.HeaderReader(r, false, revision)
		if err != nil {
			return fmt.Errorf("tuple: read column header index %d: %w", i, err)
		}
	}

	return nil
}

func (c *Tuple) Row(row int) any {
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

		err := col.ScanValue(row, sField.Addr())
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
func (c *Tuple) Columns() []ColumnBasic {
	return c.columns
}

func (c *Tuple) Validate(forInsert bool) error {
	if string(c.chType) == "Object('json')" {
		c.isJSON = true
		return nil
	}
	chType := helper.FilterSimpleAggregate(c.chType)
	if helper.IsPoint(chType) {
		chType = helper.PointMainTypeStr
	}

	if !helper.IsTuple(chType) {
		return ErrInvalidType{
			chType:     string(c.chType),
			structType: c.structType(),
		}
	}

	columnsTuple, err := helper.TypesInParentheses(chType[helper.LenTupleStr : len(chType)-1])
	if err != nil {
		return fmt.Errorf("tuple invalid types %w", err)
	}
	if len(columnsTuple) != len(c.columns) {
		//nolint:goerr113
		return fmt.Errorf("columns number for %s (%s) is not equal to tuple columns number: %d != %d",
			string(c.name),
			string(c.Type()),
			len(columnsTuple),
			len(c.columns),
		)
	}

	for i, col := range c.columns {
		if len(columnsTuple[i].Name) != 0 {
			c.isNamed = true
		}
		col.SetType(columnsTuple[i].ChType)
		col.SetName(columnsTuple[i].Name)
		if col.Validate(forInsert) != nil {
			return ErrInvalidType{
				chType:     string(c.chType),
				structType: c.structType(),
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

// WriteTo write data to ClickHouse.
// it uses internally
func (c *Tuple) WriteTo(w io.Writer) (int64, error) {
	var n int64
	if c.isJSON {
		// todo find a more efficient way
		wf := readerwriter.NewWriter()
		wf.String(c.FullType())
		nw, err := wf.WriteTo(w)
		if err != nil {
			return n, fmt.Errorf("tuple: write type: %w", err)
		}
		n += nw
	}

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
	if c.isJSON {
		w.Uint8(0)
	}
	for _, col := range c.columns {
		col.HeaderWriter(w)
	}
}

func (c *Tuple) Elem(arrayLevel int) ColumnBasic {
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

func (c *Tuple) FullType() string {
	var chType string
	if len(c.name) == 0 {
		chType = "Tuple("
	} else {
		chType = string(c.name) + " Tuple("
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
