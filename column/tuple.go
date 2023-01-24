package column

import (
	"fmt"
	"reflect"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

type TupleStruct[T any] interface {
	Column[T]
	Array() *Array[T]
	// TODO must complete
	// Nullable() *Nullable[T]
}

// Tuple is a column of Tuple(T1,T2,.....,Tn) ClickHouse data type
//
// this is actually a group of columns. it doesn't have any method for read or write data
//
// You MUST use this on Select and Insert methods and for append and read data use the sub columns
type Tuple struct {
	column
	isJSON  bool
	isNamed bool
	columns []ColumnBasic
}

// NewTuple create a new tuple of Tuple(T1,T2,.....,Tn) ClickHouse data type
//
// this is actually a group of columns. it doesn't have any method for read or write data
//
// You MUST use this on Select and Insert methods and for append and read data use the sub columns
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
			ret[string(col.Name())] = col.RowI(row)
		}
	}
	ret := make([]any, len(c.columns))
	for i, col := range c.columns {
		ret[i] = col.RowI(row)
	}
	return ret
}

func (c *Tuple) RowI(row int) any {
	return c.Row(row)
}

func (c *Tuple) Scan(row int, dest any) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("scan dest should be a pointer")
	}
	switch val.Elem().Kind() {
	case reflect.Struct:
		return c.scanStruct(row, val)
	case reflect.Map:
		if !c.isNamed {
			return fmt.Errorf("tuple: scan: map should be named")
		}
		return c.scanMap(row, val)
	case reflect.Slice:
		return c.scanSlice(row, val)
	default:
		return fmt.Errorf("tuple: scan: unsupported type %s", val.Elem().Kind())
	}
}

func (c *Tuple) scanMap(row int, val reflect.Value) error {
	if val.Type().Key().Kind() != reflect.String {
		return fmt.Errorf("tuple: scan: map key should be string")
	}
	for _, col := range c.columns {
		colName := string(col.Name())
		val.Elem().SetMapIndex(reflect.ValueOf(colName), reflect.ValueOf(col.RowI(row)))
	}
	return nil
}

func (c *Tuple) scanStruct(row int, val reflect.Value) error {
	for _, col := range c.columns {
		colName := string(col.Name())
		sField, ok := getStructFieldValue(val.Elem(), colName)
		if !ok {
			continue
		}
		sField.Set(reflect.ValueOf(col.RowI(row)))
	}
	return nil
}
func (c *Tuple) scanSlice(row int, val reflect.Value) error {
	for _, col := range c.columns {
		val.Elem().Set(reflect.Append(val.Elem(), reflect.ValueOf(col.RowI(row))))
	}
	return nil
}

func getStructFieldValue(field reflect.Value, name string) (reflect.Value, bool) {
	tField := field.Type()
	for i := 0; i < tField.NumField(); i++ {
		if tag := tField.Field(i).Tag.Get("chname"); tag == name {
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

func (c *Tuple) Validate() error {
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
			column: c,
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
		if col.Validate() != nil {
			return ErrInvalidType{
				column: c,
			}
		}
	}
	return nil
}

func (c *Tuple) ColumnType() string {
	str := helper.TupleStr
	for _, col := range c.columns {
		str += col.ColumnType() + ","
	}
	return str[:len(str)-1] + ")"
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *Tuple) Write(w *readerwriter.Writer) {
	if c.isJSON {
		w.String(c.FullType())
	}
	for _, col := range c.columns {
		col.Write(w)
	}
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
