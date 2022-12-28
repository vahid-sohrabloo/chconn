package column

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/vahid-sohrabloo/chconn/v2/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
)

// Map is a column of Map(K,V) ClickHouse data type
// Map in clickhouse actually is a array of pair(K,V)
//
// MapBase is a base class for map and also for non generic  of map to use dynamic select column
type MapBase struct {
	column
	offsetColumn *Base[uint64]
	keyColumn    ColumnBasic
	valueColumn  ColumnBasic
	offset       uint64
	resetHook    func()
}

// NewMapBase create a new map column of Map(K,V) ClickHouse data type
func NewMapBase(
	keyColumn, valueColumn ColumnBasic,
) *MapBase {
	a := &MapBase{
		keyColumn:    keyColumn,
		valueColumn:  valueColumn,
		offsetColumn: New[uint64](),
	}
	return a
}

// Each run the given function for each row in the column with start and end offsets.
//
// in some cases  like Map(K,Array(Nullable)) you can't read the data with generic for this situations. you can use this function.
//
// For example
// colNullableArrayReadKey := colNullableArrayRead.KeyColumn().Data()
// colNullableArrayReadValue := colNullableArrayRead.ValueColumn().(*column.ArrayNullable[V]).DataP()
//
//	colNullableArrayRead.Each(func(start, end uint64) bool {
//			val := make(map[string][]*V)
//			for ki, key := range colNullableArrayReadKey[start:end] {
//				val[key] = colNullableArrayReadValue[start:end][ki]
//			}
//			colArrayNullableData = append(colArrayNullableData, val)
//			return true
//		})
func (c *MapBase) Each(f func(start, end uint64) bool) {
	offsets := c.Offsets()
	if len(offsets) == 0 {
		return
	}
	var lastOffset uint64
	for _, offset := range offsets {
		if !f(lastOffset, offset) {
			return
		}
		lastOffset = offset
	}
}

// AppendLen Append len for insert
func (c *MapBase) AppendLen(v int) {
	c.offset += uint64(v)
	c.offsetColumn.Append(c.offset)
}

func (c *MapBase) RowI(row int) any {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	var val map[any]any
	endOffset := c.offsetColumn.Row(row)
	for i := lastOffset; i < endOffset; i++ {
		if val == nil {
			val = make(map[any]any)
		}
		val[c.keyColumn.RowI(int(i))] = c.valueColumn.RowI(int(i))
	}
	return val
}

func (c *MapBase) Scan(row int, dest any) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("scan dest should be a pointer")
	}
	if val.Elem().Kind() == reflect.Map {
		return c.scanMap(row, val)
	}

	if val.Elem().Kind() == reflect.Struct {
		return c.scanStruct(row, val)
	}

	return fmt.Errorf("scan dest should be a pointer of map or struct")

}

func (c *MapBase) scanMap(row int, val reflect.Value) error {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	endOffset := c.offsetColumn.Row(row)
	if val.Elem().IsNil() {
		val.Elem().Set(reflect.MakeMap(val.Elem().Type()))
	}
	for i := lastOffset; i < endOffset; i++ {
		val.Elem().SetMapIndex(reflect.ValueOf(c.keyColumn.RowI(int(i))), reflect.ValueOf(c.valueColumn.RowI(int(i))))
	}
	return nil
}

func (c *MapBase) scanStruct(row int, val reflect.Value) error {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	endOffset := c.offsetColumn.Row(row)
	// todo  find a better way to find key and value field
	for i := lastOffset; i < endOffset; i++ {
		if err := c.keyColumn.Scan(int(i), val.Elem().Field(0).Addr().Interface()); err != nil {
			return err
		}
		if err := c.valueColumn.Scan(int(i), val.Elem().Field(1).Addr().Interface()); err != nil {
			return err
		}
	}
	return nil
}

// NumRow return number of row for this block
func (c *MapBase) NumRow() int {
	return c.offsetColumn.NumRow()
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *MapBase) Reset() {
	c.offsetColumn.Reset()
	c.keyColumn.Reset()
	c.valueColumn.Reset()
	c.offset = 0
}

// Offsets return all the offsets in current block
func (c *MapBase) Offsets() []uint64 {
	return c.offsetColumn.Data()
}

// TotalRows return total rows on this block of array data
func (c *MapBase) TotalRows() int {
	if c.offsetColumn.totalByte == 0 {
		return 0
	}
	return int(binary.LittleEndian.Uint64(c.offsetColumn.b[c.offsetColumn.totalByte-8 : c.offsetColumn.totalByte]))
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *MapBase) SetWriteBufferSize(row int) {
	c.offsetColumn.SetWriteBufferSize(row)
	c.keyColumn.SetWriteBufferSize(row)
	c.valueColumn.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *MapBase) ReadRaw(num int, r *readerwriter.Reader) error {
	c.offsetColumn.Reset()
	err := c.offsetColumn.ReadRaw(num, r)
	if err != nil {
		return fmt.Errorf("map: read offset column: %w", err)
	}

	err = c.keyColumn.ReadRaw(c.TotalRows(), r)
	if err != nil {
		return fmt.Errorf("map: read key column: %w", err)
	}

	err = c.valueColumn.ReadRaw(c.TotalRows(), r)
	if err != nil {
		return fmt.Errorf("map: read value column: %w", err)
	}
	if c.resetHook != nil {
		c.resetHook()
	}
	return nil
}

// KeyColumn return the key column
func (c *MapBase) KeyColumn() ColumnBasic {
	return c.keyColumn
}

// ValueColumn return the value column
func (c *MapBase) ValueColumn() ColumnBasic {
	return c.valueColumn
}

// HeaderReader reads header data from reader
// it uses internally
func (c *MapBase) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	err := c.offsetColumn.HeaderReader(r, readColumn, revision)
	if err != nil {
		return err
	}
	c.name = c.offsetColumn.name
	c.chType = c.offsetColumn.chType
	c.keyColumn.SetName(c.name)
	c.valueColumn.SetName(c.name)

	err = c.keyColumn.HeaderReader(r, false, revision)
	if err != nil {
		return fmt.Errorf("map: read key header: %w", err)
	}
	err = c.valueColumn.HeaderReader(r, false, revision)
	if err != nil {
		return fmt.Errorf("map: read value header: %w", err)
	}
	return nil
}

func (c *MapBase) Validate() error {
	chType := helper.FilterSimpleAggregate(c.chType)

	if !helper.IsMap(chType) {
		return ErrInvalidType{
			column: c,
		}
	}
	columnsMap, err := helper.TypesInParentheses(chType[helper.LenMapStr : len(chType)-1])
	if err != nil {
		return fmt.Errorf("map invalid types %w", err)
	}

	if len(columnsMap) != 2 {
		//nolint:goerr113
		return fmt.Errorf("columns number is not equal to map columns number: %d != %d", len(columnsMap), 2)
	}

	c.keyColumn.SetType(columnsMap[0].ChType)
	c.keyColumn.SetName(columnsMap[0].Name)
	c.valueColumn.SetType(columnsMap[1].ChType)
	c.valueColumn.SetName(columnsMap[1].Name)

	if c.keyColumn.Validate() != nil {
		return ErrInvalidType{
			column: c,
		}
	}
	if c.valueColumn.Validate() != nil {
		return ErrInvalidType{
			column: c,
		}
	}
	return nil
}

func (c *MapBase) ColumnType() string {
	return strings.ReplaceAll(
		strings.ReplaceAll(helper.MapTypeStr, "<key>", c.keyColumn.ColumnType()),
		"<value>", c.valueColumn.ColumnType())
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *MapBase) WriteTo(w io.Writer) (int64, error) {
	nw, err := c.offsetColumn.WriteTo(w)
	if err != nil {
		return nw, fmt.Errorf("write len data: %w", err)
	}
	n, errDataColumn := c.keyColumn.WriteTo(w)
	nw += n
	if errDataColumn != nil {
		return nw, fmt.Errorf("write key data: %w", errDataColumn)
	}

	n, errDataColumn = c.valueColumn.WriteTo(w)
	nw += n
	if errDataColumn != nil {
		return nw, fmt.Errorf("write value data: %w", errDataColumn)
	}

	return nw + n, errDataColumn
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *MapBase) HeaderWriter(w *readerwriter.Writer) {
	c.keyColumn.HeaderWriter(w)
	c.valueColumn.HeaderWriter(w)
}

func (c *MapBase) FullType() string {
	if len(c.name) == 0 {
		return "Map(" + c.keyColumn.FullType() + ", " + c.valueColumn.FullType() + ")"
	}
	return string(c.name) + " Map(" + c.keyColumn.FullType() + ", " + c.valueColumn.FullType() + ")"
}
