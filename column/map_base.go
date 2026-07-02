package column

import (
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/kelindar/bitmap"
	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

// Map is a column of Map(K,V) ClickHouse data type
// Map in clickhouse actually is a array of pair(K,V)
//
// MapBase is a base class for map and also for non generic  of map to use dynamic select column
type MapBase struct {
	column
	offsetColumn     *Base[uint64]
	keyColumn        ColumnCore
	valueColumn      ColumnCore
	offset           uint64
	mapChconnType    string
	resetHook        func()
	bitmapDeleteKeep bitmap.Bitmap
}

// NewMapBase create a new map column of Map(K,V) ClickHouse data type
func NewMapBase(
	keyColumn, valueColumn ColumnCore,
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
	c.preHookAppend()
	c.offset += uint64(v)
	c.offsetColumn.Append(c.offset)
}

func (c *MapBase) canAppend(value any) bool {
	mapVal := reflect.ValueOf(value)
	if mapVal.Kind() != reflect.Map {
		return false
	}

	c.AppendLen(mapVal.Len())
	for _, key := range mapVal.MapKeys() {
		k := key.Interface()
		if !c.keyColumn.canAppend(k) {
			return false
		}
		val := mapVal.MapIndex(key).Interface()
		if !c.valueColumn.canAppend(val) {
			return false
		}
	}

	return true
}

func (c *MapBase) AppendAny(value any) error {
	mapVal := reflect.ValueOf(value)
	if mapVal.Kind() != reflect.Map {
		return fmt.Errorf("value is not a map")
	}

	c.AppendLen(mapVal.Len())
	for _, key := range mapVal.MapKeys() {
		k := key.Interface()
		err := c.keyColumn.AppendAny(k)
		if err != nil {
			return fmt.Errorf("coult not append key %v to key column: %w", k, err)
		}
		val := mapVal.MapIndex(key).Interface()
		err = c.valueColumn.AppendAny(val)
		if err != nil {
			return fmt.Errorf("coult not append value %v to value column: %w", val, err)
		}
	}

	return nil
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *MapBase) Remove(n int) {
	if c.NumRow() == 0 || c.NumRow() <= n {
		return
	}
	var offset uint64
	if n != 0 {
		offset = c.offsetColumn.values[n-1]
	}
	c.offsetColumn.Remove(n)
	c.keyColumn.Remove(int(offset))
	c.valueColumn.Remove(int(offset))
}

// Delete removes rows in the range [start, end)
func (c *MapBase) Delete(start, end int) {
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
	c.keyColumn.Delete(int(startOffset), int(endOffset))
	c.valueColumn.Delete(int(startOffset), int(endOffset))

	// Remove the offsets for the deleted rows
	c.offsetColumn.Delete(start, end)

	// Adjust all remaining offsets after the deleted range
	for i := start; i < c.offsetColumn.NumRow(); i++ {
		c.offsetColumn.values[i] -= elementsToRemove
	}
}

func (c *MapBase) DeleteFunc(del func(row int) bool) {
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

func (c *MapBase) startBatchDelete() {
	lenNeeded := (c.NumRow() % 64) + 1
	if cap(c.bitmapDeleteKeep) < lenNeeded {
		c.bitmapDeleteKeep = make(bitmap.Bitmap, lenNeeded)
	} else {
		clear(c.bitmapDeleteKeep)
	}
}

func (c *MapBase) batchDeleteKeep(start, end int) {
	for i := start; i < end; i++ {
		c.bitmapDeleteKeep.Set(uint32(i))
	}
}

func (c *MapBase) endBatchDelete() {
	prevOffset := uint64(0)
	keepIndex := 0
	c.offset = 0
	c.keyColumn.startBatchDelete()
	c.valueColumn.startBatchDelete()
	for i := 0; i < c.NumRow(); i++ {
		if !c.bitmapDeleteKeep.Contains(uint32(i)) {
			continue
		}
		if i != 0 {
			prevOffset = c.offsetColumn.Row(i - 1)
		}
		currentOffset := c.offsetColumn.Row(i)
		c.keyColumn.batchDeleteKeep(int(prevOffset), int(currentOffset))
		c.valueColumn.batchDeleteKeep(int(prevOffset), int(currentOffset))
		c.offset += currentOffset - prevOffset
		c.offsetColumn.values[keepIndex] = c.offset
		keepIndex++
	}

	c.offsetColumn.values = c.offsetColumn.values[:keepIndex]
	c.offsetColumn.numRow = keepIndex
	c.keyColumn.endBatchDelete()
	c.valueColumn.endBatchDelete()
}

func (c *MapBase) RowAny(row int) any {
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
		val[c.keyColumn.RowAny(int(i))] = c.valueColumn.RowAny(int(i))
	}
	return val
}

func (c *MapBase) Scan(row int, dest any) error {
	val := reflect.ValueOf(dest)
	return c.ScanValue(row, val)
}

func (c *MapBase) ScanValue(row int, dest reflect.Value) error {
	if dest.Kind() != reflect.Ptr {
		return fmt.Errorf("scan dest should be a pointer")
	}
	if dest.Elem().Kind() == reflect.Map {
		return c.scanMap(row, dest)
	}

	if dest.Elem().Kind() == reflect.Slice && dest.Elem().Type().Elem().Kind() == reflect.Struct {
		return c.scanStruct(row, dest)
	}

	return fmt.Errorf("scan dest should be a pointer of map or slice of struct")
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
		val.Elem().SetMapIndex(reflect.ValueOf(c.keyColumn.RowAny(int(i))), reflect.ValueOf(c.valueColumn.RowAny(int(i))))
	}
	return nil
}

func (c *MapBase) scanStruct(row int, dest reflect.Value) error {
	var lastOffset int
	if row != 0 {
		lastOffset = int(c.offsetColumn.Row(row - 1))
	}

	offset := int(c.offsetColumn.Row(row))
	rSlice := reflect.MakeSlice(dest.Elem().Type(), offset-lastOffset, offset-lastOffset)
	for i, b := lastOffset, 0; i < offset; i, b = i+1, b+1 {
		sFieldKey, ok := getStructFieldValue(rSlice.Index(b), "key")
		if !ok {
			sFieldKey = rSlice.Index(b).Field(0)
		}
		if err := c.keyColumn.Scan(i, sFieldKey.Addr().Interface()); err != nil {
			return fmt.Errorf("scan key: %w", err)
		}
		sFieldValue, ok := getStructFieldValue(rSlice.Index(b), "value")
		if !ok {
			sFieldValue = rSlice.Index(b).Field(1)
		}
		if err := c.valueColumn.Scan(i, sFieldValue.Addr().Interface()); err != nil {
			return fmt.Errorf("scan value: %w", err)
		}
	}
	dest.Elem().Set(rSlice)
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
	if len(c.offsetColumn.values) == 0 {
		return 0
	}
	return int(c.offsetColumn.values[len(c.offsetColumn.values)-1])
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
func (c *MapBase) ReadRaw(num int) error {
	c.offsetColumn.Reset()
	err := c.offsetColumn.ReadRaw(num)
	if err != nil {
		return fmt.Errorf("map: read offset column: %w", err)
	}

	err = c.keyColumn.ReadRaw(c.TotalRows())
	if err != nil {
		return fmt.Errorf("map: read key column: %w", err)
	}

	err = c.valueColumn.ReadRaw(c.TotalRows())
	if err != nil {
		return fmt.Errorf("map: read value column: %w", err)
	}
	if c.resetHook != nil {
		c.resetHook()
	}
	return nil
}

// KeyColumn return the key column
func (c *MapBase) KeyColumn() ColumnCore {
	return c.keyColumn
}

// ValueColumn return the value column
func (c *MapBase) ValueColumn() ColumnCore {
	return c.valueColumn
}

// ReadHeader reads header data from reader
// it uses internally
func (c *MapBase) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	err := c.column.ReadHeader(r, serverInfo)
	if err != nil {
		return err
	}
	c.offsetColumn.r = r

	err = c.keyColumn.ReadHeader(r, serverInfo)
	if err != nil {
		return fmt.Errorf("map: read key header: %w", err)
	}
	err = c.valueColumn.ReadHeader(r, serverInfo)
	if err != nil {
		return fmt.Errorf("map: read value header: %w", err)
	}
	return nil
}

func (c *MapBase) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	chType := helper.FilterSimpleAggregate(c.columnHeader.ChType)

	if !helper.IsMap(chType) {
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	columnsMap, err := helper.TypesInParentheses(chType[helper.LenMapStr : len(chType)-1])
	if err != nil {
		return fmt.Errorf("map invalid types %w", err)
	}

	if len(columnsMap) != 2 {
		//nolint:err113
		return fmt.Errorf("columns number is not equal to map columns number: %d != %d", len(columnsMap), 2)
	}

	if err := c.keyColumn.SetColumnHeader(ColumnHeader{
		ChType: columnsMap[0].ChType,
		Name:   columnsMap[0].Name,
	}); err != nil {
		if !isInvalidType(err) {
			return fmt.Errorf("set key column header: %w", err)
		}
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}

	if err := c.valueColumn.SetColumnHeader(ColumnHeader{
		ChType: columnsMap[1].ChType,
		Name:   columnsMap[1].Name,
	}); err != nil {
		if !isInvalidType(err) {
			return fmt.Errorf("set value column header: %w", err)
		}
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	return nil
}
func (c *MapBase) ValidateInsert() error {
	if err := c.keyColumn.ValidateInsert(); err != nil {
		return err
	}
	return c.valueColumn.ValidateInsert()
}

func (c *MapBase) chconnType() string {
	if c.mapChconnType != "" {
		return c.mapChconnType
	}
	return "MapBase(" + c.keyColumn.chconnType() + ", " + c.valueColumn.chconnType() + ")"
}

func (c *MapBase) structType() string {
	return strings.ReplaceAll(
		strings.ReplaceAll(helper.MapTypeStr, "<key>", c.keyColumn.structType()),
		"<value>", c.valueColumn.structType())
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
	if len(c.columnHeader.Name) == 0 {
		return "Map(" + c.keyColumn.FullType() + ", " + c.valueColumn.FullType() + ")"
	}
	return string(c.columnHeader.Name) + " Map(" + c.keyColumn.FullType() + ", " + c.valueColumn.FullType() + ")"
}

// ToJSON
func (c *MapBase) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	b = append(b, '{')
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	offset := c.offsetColumn.Row(row)
	for i := lastOffset; i < offset; i++ {
		if i != lastOffset {
			b = append(b, ',')
		}
		b = append(b, '"')
		b = c.keyColumn.ToJSON(int(i), true, b)
		b = append(b, '"', ':')
		b = c.valueColumn.ToJSON(int(i), false, b)
	}
	return append(b, '}')
}

func (c *MapBase) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexMap))
	c.keyColumn.writeBinaryDataTo(w)
	c.valueColumn.writeBinaryDataTo(w)
}
