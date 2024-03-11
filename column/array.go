package column

import (
	"fmt"
	"reflect"
	"unsafe"
)

// Array is a column of Array(T) ClickHouse data type
type Array[T any] struct {
	ArrayBase
	columnData []T
	rtype      reflect.Type
}

// NewArray create a new array column of Array(T) ClickHouse data type
func NewArray[T any](dataColumn Column[T]) *Array[T] {
	rtype := reflect.TypeOf((*T)(nil)).Elem()
	a := &Array[T]{
		rtype: rtype,
		ArrayBase: ArrayBase{
			dataColumn:      dataColumn,
			offsetColumn:    New[uint64](),
			arrayChconnType: "column.Array[" + rtype.String() + "]",
		},
	}
	a.resetHook = func() {
		a.columnData = a.columnData[:0]
	}
	return a
}

// Data get all the data in current block as a slice.
func (c *Array[T]) Data() [][]T {
	values := make([][]T, c.offsetColumn.numRow)
	offsets := c.Offsets()
	var lastOffset uint64
	columnData := c.getColumnData()
	for i, offset := range offsets {
		val := make([]T, offset-lastOffset)
		copy(val, columnData[lastOffset:offset])
		values[i] = val
		lastOffset = offset
	}
	return values
}

// Read reads all the data in current block and append to the input.
func (c *Array[T]) Read(value [][]T) [][]T {
	offsets := c.Offsets()
	var lastOffset uint64
	columnData := c.getColumnData()
	for _, offset := range offsets {
		val := make([]T, offset-lastOffset)
		copy(val, columnData[lastOffset:offset])
		value = append(value, val)
		lastOffset = offset
	}
	return value
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Array[T]) Row(row int) []T {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	var val []T
	val = append(val, c.getColumnData()[lastOffset:c.offsetColumn.Row(row)]...)
	return val
}

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *Array[T]) RowAny(row int) any {
	return c.Row(row)
}

func (c *Array[T]) Scan(row int, dest any) error {
	switch d := dest.(type) {
	case *[]T:
		*d = c.Row(row)
		return nil
	case *any:
		*d = c.Row(row)
		return nil
	}

	err := c.ScanValue(row, reflect.ValueOf(dest))
	return err
}

func (c *Array[T]) ScanValue(row int, dest reflect.Value) error {
	destValue := reflect.Indirect(dest)
	if destValue.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	if destValue.Type().AssignableTo(reflect.TypeOf([]T{})) {
		destValue.Set(reflect.ValueOf(c.Row(row)))
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

// Append value for insert
func (c *Array[T]) Append(v []T) {
	c.AppendLen(len(v))
	c.dataColumn.(Column[T]).AppendMulti(v...)
}

func (c *Array[T]) AppendAny(value any) error {
	switch v := value.(type) {
	case []T:
		c.Append(v)
		return nil
	case []bool:
		if c.rtype.Kind() == reflect.Int8 || c.rtype.Kind() == reflect.Uint8 {
			c.Append(*(*[]T)(unsafe.Pointer(&v)))
			return nil
		}
	}
	return fmt.Errorf("AppendAny error: expected []%s, got %T", c.rtype.String(), value)
}

// AppendMulti value for insert
func (c *Array[T]) AppendMulti(v ...[]T) {
	for _, v := range v {
		c.AppendLen(len(v))
		c.dataColumn.(Column[T]).AppendMulti(v...)
	}
}

// Append single item value for insert
//
// it should use with AppendLen
//
// Example:
//
//	c.AppendLen(2) // insert 2 items
//	c.AppendItem(1)
//	c.AppendItem(2)
func (c *Array[T]) AppendItem(v T) {
	c.dataColumn.(Column[T]).Append(v)
}

// Array return a Array type for this column
func (c *Array[T]) Array() *Array2[T] {
	return NewArray2(c)
}

func (c *Array[T]) getColumnData() []T {
	if len(c.columnData) == 0 {
		c.columnData = c.dataColumn.(Column[T]).Data()
	}
	return c.columnData
}

func (c *Array[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *Array[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
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
