package column

import (
	"database/sql"
	"fmt"
	"reflect"
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
	case sql.Scanner:
		return d.Scan(c.Row(row))
	}

	if c.rtype.String() == "column.NothingData" {
		return nil
	}

	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "*[]" + c.rtype.String(),
	}
}

// Append value for insert
func (c *Array[T]) Append(v []T) {
	c.AppendLen(len(v))
	c.dataColumn.(Column[T]).AppendMulti(v...)
}

func (c *Array[T]) AppendAny(value any) error {
	if v, ok := value.([]T); ok {
		c.Append(v)
		return nil
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
