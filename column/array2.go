package column

import (
	"fmt"
	"reflect"
)

// Array2 is a column of Array(Array(T)) ClickHouse data type
type Array2[T any] struct {
	ArrayBase
}

// NewArray create a new array column of Array(Array(T)) ClickHouse data type
func NewArray2[T any](array *Array[T]) *Array2[T] {
	a := &Array2[T]{
		ArrayBase: ArrayBase{
			dataColumn:      array,
			offsetColumn:    New[uint64](),
			arrayChconnType: "column.Array2[" + reflect.TypeOf((*T)(nil)).String() + "]",
		},
	}
	return a
}

// Data get all the data in current block as a slice.
func (c *Array2[T]) Data() [][][]T {
	values := make([][][]T, c.offsetColumn.numRow)
	for i := range values {
		values[i] = c.Row(i)
	}
	return values
}

// Read reads all the data in current block and append to the input.
func (c *Array2[T]) Read(value [][][]T) [][][]T {
	if cap(value)-len(value) >= c.NumRow() {
		value = (value)[:len(value)+c.NumRow()]
	} else {
		value = append(value, make([][][]T, c.NumRow())...)
	}
	val := (value)[len(value)-c.NumRow():]
	for i := 0; i < c.NumRow(); i++ {
		val[i] = c.Row(i)
	}
	return value
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Array2[T]) Row(row int) [][]T {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	var val [][]T
	lastRow := c.offsetColumn.Row(row)
	for ; lastOffset < lastRow; lastOffset++ {
		val = append(val, c.dataColumn.(*Array[T]).Row(int(lastOffset)))
	}
	return val
}

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *Array2[T]) RowAny(row int) any {
	return c.Row(row)
}

func (c *Array2[T]) Scan(row int, dest any) error {
	switch d := dest.(type) {
	case *[][]T:
		*d = c.Row(row)
		return nil
	case *any:
		*d = c.Row(row)
		return nil
	}

	return c.ScanValue(row, reflect.ValueOf(dest))
}

func (c *Array2[T]) ScanValue(row int, dest reflect.Value) error {
	destValue := reflect.Indirect(dest)
	if destValue.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	if destValue.Type().AssignableTo(reflect.TypeOf([][]T{})) {
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
func (c *Array2[T]) Append(v [][]T) {
	c.AppendLen(len(v))
	c.dataColumn.(*Array[T]).AppendMulti(v...)
}

// AppendMulti value for insert
func (c *Array2[T]) AppendMulti(v ...[][]T) {
	for _, v := range v {
		c.AppendLen(len(v))
		c.dataColumn.(*Array[T]).AppendMulti(v...)
	}
}

func (c *Array2[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *Array2[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
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
