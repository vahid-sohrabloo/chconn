package column

import (
	"database/sql"
	"fmt"
	"reflect"
)

// Array2 is a column of Array(Array(T)) ClickHouse data type
type Array2[T any] struct {
	ArrayBase
	rtype reflect.Type
}

// NewArray create a new array column of Array(Array(T)) ClickHouse data type
func NewArray2[T any](array *Array[T]) *Array2[T] {
	rtype := reflect.TypeOf((*T)(nil)).Elem()
	a := &Array2[T]{
		rtype: rtype,
		ArrayBase: ArrayBase{
			dataColumn:      array,
			offsetColumn:    New[uint64](),
			arrayChconnType: "column.Array2[" + rtype.String() + "]",
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
	case sql.Scanner:
		return d.Scan(c.Row(row))
	}

	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "*[][]" + c.rtype.String(),
	}
}

func (c *Array2[T]) Append(v [][]T) {
	c.AppendLen(len(v))
	c.dataColumn.(*Array[T]).AppendMulti(v...)
}

func (c *Array2[T]) AppendAny(value any) error {
	if v, ok := value.([][]T); ok {
		c.Append(v)
		return nil
	}
	return fmt.Errorf("AppendAny error: expected [][]%[1]s, got %[2]T", c.rtype.String(), value)
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
