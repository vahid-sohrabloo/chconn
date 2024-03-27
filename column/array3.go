package column

import (
	"database/sql"
	"fmt"
	"reflect"
)

// Array3 is a column of Array(Array(Array(T))) ClickHouse data type
type Array3[T any] struct {
	ArrayBase
	rtype reflect.Type
}

// NewArray create a new array column of Array(Array(Array(T))) ClickHouse data type
func NewArray3[T any](array *Array2[T]) *Array3[T] {
	rtype := reflect.TypeOf((*T)(nil)).Elem()
	a := &Array3[T]{
		rtype: rtype,
		ArrayBase: ArrayBase{
			dataColumn:      array,
			offsetColumn:    New[uint64](),
			arrayChconnType: "column.Array3[" + rtype.String() + "]",
		},
	}
	return a
}

// Data get all the data in current block as a slice.
func (c *Array3[T]) Data() [][][][]T {
	values := make([][][][]T, c.offsetColumn.numRow)
	for i := range values {
		values[i] = c.Row(i)
	}
	return values
}

// Read reads all the data in current block and append to the input.
func (c *Array3[T]) Read(value [][][][]T) [][][][]T {
	if cap(value)-len(value) >= c.NumRow() {
		value = (value)[:len(value)+c.NumRow()]
	} else {
		value = append(value, make([][][][]T, c.NumRow())...)
	}
	val := (value)[len(value)-c.NumRow():]
	for i := 0; i < c.NumRow(); i++ {
		val[i] = c.Row(i)
	}
	return value
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Array3[T]) Row(row int) [][][]T {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	var val [][][]T
	lastRow := c.offsetColumn.Row(row)
	for ; lastOffset < lastRow; lastOffset++ {
		val = append(val, c.dataColumn.(*Array2[T]).Row(int(lastOffset)))
	}
	return val
}

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *Array3[T]) RowAny(row int) any {
	return c.Row(row)
}

func (c *Array3[T]) Scan(row int, dest any) error {
	switch v := dest.(type) {
	case *[][][]T:
		*v = c.Row(row)
		return nil
	case *any:
		*v = c.Row(row)
		return nil
	case sql.Scanner:
		return v.Scan(c.Row(row))
	}

	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "*[][][]" + c.rtype.String(),
	}
}

// Append value for insert
func (c *Array3[T]) Append(v [][][]T) {
	c.AppendLen(len(v))
	c.dataColumn.(*Array2[T]).AppendMulti(v...)
}

func (c *Array3[T]) canAppend(value any) bool {
	if _, ok := value.([][][]T); ok {
		return true
	}
	return false
}

func (c *Array3[T]) AppendAny(value any) error {
	if v, ok := value.([][][]T); ok {
		c.Append(v)
		return nil
	}
	return fmt.Errorf("AppendAny error: expected [][][]%[1]s, got %[2]T", c.rtype.String(), value)
}

// AppendMulti value for insert
func (c *Array3[T]) AppendMulti(v ...[][][]T) {
	for _, v := range v {
		c.AppendLen(len(v))
		c.dataColumn.(*Array2[T]).AppendMulti(v...)
	}
}

// Array return a Array type for this column
func (c *Array2[T]) Array() *Array3[T] {
	return NewArray3(c)
}

func (c *Array3[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		panic("array level is too deep")
	}
	return c
}

func (c *Array3[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	b = append(b, '[')

	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	offset := c.offsetColumn.Row(row)
	first := true
	for i := lastOffset; i < offset; i++ {
		b = c.dataColumn.ToJSON(int(i), ignoreDoubleQuotes, b)

		if !first {
			b = append(b, ',')
		} else {
			first = false
		}
	}
	return append(b, ']')
}
