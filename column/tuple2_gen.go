package column

import (
	"unsafe"
)

type tuple2Value[T1, T2 any] struct {
	Col1 T1
	Col2 T2
}

// Tuple2 is a column of Tuple(T1, T2) ClickHouse data type
type Tuple2[T ~struct {
	Col1 T1
	Col2 T2
}, T1, T2 any] struct {
	Tuple
	col1 Column[T1]
	col2 Column[T2]
}

// NewTuple2 create a new tuple of Tuple(T1, T2) ClickHouse data type
func NewTuple2[T ~struct {
	Col1 T1
	Col2 T2
}, T1, T2 any](
	column1 Column[T1],
	column2 Column[T2],
) *Tuple2[T, T1, T2] {
	return &Tuple2[T, T1, T2]{
		Tuple: Tuple{
			columns: []ColumnBasic{
				column1,
				column2,
			},
		},
		col1: column1,
		col2: column2,
	}
}

// NewNested2 create a new nested of Nested(T1, T2) ClickHouse data type
//
// this is actually an alias for NewTuple2(T1, T2).Array()
func NewNested2[T ~struct {
	Col1 T1
	Col2 T2
}, T1, T2 any](
	column1 Column[T1],
	column2 Column[T2],
) *Array[T] {
	return NewTuple2[T](
		column1,
		column2,
	).Array()
}

// Data get all the data in current block as a slice.
func (c *Tuple2[T, T1, T2]) Data() []T {
	val := make([]T, c.NumRow())
	for i := 0; i < c.NumRow(); i++ {
		val[i] = T(tuple2Value[T1, T2]{
			Col1: c.col1.Row(i),
			Col2: c.col2.Row(i),
		})
	}
	return val
}

// Read reads all the data in current block and append to the input.
func (c *Tuple2[T, T1, T2]) Read(value []T) []T {
	valTuple := *(*[]tuple2Value[T1, T2])(unsafe.Pointer(&value))
	if cap(valTuple)-len(valTuple) >= c.NumRow() {
		valTuple = valTuple[:len(value)+c.NumRow()]
	} else {
		valTuple = append(valTuple, make([]tuple2Value[T1, T2], c.NumRow())...)
	}

	val := valTuple[len(valTuple)-c.NumRow():]
	for i := 0; i < c.NumRow(); i++ {
		val[i].Col1 = c.col1.Row(i)
		val[i].Col2 = c.col2.Row(i)
	}
	return *(*[]T)(unsafe.Pointer(&valTuple))
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Tuple2[T, T1, T2]) Row(row int) T {
	return T(tuple2Value[T1, T2]{
		Col1: c.col1.Row(row),
		Col2: c.col2.Row(row),
	})
}

// Append value for insert
func (c *Tuple2[T, T1, T2]) Append(v ...T) {
	for _, v := range v {
		t := tuple2Value[T1, T2](v)
		c.col1.Append(t.Col1)
		c.col2.Append(t.Col2)
	}
}

// Array return a Array type for this column
func (c *Tuple2[T, T1, T2]) Array() *Array[T] {
	return NewArray[T](c)
}
