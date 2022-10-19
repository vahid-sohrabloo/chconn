package column

import (
	"unsafe"
)

type tuple3Value[T1, T2, T3 any] struct {
	Col1 T1
	Col2 T2
	Col3 T3
}

// Tuple3 is a column of Tuple(T1, T2, T3) ClickHouse data type
type Tuple3[T ~struct {
	Col1 T1
	Col2 T2
	Col3 T3
}, T1, T2, T3 any] struct {
	Tuple
	col1 Column[T1]
	col2 Column[T2]
	col3 Column[T3]
}

// NewTuple3 create a new tuple of Tuple(T1, T2, T3) ClickHouse data type
func NewTuple3[T ~struct {
	Col1 T1
	Col2 T2
	Col3 T3
}, T1, T2, T3 any](
	column1 Column[T1],
	column2 Column[T2],
	column3 Column[T3],
) *Tuple3[T, T1, T2, T3] {
	return &Tuple3[T, T1, T2, T3]{
		Tuple: Tuple{
			columns: []ColumnBasic{
				column1,
				column2,
				column3,
			},
		},
		col1: column1,
		col2: column2,
		col3: column3,
	}
}

// NewNested3 create a new nested of Nested(T1, T2, T3) ClickHouse data type
//
// this is actually an alias for NewTuple3(T1, T2, T3).Array()
func NewNested3[T ~struct {
	Col1 T1
	Col2 T2
	Col3 T3
}, T1, T2, T3 any](
	column1 Column[T1],
	column2 Column[T2],
	column3 Column[T3],
) *Array[T] {
	return NewTuple3[T](
		column1,
		column2,
		column3,
	).Array()
}

// Data get all the data in current block as a slice.
func (c *Tuple3[T, T1, T2, T3]) Data() []T {
	val := make([]T, c.NumRow())
	for i := 0; i < c.NumRow(); i++ {
		val[i] = T(tuple3Value[T1, T2, T3]{
			Col1: c.col1.Row(i),
			Col2: c.col2.Row(i),
			Col3: c.col3.Row(i),
		})
	}
	return val
}

// Read reads all the data in current block and append to the input.
func (c *Tuple3[T, T1, T2, T3]) Read(value []T) []T {
	valTuple := *(*[]tuple3Value[T1, T2, T3])(unsafe.Pointer(&value))
	if cap(valTuple)-len(valTuple) >= c.NumRow() {
		valTuple = valTuple[:len(value)+c.NumRow()]
	} else {
		valTuple = append(valTuple, make([]tuple3Value[T1, T2, T3], c.NumRow())...)
	}

	val := valTuple[len(valTuple)-c.NumRow():]
	for i := 0; i < c.NumRow(); i++ {
		val[i].Col1 = c.col1.Row(i)
		val[i].Col2 = c.col2.Row(i)
		val[i].Col3 = c.col3.Row(i)
	}
	return *(*[]T)(unsafe.Pointer(&valTuple))
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Tuple3[T, T1, T2, T3]) Row(row int) T {
	return T(tuple3Value[T1, T2, T3]{
		Col1: c.col1.Row(row),
		Col2: c.col2.Row(row),
		Col3: c.col3.Row(row),
	})
}

// Append value for insert
func (c *Tuple3[T, T1, T2, T3]) Append(v ...T) {
	for _, v := range v {
		t := tuple3Value[T1, T2, T3](v)
		c.col1.Append(t.Col1)
		c.col2.Append(t.Col2)
		c.col3.Append(t.Col3)
	}
}

// Array return a Array type for this column
func (c *Tuple3[T, T1, T2, T3]) Array() *Array[T] {
	return NewArray[T](c)
}
