package column

import (
	"unsafe"
)

type tuple5Value[T1, T2, T3, T4, T5 any] struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
	Col5 T5
}

// Tuple5 is a column of Tuple(T1, T2, T3, T4, T5) ClickHouse data type
type Tuple5[T ~struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
	Col5 T5
}, T1, T2, T3, T4, T5 any] struct {
	Tuple
	col1 Column[T1]
	col2 Column[T2]
	col3 Column[T3]
	col4 Column[T4]
	col5 Column[T5]
}

// NewTuple5 create a new tuple of Tuple(T1, T2, T3, T4, T5) ClickHouse data type
func NewTuple5[T ~struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
	Col5 T5
}, T1, T2, T3, T4, T5 any](
	column1 Column[T1],
	column2 Column[T2],
	column3 Column[T3],
	column4 Column[T4],
	column5 Column[T5],
) *Tuple5[T, T1, T2, T3, T4, T5] {
	return &Tuple5[T, T1, T2, T3, T4, T5]{
		Tuple: Tuple{
			columns: []ColumnBasic{
				column1,
				column2,
				column3,
				column4,
				column5,
			},
		},
		col1: column1,
		col2: column2,
		col3: column3,
		col4: column4,
		col5: column5,
	}
}

// NewNested5 create a new nested of Nested(T1, T2, T3, T4, T5) ClickHouse data type
//
// this is actually an alias for NewTuple5(T1, T2, T3, T4, T5).Array()
func NewNested5[T ~struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
	Col5 T5
}, T1, T2, T3, T4, T5 any](
	column1 Column[T1],
	column2 Column[T2],
	column3 Column[T3],
	column4 Column[T4],
	column5 Column[T5],
) *Array[T] {
	return NewTuple5[T](
		column1,
		column2,
		column3,
		column4,
		column5,
	).Array()
}

// Data get all the data in current block as a slice.
func (c *Tuple5[T, T1, T2, T3, T4, T5]) Data() []T {
	val := make([]T, c.NumRow())
	for i := 0; i < c.NumRow(); i++ {
		val[i] = T(tuple5Value[T1, T2, T3, T4, T5]{
			Col1: c.col1.Row(i),
			Col2: c.col2.Row(i),
			Col3: c.col3.Row(i),
			Col4: c.col4.Row(i),
			Col5: c.col5.Row(i),
		})
	}
	return val
}

// Read reads all the data in current block and append to the input.
func (c *Tuple5[T, T1, T2, T3, T4, T5]) Read(value []T) []T {
	valTuple := *(*[]tuple5Value[T1, T2, T3, T4, T5])(unsafe.Pointer(&value))
	if cap(valTuple)-len(valTuple) >= c.NumRow() {
		valTuple = valTuple[:len(value)+c.NumRow()]
	} else {
		valTuple = append(valTuple, make([]tuple5Value[T1, T2, T3, T4, T5], c.NumRow())...)
	}

	val := valTuple[len(valTuple)-c.NumRow():]
	for i := 0; i < c.NumRow(); i++ {
		val[i].Col1 = c.col1.Row(i)
		val[i].Col2 = c.col2.Row(i)
		val[i].Col3 = c.col3.Row(i)
		val[i].Col4 = c.col4.Row(i)
		val[i].Col5 = c.col5.Row(i)
	}
	return *(*[]T)(unsafe.Pointer(&valTuple))
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Tuple5[T, T1, T2, T3, T4, T5]) Row(row int) T {
	return T(tuple5Value[T1, T2, T3, T4, T5]{
		Col1: c.col1.Row(row),
		Col2: c.col2.Row(row),
		Col3: c.col3.Row(row),
		Col4: c.col4.Row(row),
		Col5: c.col5.Row(row),
	})
}

// Append value for insert
func (c *Tuple5[T, T1, T2, T3, T4, T5]) Append(v ...T) {
	for _, v := range v {
		t := tuple5Value[T1, T2, T3, T4, T5](v)
		c.col1.Append(t.Col1)
		c.col2.Append(t.Col2)
		c.col3.Append(t.Col3)
		c.col4.Append(t.Col4)
		c.col5.Append(t.Col5)
	}
}

// Array return a Array type for this column
func (c *Tuple5[T, T1, T2, T3, T4, T5]) Array() *Array[T] {
	return NewArray[T](c)
}
