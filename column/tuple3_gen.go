package column

import (
	"fmt"
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
		val[i] = c.Row(i)
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

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *Tuple3[T, T1, T2, T3]) RowAny(row int) any {
	return c.Row(row)
}

// Append value for insert
func (c *Tuple3[T, T1, T2, T3]) Append(v T) {
	t := tuple3Value[T1, T2, T3](v)
	c.col1.Append(t.Col1)
	c.col2.Append(t.Col2)
	c.col3.Append(t.Col3)
}

func (c *Tuple3[T, T1, T2, T3]) canAppend(value any) bool {
	switch v := value.(type) {
	case T:
		return true
	case []any:
		if len(v) != 2 {
			return false
		}

		if !c.col1.canAppend(v[0]) {
			return false
		}
		if !c.col2.canAppend(v[1]) {
			return false
		}
		if !c.col3.canAppend(v[2]) {
			return false
		}

		return true
	default:
		return false
	}
}

func (c *Tuple3[T, T1, T2, T3]) AppendAny(value any) error {
	switch v := value.(type) {
	case T:
		c.Append(v)

		return nil
	case []any:
		if len(v) != 3 {
			return fmt.Errorf("length of the value slice must be 3")
		}

		err := c.col1.AppendAny(v[0])
		if err != nil {
			return fmt.Errorf("could not append col1: %w", err)
		}
		err = c.col2.AppendAny(v[1])
		if err != nil {
			return fmt.Errorf("could not append col2: %w", err)
		}

		err = c.col3.AppendAny(v[2])
		if err != nil {
			return fmt.Errorf("could not append col3: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("could not append value of type %T", value)
	}
}

// AppendMulti value for insert
func (c *Tuple3[T, T1, T2, T3]) AppendMulti(v ...T) {
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
