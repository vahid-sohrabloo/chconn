package column

import (
	"fmt"
	"unsafe"
)

type tuple4Value[T1, T2, T3, T4 any] struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
}

// Tuple4 is a column of Tuple(T1, T2, T3, T4) ClickHouse data type
type Tuple4[T ~struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
}, T1, T2, T3, T4 any] struct {
	Tuple
	col1 Column[T1]
	col2 Column[T2]
	col3 Column[T3]
	col4 Column[T4]
}

// NewTuple4 create a new tuple of Tuple(T1, T2, T3, T4) ClickHouse data type
func NewTuple4[T ~struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
}, T1, T2, T3, T4 any](
	column1 Column[T1],
	column2 Column[T2],
	column3 Column[T3],
	column4 Column[T4],
) *Tuple4[T, T1, T2, T3, T4] {
	return &Tuple4[T, T1, T2, T3, T4]{
		Tuple: Tuple{
			columns: []ColumnBasic{
				column1,
				column2,
				column3,
				column4,
			},
		},
		col1: column1,
		col2: column2,
		col3: column3,
		col4: column4,
	}
}

// NewNested4 create a new nested of Nested(T1, T2, T3, T4) ClickHouse data type
//
// this is actually an alias for NewTuple4(T1, T2, T3, T4).Array()
func NewNested4[T ~struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
}, T1, T2, T3, T4 any](
	column1 Column[T1],
	column2 Column[T2],
	column3 Column[T3],
	column4 Column[T4],
) *Array[T] {
	return NewTuple4[T](
		column1,
		column2,
		column3,
		column4,
	).Array()
}

// Data get all the data in current block as a slice.
func (c *Tuple4[T, T1, T2, T3, T4]) Data() []T {
	val := make([]T, c.NumRow())
	for i := 0; i < c.NumRow(); i++ {
		val[i] = c.Row(i)
	}
	return val
}

// Read reads all the data in current block and append to the input.
func (c *Tuple4[T, T1, T2, T3, T4]) Read(value []T) []T {
	valTuple := *(*[]tuple4Value[T1, T2, T3, T4])(unsafe.Pointer(&value))
	if cap(valTuple)-len(valTuple) >= c.NumRow() {
		valTuple = valTuple[:len(value)+c.NumRow()]
	} else {
		valTuple = append(valTuple, make([]tuple4Value[T1, T2, T3, T4], c.NumRow())...)
	}

	val := valTuple[len(valTuple)-c.NumRow():]
	for i := 0; i < c.NumRow(); i++ {
		val[i].Col1 = c.col1.Row(i)
		val[i].Col2 = c.col2.Row(i)
		val[i].Col3 = c.col3.Row(i)
		val[i].Col4 = c.col4.Row(i)
	}
	return *(*[]T)(unsafe.Pointer(&valTuple))
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Tuple4[T, T1, T2, T3, T4]) Row(row int) T {
	return T(tuple4Value[T1, T2, T3, T4]{
		Col1: c.col1.Row(row),
		Col2: c.col2.Row(row),
		Col3: c.col3.Row(row),
		Col4: c.col4.Row(row),
	})
}

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *Tuple4[T, T1, T2, T3, T4]) RowAny(row int) any {
	return c.Row(row)
}

// Append value for insert
func (c *Tuple4[T, T1, T2, T3, T4]) Append(v T) {
	t := tuple4Value[T1, T2, T3, T4](v)
	c.col1.Append(t.Col1)
	c.col2.Append(t.Col2)
	c.col3.Append(t.Col3)
	c.col4.Append(t.Col4)
}

func (c *Tuple4[T, T1, T2, T3, T4]) canAppend(value any) bool {
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
		if !c.col4.canAppend(v[3]) {
			return false
		}

		return true
	default:
		return false
	}
}

func (c *Tuple4[T, T1, T2, T3, T4]) AppendAny(value any) error {
	switch v := value.(type) {
	case T:
		c.Append(v)

		return nil
	case []any:
		if len(v) != 4 {
			return fmt.Errorf("length of the value slice must be 4")
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

		err = c.col4.AppendAny(v[3])
		if err != nil {
			return fmt.Errorf("could not append col4: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("could not append value of type %T", value)
	}
}

// AppendMulti value for insert
func (c *Tuple4[T, T1, T2, T3, T4]) AppendMulti(v ...T) {
	for _, v := range v {
		t := tuple4Value[T1, T2, T3, T4](v)
		c.col1.Append(t.Col1)
		c.col2.Append(t.Col2)
		c.col3.Append(t.Col3)
		c.col4.Append(t.Col4)
	}
}

// Array return a Array type for this column
func (c *Tuple4[T, T1, T2, T3, T4]) Array() *Array[T] {
	return NewArray[T](c)
}
