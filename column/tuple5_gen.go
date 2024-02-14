package column

import (
	"fmt"
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
		val[i] = c.Row(i)
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

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *Tuple5[T, T1, T2, T3, T4, T5]) RowAny(row int) any {
	return c.Row(row)
}

// Append value for insert
func (c *Tuple5[T, T1, T2, T3, T4, T5]) Append(v T) {
	t := tuple5Value[T1, T2, T3, T4, T5](v)
	c.col1.Append(t.Col1)
	c.col2.Append(t.Col2)
	c.col3.Append(t.Col3)
	c.col4.Append(t.Col4)
	c.col5.Append(t.Col5)
}

func (c *Tuple5[T, T1, T2, T3, T4, T5]) AppendAny(value any) error {
	switch v := value.(type) {
	case T:
		c.Append(v)

		return nil
	case []any:
		if len(v) != 5 {
			return fmt.Errorf("length of the value slice must be 5")
		}

		err := c.col1.AppendAny(v[0])
		if err != nil {
			return fmt.Errorf("could not append col1: %w", err)
		}

		err = c.col2.AppendAny(v[1])
		if err != nil {
			c.col1.Remove(c.col1.NumRow() - 1)
			return fmt.Errorf("could not append col2: %w", err)
		}

		err = c.col3.AppendAny(v[2])
		if err != nil {
			c.col1.Remove(c.col1.NumRow() - 1)
			c.col2.Remove(c.col2.NumRow() - 1)
			return fmt.Errorf("could not append col3: %w", err)
		}

		err = c.col4.AppendAny(v[3])
		if err != nil {
			c.col1.Remove(c.col1.NumRow() - 1)
			c.col2.Remove(c.col2.NumRow() - 1)
			c.col3.Remove(c.col3.NumRow() - 1)
			return fmt.Errorf("could not append col4: %w", err)
		}

		err = c.col4.AppendAny(v[4])
		if err != nil {
			c.col1.Remove(c.col1.NumRow() - 1)
			c.col2.Remove(c.col2.NumRow() - 1)
			c.col3.Remove(c.col3.NumRow() - 1)
			c.col4.Remove(c.col4.NumRow() - 1)
			return fmt.Errorf("could not append col5: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("could not append value of type %T", value)
	}
}

// AppendMulti value for insert
func (c *Tuple5[T, T1, T2, T3, T4, T5]) AppendMulti(v ...T) {
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
