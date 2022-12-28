package column

// Tuple1 is a column of Tuple(T1) ClickHouse data type
type Tuple1[T1 any] struct {
	Tuple
	col1 Column[T1]
}

// NewTuple1 create a new tuple of Tuple(T1) ClickHouse data type
func NewTuple1[T1 any](
	column1 Column[T1],
) *Tuple1[T1] {
	return &Tuple1[T1]{
		Tuple: Tuple{
			columns: []ColumnBasic{
				column1,
			},
		},
		col1: column1,
	}
}

// NewNested1 create a new nested of Nested(T1) ClickHouse data type
//
// this is actually an alias for NewTuple1(T1).Array()
func NewNested1[T any](
	column1 Column[T],
) *Array[T] {
	return NewTuple1(
		column1,
	).Array()
}

// Data get all the data in current block as a slice.
func (c *Tuple1[T]) Data() []T {
	return c.col1.Data()
}

// Read reads all the data in current block and append to the input.
func (c *Tuple1[T]) Read(value []T) []T {
	return c.col1.Read(value)
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Tuple1[T]) Row(row int) T {
	return c.col1.Row(row)
}

// RowI return the value of given row.
// NOTE: Row number start from zero
func (c *Tuple1[T]) RowI(row int) any {
	return c.Row(row)
}

// Append value for insert
func (c *Tuple1[T]) Append(v ...T) {
	c.col1.Append(v...)
}

// Array return a Array type for this column
func (c *Tuple1[T]) Array() *Array[T] {
	return NewArray[T](c)
}
