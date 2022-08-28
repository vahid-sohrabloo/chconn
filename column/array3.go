package column

// Array3 is a column of Array(Array(Array(T))) ClickHouse data type
type Array3[T any] struct {
	ArrayBase
}

// NewArray create a new array column of Array(Array(Array(T))) ClickHouse data type
func NewArray3[T any](array *Array2[T]) *Array3[T] {
	a := &Array3[T]{
		ArrayBase: ArrayBase{
			dataColumn:   array,
			offsetColumn: New[uint64](),
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

// Append value for insert
func (c *Array3[T]) Append(v ...[][][]T) {
	for _, v := range v {
		c.AppendLen(len(v))
		c.dataColumn.(*Array2[T]).Append(v...)
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
