package column

// Array2 is a column of Array(Array(T)) ClickHouse data type
type Array2[T any] struct {
	ArrayBase
}

// NewArray create a new array column of Array(Array(T)) ClickHouse data type
func NewArray2[T any](array *Array[T]) *Array2[T] {
	a := &Array2[T]{
		ArrayBase: ArrayBase{
			dataColumn:   array,
			offsetColumn: New[uint64](),
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

// RowI return the value of given row.
// NOTE: Row number start from zero
func (c *Array2[T]) RowI(row int) any {
	return c.Row(row)
}

// Append value for insert
func (c *Array2[T]) Append(v ...[][]T) {
	for _, v := range v {
		c.AppendLen(len(v))
		c.dataColumn.(*Array[T]).Append(v...)
	}
}

func (c *Array2[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}
