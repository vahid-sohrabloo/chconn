package column

// Array is a column of Array(T) ClickHouse data type
type Array[T any] struct {
	ArrayBase
	columnData []T
}

// NewArray create a new array column of Array(T) ClickHouse data type
func NewArray[T any](dataColumn Column[T]) *Array[T] {
	a := &Array[T]{
		ArrayBase: ArrayBase{
			dataColumn:   dataColumn,
			offsetColumn: New[uint64](),
		},
	}
	a.resetHook = func() {
		a.columnData = a.columnData[:0]
	}
	return a
}

// Data get all the data in current block as a slice.
func (c *Array[T]) Data() [][]T {
	values := make([][]T, c.offsetColumn.numRow)
	offsets := c.Offsets()
	var lastOffset uint64
	columnData := c.getColumnData()
	for i, offset := range offsets {
		val := make([]T, offset-lastOffset)
		copy(val, columnData[lastOffset:offset])
		values[i] = val
		lastOffset = offset
	}
	return values
}

// Read reads all the data in current block and append to the input.
func (c *Array[T]) Read(value [][]T) [][]T {
	offsets := c.Offsets()
	var lastOffset uint64
	columnData := c.getColumnData()
	for _, offset := range offsets {
		val := make([]T, offset-lastOffset)
		copy(val, columnData[lastOffset:offset])
		lastOffset = offset
		value = append(value, val)
	}
	return value
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Array[T]) Row(row int) []T {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	var val []T
	val = append(val, c.getColumnData()[lastOffset:c.offsetColumn.Row(row)]...)
	return val
}

// Append value for insert
func (c *Array[T]) Append(v []T) {
	c.AppendLen(len(v))
	c.dataColumn.(Column[T]).AppendSlice(v)
}

// Append single item value for insert
//
// it's should use with AppendLen
//
// Example:
//
//	c.AppendLen(2) // insert 2 items
//	c.AppendItem(1) // insert item 1
//	c.AppendItem(2) // insert item 2
func (c *Array[T]) AppendItem(v T) {
	c.dataColumn.(Column[T]).Append(v)
}

// AppendSlice append slice of value for insert
func (c *Array[T]) AppendSlice(v [][]T) {
	for _, vv := range v {
		c.Append(vv)
	}
}

// Array return a Array type for this column
func (c *Array[T]) Array() *Array2[T] {
	return NewArray2(c)
}

func (c *Array[T]) getColumnData() []T {
	if len(c.columnData) == 0 {
		c.columnData = c.dataColumn.(Column[T]).Data()
	}
	return c.columnData
}

func (c *Array[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}
