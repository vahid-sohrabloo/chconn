package column

import "github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"

// Array is a column of Array(Nullable(T)) ClickHouse data type
type ArrayNullable[T comparable] struct {
	Array[T]
	dataColumn NullableColumn[T]
	columnData []*T
}

// NewArrayNullable create a new array column of Array(Nullable(T)) ClickHouse data type
func NewArrayNullable[T comparable](dataColumn NullableColumn[T]) *ArrayNullable[T] {
	a := &ArrayNullable[T]{
		dataColumn: dataColumn,
		Array: Array[T]{
			ArrayBase: ArrayBase{
				dataColumn:   dataColumn,
				offsetColumn: New[uint64](),
			},
		},
	}
	a.resetHook = func() {
		a.columnData = a.columnData[:0]
	}
	return a
}

// Data get all the nullable data in current block as a slice of pointer.
func (c *ArrayNullable[T]) DataP() [][]*T {
	values := make([][]*T, c.offsetColumn.numRow)
	var lastOffset uint64
	columnData := c.getColumnData()
	for i := 0; i < c.offsetColumn.numRow; i++ {
		values[i] = columnData[lastOffset:c.offsetColumn.Row(i)]
		lastOffset = c.offsetColumn.Row(i)
	}
	return values
}

// Read reads all the nullable data in current block as a slice pointer and append to the input.
func (c *ArrayNullable[T]) ReadP(value [][]*T) [][]*T {
	var lastOffset uint64
	columnData := c.getColumnData()
	for i := 0; i < c.offsetColumn.numRow; i++ {
		value = append(value, columnData[lastOffset:c.offsetColumn.Row(i)])
		lastOffset = c.offsetColumn.Row(i)
	}
	return value
}

// RowP return the nullable value of given row as a pointer
// NOTE: Row number start from zero
func (c *ArrayNullable[T]) RowP(row int) []*T {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	var val []*T
	val = append(val, c.getColumnData()[lastOffset:c.offsetColumn.Row(row)]...)
	return val
}

// AppendP a nullable value for insert
func (c *ArrayNullable[T]) AppendP(v []*T) {
	c.AppendLen(len(v))
	c.dataColumn.AppendSliceP(v)
}

//	AppendItemP Append single nullable item value for insert
//
// it's should use with AppendLen
//
// Example:
//
//	c.AppendLen(2) // insert 2 items
//	c.AppendItemP(val1) // insert item 1
//	c.AppendItemP(val2) // insert item 2
func (c *ArrayNullable[T]) AppendItemP(v *T) {
	c.dataColumn.AppendP(v)
}

// AppendSliceP append slice of nullable value for insert
func (c *ArrayNullable[T]) AppendSliceP(v [][]*T) {
	for _, vv := range v {
		c.AppendP(vv)
	}
}

// ArrayOf return a Array type for this column
func (c *ArrayNullable[T]) ArrayOf() *Array2Nullable[T] {
	return NewArray2Nullable(c)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *ArrayNullable[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	err := c.Array.ReadRaw(num, r)
	if err != nil {
		return err
	}
	c.columnData = c.dataColumn.DataP()
	return nil
}

func (c *ArrayNullable[T]) getColumnData() []*T {
	if len(c.columnData) == 0 {
		c.columnData = c.dataColumn.DataP()
	}
	return c.columnData
}

func (c *ArrayNullable[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.ArrayOf().elem(arrayLevel - 1)
	}
	return c
}
