package column

import "github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"

// Array is a column of Array(Array(Nullable(T))) ClickHouse data type
type Array2Nullable[T comparable] struct {
	Array2[T]
	dataColumn *ArrayNullable[T]
	columnData [][]*T
}

// NewArrayNullable create a new array column of Array(Nullable(T)) ClickHouse data type
func NewArray2Nullable[T comparable](dataColumn *ArrayNullable[T]) *Array2Nullable[T] {
	a := &Array2Nullable[T]{
		dataColumn: dataColumn,
		Array2: Array2[T]{
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
func (c *Array2Nullable[T]) DataP() [][][]*T {
	values := make([][][]*T, c.offsetColumn.numRow)
	var lastOffset uint64
	columnData := c.getColumnData()
	for i := 0; i < c.offsetColumn.numRow; i++ {
		values[i] = columnData[lastOffset:c.offsetColumn.Row(i)]
		lastOffset = c.offsetColumn.Row(i)
	}
	return values
}

// Read reads all the nullable data in current block as a slice pointer and append to the input.
func (c *Array2Nullable[T]) ReadP(value *[][][]*T) {
	var lastOffset uint64
	columnData := c.getColumnData()
	for i := 0; i < c.offsetColumn.numRow; i++ {
		*value = append(*value, columnData[lastOffset:c.offsetColumn.Row(i)])
		lastOffset = c.offsetColumn.Row(i)
	}
}

// RowP return the nullable value of given row as a pointer
// NOTE: Row number start from zero
func (c *Array2Nullable[T]) RowP(row int) [][]*T {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	var val [][]*T
	val = append(val, c.getColumnData()[lastOffset:c.offsetColumn.Row(row)]...)
	return val
}

// AppendP a nullable value for insert
func (c *Array2Nullable[T]) AppendP(v [][]*T) {
	c.AppendLen(uint64(len(v)))
	c.dataColumn.AppendSliceP(v)
}

// AppendSliceP append slice of nullable value for insert
func (c *Array2Nullable[T]) AppendSliceP(v [][][]*T) {
	for _, vv := range v {
		c.AppendP(vv)
	}
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *Array2Nullable[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	err := c.Array2.ReadRaw(num, r)
	if err != nil {
		return err
	}
	c.columnData = c.dataColumn.DataP()
	return nil
}

// Array return a Array type for this column
func (c *Array2Nullable[T]) ArrayOf() *Array3Nullable[T] {
	return NewArray3Nullable[T](c)
}

func (c *Array2Nullable[T]) getColumnData() [][]*T {
	if len(c.columnData) == 0 {
		c.columnData = c.dataColumn.DataP()
	}
	return c.columnData
}

func (c *Array2Nullable[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}
