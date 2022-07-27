package column

// TupleType is an interface to handle  tuple data types.
type TupleType[T any] interface {
	Append([]ColumnBasic)
	Get([]ColumnBasic, int) T
	Column() []ColumnBasic
}

type TupleOf[T TupleType[T]] struct {
	Tuple
}

func NewTupleOf[T TupleType[T]]() *TupleOf[T] {
	var tmpVal T
	columns := tmpVal.Column()
	if len(columns) < 1 {
		panic("tupleOf must have at least one column")
	}
	return &TupleOf[T]{
		Tuple{
			columns: columns,
		},
	}
}

// Data get all the data in current block as a slice.
func (c *TupleOf[T]) Data() []T {
	values := make([]T, c.NumRow())
	for i := 0; i < c.NumRow(); i++ {
		values[i] = values[i].Get(c.columns, i)
	}
	return values
}

// Read reads all the data in current block and append to the input.
func (c *TupleOf[T]) Read(value []T) []T {
	if cap(value)-len(value) >= c.NumRow() {
		value = (value)[:len(value)+c.NumRow()]
	} else {
		value = append(value, make([]T, c.NumRow())...)
	}
	val := (value)[len(value)-c.NumRow():]
	for i := 0; i < c.NumRow(); i++ {
		val[i] = val[i].Get(c.columns, i)
	}
	return value
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *TupleOf[T]) Row(row int) T {
	var val T
	return val.Get(c.columns, row)
}

// Append value for insert
func (c *TupleOf[T]) Append(v T) {
	v.Append(c.columns)
}

// Append slice of value for insert
func (c *TupleOf[T]) AppendSlice(v []T) {
	for _, v := range v {
		c.Append(v)
	}
}

// Array return a Array type for this column
func (c *TupleOf[T]) Array() *Array[T] {
	return NewArray[T](c)
}
