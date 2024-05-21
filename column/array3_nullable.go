package column

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

// Array is a column of Array(Array(Nullable(T))) ClickHouse data type
type Array3Nullable[T any] struct {
	Array3[T]
	dataColumn *Array2Nullable[T]
	columnData [][][]*T
}

// NewArrayNullable create a new array column of Array(Nullable(T)) ClickHouse data type
func NewArray3Nullable[T any](dataColumn *Array2Nullable[T]) *Array3Nullable[T] {
	a := &Array3Nullable[T]{
		dataColumn: dataColumn,
		Array3: Array3[T]{
			rtype: reflect.TypeOf((*T)(nil)).Elem(),
			ArrayBase: ArrayBase{
				dataColumn:      dataColumn,
				offsetColumn:    New[uint64](),
				arrayChconnType: "column.Array3Nullable[" + reflect.TypeOf((*T)(nil)).Elem().String() + "]",
			},
		},
	}
	a.resetHook = func() {
		a.columnData = a.columnData[:0]
	}
	return a
}

// Data get all the nullable data in current block as a slice of pointer.
func (c *Array3Nullable[T]) DataP() [][][][]*T {
	values := make([][][][]*T, c.offsetColumn.numRow)
	var lastOffset uint64
	columnData := c.getColumnData()
	for i := 0; i < c.offsetColumn.numRow; i++ {
		values[i] = columnData[lastOffset:c.offsetColumn.Row(i)]
		lastOffset = c.offsetColumn.Row(i)
	}
	return values
}

// Read reads all the nullable data in current block as a slice pointer and append to the input.
func (c *Array3Nullable[T]) ReadP(value [][][][]*T) [][][][]*T {
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
func (c *Array3Nullable[T]) RowP(row int) [][][]*T {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	var val [][][]*T
	val = append(val, c.getColumnData()[lastOffset:c.offsetColumn.Row(row)]...)
	return val
}

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *Array3Nullable[T]) RowAny(row int) any {
	return c.RowP(row)
}

//nolint:dupl
func (c *Array3Nullable[T]) Scan(row int, dest any) error {
	switch v := dest.(type) {
	case *[][][]*T:
		*v = c.RowP(row)
		return nil
	case *any:
		*v = c.RowP(row)
		return nil
	case sql.Scanner:
		return v.Scan(c.RowP(row))
	}

	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "*[][][]*" + c.rtype.String(),
	}
}

// AppendP a nullable value for insert
func (c *Array3Nullable[T]) AppendP(v [][][]*T) {
	c.AppendLen(len(v))
	c.dataColumn.AppendMultiP(v...)
}

// AppendMultiP a nullable value for insert
func (c *Array3Nullable[T]) AppendMultiP(v ...[][][]*T) {
	for _, v := range v {
		c.AppendLen(len(v))
		c.dataColumn.AppendMultiP(v...)
	}
}

func (c *Array3Nullable[T]) canAppend(value any) bool {
	switch value.(type) {
	case [][][]*T:
		return true
	case [][][]T:
		return true
	}
	return false
}

func (c *Array3Nullable[T]) AppendAny(value any) error {
	switch v := value.(type) {
	case [][][]T:
		c.Append(v)
		return nil
	case [][][]*T:
		c.AppendP(v)
		return nil
	}
	return fmt.Errorf("AppendAny error: expected *[][][]%[1]s or [][][]%[1]s, got %[2]T", c.rtype.String(), value)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *Array3Nullable[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	err := c.Array3.ReadRaw(num, r)
	if err != nil {
		return err
	}
	c.columnData = c.dataColumn.DataP()
	return nil
}

func (c *Array3Nullable[T]) getColumnData() [][][]*T {
	if len(c.columnData) == 0 {
		c.columnData = c.dataColumn.DataP()
	}
	return c.columnData
}

func (c *Array3Nullable[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		panic("array level is too deep")
	}
	return c
}

func (c *Array3Nullable[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	b = append(b, '[')

	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	offset := c.offsetColumn.Row(row)
	for i := lastOffset; i < offset; i++ {
		if i != lastOffset {
			b = append(b, ',')
		}
		b = c.dataColumn.ToJSON(int(i), ignoreDoubleQuotes, b)
	}
	return append(b, ']')
}
