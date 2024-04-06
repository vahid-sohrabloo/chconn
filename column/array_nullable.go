package column

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

type arrayAlias[T any] struct {
	Array[T]
}

// Array is a column of Array(Nullable(T)) ClickHouse data type
type ArrayNullable[T any] struct {
	arrayAlias[T]
	dataColumn NullableColumn[T]
	columnData []*T
}

// NewArrayNullable create a new array column of Array(Nullable(T)) ClickHouse data type
func NewArrayNullable[T any](dataColumn NullableColumn[T]) *ArrayNullable[T] {
	rtype := reflect.TypeOf((*T)(nil)).Elem()

	a := &ArrayNullable[T]{
		dataColumn: dataColumn,
		arrayAlias: arrayAlias[T]{
			Array: Array[T]{
				rtype: rtype,
				ArrayBase: ArrayBase{
					dataColumn:      dataColumn,
					offsetColumn:    New[uint64](),
					arrayChconnType: "column.ArrayNullable[" + rtype.String() + "]",
				},
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

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *ArrayNullable[T]) RowAny(row int) any {
	return c.RowP(row)
}

//nolint:dupl
func (c *ArrayNullable[T]) Scan(row int, dest any) error {
	switch d := dest.(type) {
	case *[]*T:
		*d = c.RowP(row)
		return nil
	case *any:
		*d = c.RowP(row)
		return nil
	case sql.Scanner:
		return d.Scan(c.RowP(row))
	}

	if c.rtype.String() == "column.NothingData" {
		return nil
	}

	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: c.rtype.String(),
	}
}

// AppendP a nullable value for insert
func (c *ArrayNullable[T]) AppendP(v []*T) {
	c.AppendLen(len(v))
	c.dataColumn.AppendMultiP(v...)
}

// AppendMultiP a nullable value for insert
func (c *ArrayNullable[T]) AppendMultiP(v [][]*T) {
	for _, v := range v {
		c.AppendLen(len(v))
		c.dataColumn.AppendMultiP(v...)
	}
}

func (c *ArrayNullable[T]) canAppend(value any) bool {
	switch value.(type) {
	case []T:
		return true
	case []*T:
		return true
	}
	return false
}

func (c *ArrayNullable[T]) AppendAny(value any) error {
	switch v := value.(type) {
	case []T:
		c.Append(v)
		return nil
	case []*T:
		c.AppendP(v)
		return nil
	}
	return fmt.Errorf("AppendAny error: expected *[]%[1]s or []%[1]s, got %[2]T", c.rtype.String(), value)
}

//	AppendItemP Append nullable item value for insert
//
// it should use with AppendLen
//
// Example:
//
//	c.AppendLen(2) // insert 2 items
//	c.AppendItemP(val1) // insert item 1
//	c.AppendItemP(val2) // insert item 2
func (c *ArrayNullable[T]) AppendItemP(v *T) {
	c.dataColumn.AppendP(v)
}

// ArrayOf return a Array type for this column
func (c *ArrayNullable[T]) Array() *Array2Nullable[T] {
	return NewArray2Nullable(c)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *ArrayNullable[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	err := c.arrayAlias.Array.ReadRaw(num, r)
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
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *ArrayNullable[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
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
		if c.dataColumn.RowIsNil(int(i)) {
			b = append(b, "null"...)
		} else {
			b = c.dataColumn.ToJSON(int(i), ignoreDoubleQuotes, b)
		}
	}
	return append(b, ']')
}
