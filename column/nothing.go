package column

import (
	"reflect"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

// Nothing represents column of nothing value.
//
// https://clickhouse.com/docs/en/sql-reference/data-types/special-data-types/nothing
type Nothing struct {
	Base[int8]
}

type NothingData struct{}

// New create a new column
func NewNothing() *Nothing {
	size := 1
	return &Nothing{
		Base: Base[int8]{
			size:   size,
			strict: true,
			kind:   reflect.TypeFor[int8]().Kind(),
			rtype:  reflect.TypeFor[int8](),
		},
	}
}

// Data get all the nullable  data in current block as a slice of pointer.
//
// NOTE: it always return slice of nil
func (c *Nothing) Data() []NothingData {
	return make([]NothingData, c.numRow)
}

// Read reads all the data in current block and append to the input.
//
// NOTE: it always append values of NothingData
func (c *Nothing) Read(value []NothingData) []NothingData {
	return append(value, make([]NothingData, c.numRow)...)
}

// Append value for insert
func (c *Nothing) Row(i int) NothingData {
	return NothingData{}
}

func (c *Nothing) Scan(row int, dest any) error {
	return nil
}

// Append value for insert
//
// Should not use this method. Nothing column is only for select query
func (c *Nothing) Append(v NothingData) {
}

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *Nothing) RowAny(row int) any {
	return nil
}

func (c *Nothing) canAppend(value any) bool {
	return false
}

func (c *Nothing) AppendAny(value any) error {
	return nil
}

// AppendMulti value for insert
//
// Should not use this method. Nothing column is only for select query
func (c *Nothing) AppendMulti(v ...NothingData) {
}

func (c *Nothing) FullType() string {
	if len(c.columnHeader.Name) == 0 {
		return helper.NothingStr
	}
	return string(c.columnHeader.Name) + " " + helper.NothingStr
}

func (c *Nothing) String(row int) string {
	return ""
}

// Array return a Array type for this column
func (c *Nothing) Array() *Array[NothingData] {
	return NewArray[NothingData](c)
}

// Nullable return a nullable type for this column
func (c *Nothing) Nullable() *NothingNullable {
	return NewNothingNullable(c)
}

func (c *Nothing) Elem(arrayLevel int, nullable bool) ColumnCore {
	if nullable {
		return c.Nullable().elem(arrayLevel)
	}
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *Nothing) chconnType() string {
	return "column.Nothing"
}

func (c *Nothing) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	chType := helper.FilterSimpleAggregate(c.columnHeader.ChType)
	if !helper.IsNothing(chType) {
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			goToChType: "Nothing",
			chconnType: c.chconnType(),
		}
	}
	return nil
}

func (c *Nothing) ValidateInsert() error {
	return nil
}

func (c *Nothing) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	return b
}

func (c *Nothing) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexNothing))
}
