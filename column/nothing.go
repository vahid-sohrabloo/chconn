package column

import (
	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
)

// ColNothing represents column of nothing value.
// Value is row count.
//
// https://clickhouse.com/docs/ru/sql-reference/data-types/special-data-types/nothing
type Nothing struct {
	Base[int8]
}

// New create a new column
func NewNothing() *Nothing {
	return &Nothing{}
}

func (c *Nothing) Scan(row int, dest any) error {
	return nil
}

// Append value for insert
//
// Should not use this method. Nothing column is only for select query
func (c *Nothing) Append(v int8) {
	c.numRow++
}

// AppendMulti value for insert
//
// Should not use this method. Nothing column is only for select query
func (c *Nothing) AppendMulti(v ...int8) {
	c.numRow += len(v)
}

func (c *Nothing) FullType() string {
	if len(c.name) == 0 {
		return helper.NothingStr
	}
	return string(c.name) + " " + helper.NothingStr
}

func (c *Nothing) String(row int) string {
	return ""
}

// Array return a Array type for this column
func (c *Nothing) Array() *Array[int8] {
	return NewArray[int8](c)
}

// Nullable return a nullable type for this column
func (c *Nothing) Nullable() *NothingNullable {
	return NewNothingNullable(c)
}

// LC return a low cardinality type for this column
func (c *Nothing) LC() *LowCardinality[int8] {
	return NewLC[int8](c)
}

// LowCardinality return a low cardinality type for this column
func (c *Nothing) LowCardinality() *LowCardinality[int8] {
	return NewLowCardinality[int8](c)
}

func (c *Nothing) Elem(arrayLevel int, nullable, lc bool) ColumnBasic {
	if lc {
		return c.LowCardinality().elem(arrayLevel, nullable)
	}
	if nullable {
		return c.Nullable().elem(arrayLevel)
	}
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *Nothing) Validate() error {
	chType := helper.FilterSimpleAggregate(c.chType)
	if !helper.IsNothing(chType) {
		return ErrInvalidType{
			column: c,
		}
	}
	return nil
}
