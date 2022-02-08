package column

import (
	"encoding/binary"
	"math"
	"time"
)

// DateTime64 use for DateTime ClickHouse DateTime64
type DateTime64 struct {
	column
	precision int64
	loc       *time.Location
}

// NewDateTime64 return new DateTime for DateTime ClickHouse DataType
func NewDateTime64(precision int, nullable bool) *DateTime64 {
	return &DateTime64{
		precision: int64(math.Pow10(9 - precision)),
		loc:       time.Local,
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Datetime64Size,
		},
	}
}

// SetLocation set location for time
func (c *DateTime64) SetLocation(loc *time.Location) {
	c.loc = loc
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *DateTime64) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += Datetime64Size
	return true
}

// Value of current pointer
//
// Use with Next()
func (c *DateTime64) Value() time.Time {
	return c.toDate(int64(binary.LittleEndian.Uint64(c.b[c.i-Datetime64Size : c.i])))
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *DateTime64) ValueP() *time.Time {
	if c.colNullable.b[(c.i-Datetime64Size)/(Datetime64Size)] == 1 {
		return nil
	}
	val := c.toDate(int64(binary.LittleEndian.Uint64(c.b[c.i-Datetime64Size : c.i])))
	return &val
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *DateTime64) Row(row int) time.Time {
	i := row * DatetimeSize
	return c.toDate(int64(binary.LittleEndian.Uint64(c.b[i : i+Datetime64Size])))
}

// ReadAll read all value in this block and append to the input slice
func (c *DateTime64) ReadAll(value *[]time.Time) {
	for i := 0; i < c.totalByte; i += Datetime64Size {
		*value = append(*value,
			c.toDate(int64(binary.LittleEndian.Uint64(c.b[i:i+Datetime64Size]))))
	}
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *DateTime64) Fill(value []time.Time) {
	for i := range value {
		value[i] = c.toDate(int64(binary.LittleEndian.Uint64(c.b[c.i : c.i+Datetime64Size])))
		c.i += 8
	}
}

func (c *DateTime64) toDate(usec int64) time.Time {
	nano := usec * c.precision
	sec := nano / int64(10e8)
	nsec := nano - sec*10e8
	return time.Unix(sec, nsec).In(c.loc)
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *DateTime64) ReadAllP(value *[]*time.Time) {
	for i := 0; i < c.totalByte; i += Datetime64Size {
		if c.colNullable.b[i/Datetime64Size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := c.toDate(int64(binary.LittleEndian.Uint64(c.b[i : i+Datetime64Size])))
		*value = append(*value, &val)
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *DateTime64) FillP(value []*time.Time) {
	for i := range value {
		if c.colNullable.b[c.i/Datetime64Size] == 1 {
			value[i] = nil
			c.i += Datetime64Size
			continue
		}
		val := c.toDate(int64(binary.LittleEndian.Uint64(c.b[c.i : c.i+Datetime64Size])))
		value[i] = &val
		c.i += Datetime64Size
	}
}

// Append value for insert
func (c *DateTime64) Append(v time.Time) {
	c.numRow++
	if v.Unix() < 0 {
		c.writerData = append(c.writerData, emptyByte[:Datetime64Size]...)
		return
	}
	timestamp := v.UnixNano() / c.precision
	c.writerData = append(c.writerData,
		byte(timestamp),
		byte(timestamp>>8),
		byte(timestamp>>16),
		byte(timestamp>>24),
		byte(timestamp>>32),
		byte(timestamp>>40),
		byte(timestamp>>48),
		byte(timestamp>>56),
	)
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *DateTime64) AppendP(v *time.Time) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *DateTime64) Reset() {
	c.column.Reset()
}
