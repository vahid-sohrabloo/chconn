package column

import (
	"encoding/binary"
	"time"
)

// DateTime use for DateTime ClickHouse DataType
type DateTime struct {
	column
	dict map[time.Time]int
	keys []int
}

// NewDateTime return new DateTime for DateTime ClickHouse DataType
func NewDateTime(nullable bool) *DateTime {
	return &DateTime{
		dict: make(map[time.Time]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        DatetimeSize,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *DateTime) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += DatetimeSize
	return true
}

// ReadAll read all value in this block and append to the input slice
func (c *DateTime) ReadAll(value *[]time.Time) {
	for i := 0; i < c.totalByte; i += DatetimeSize {
		*value = append(*value,
			time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+DatetimeSize])), 0))
	}
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *DateTime) ReadAllP(value *[]*time.Time) {
	for i := 0; i < c.totalByte; i += DatetimeSize {
		if c.colNullable.b[i/DatetimeSize] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+DatetimeSize])), 0)
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *DateTime) Row(row int) time.Time {
	i := row * DatetimeSize
	return time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+DatetimeSize])), 0)
}

// Row return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *DateTime) RowP(row int) *time.Time {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * DatetimeSize
	val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+DatetimeSize])), 0)
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *DateTime) Value() time.Time {
	return time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i-DatetimeSize:c.i])), 0)
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *DateTime) ValueP() *time.Time {
	if c.colNullable.b[(c.i-DatetimeSize)/(DatetimeSize)] == 1 {
		return nil
	}
	val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i-DatetimeSize:c.i])), 0)
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *DateTime) Fill(value []time.Time) {
	for i := range value {
		value[i] = time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i:c.i+DatetimeSize])), 0)
		c.i += DatetimeSize
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *DateTime) FillP(value []*time.Time) {
	for i := range value {
		if c.colNullable.b[c.i/DatetimeSize] == 1 {
			value[i] = nil
			c.i += DatetimeSize
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i:c.i+DatetimeSize])), 0)
		value[i] = &val
		c.i += DatetimeSize
	}
}

// Append value for insert
func (c *DateTime) Append(v time.Time) {
	c.numRow++
	if v.Unix() <= 0 {
		c.writerData = append(c.writerData, emptyByte[:c.size]...)
		return
	}
	timestamp := v.Unix()
	c.writerData = append(c.writerData,
		byte(timestamp),
		byte(timestamp>>8),
		byte(timestamp>>16),
		byte(timestamp>>24),
	)
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *DateTime) AppendP(v *time.Time) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

// AppendDict add value to the dictionary (if doesn't exist on dictionary) and append key of the dictionary to keys
//
// Only use for LowCardinality data type
func (c *DateTime) AppendDict(v time.Time) {
	key, ok := c.dict[v]
	if !ok {
		key = len(c.dict)
		c.dict[v] = key
		c.Append(v)
	}
	if c.nullable {
		c.keys = append(c.keys, key+1)
	} else {
		c.keys = append(c.keys, key)
	}
}

// AppendDictNil add nil key for LowCardinality nullable data type
func (c *DateTime) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

// AppendDictP add value to the dictionary (if doesn't exist on dictionary)
// and append key of the dictionary to keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *DateTime) AppendDictP(v *time.Time) {
	if v == nil {
		c.keys = append(c.keys, 0)
		return
	}
	key, ok := c.dict[*v]
	if !ok {
		key = len(c.dict)
		c.dict[*v] = key
		c.Append(*v)
	}
	c.keys = append(c.keys, key+1)
}

// Keys current keys for LowCardinality data type
func (c *DateTime) getKeys() []int {
	return c.keys
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *DateTime) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[time.Time]int)
}
