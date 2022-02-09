package column

import (
	"encoding/binary"
	"time"
)

// Date use for Date ClickHouse DataType
type Date struct {
	column
	dict map[time.Time]int
	keys []int
}

// NewDate return new Date for Date ClickHouse DataType
func NewDate(nullable bool) *Date {
	return &Date{
		dict: make(map[time.Time]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        DateSize,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *Date) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += DateSize
	return true
}

// ReadAll read all value in this block and append to the input slice
func (c *Date) ReadAll(value *[]time.Time) {
	for i := 0; i < c.totalByte; i += DateSize {
		*value = append(*value,
			time.Unix(int64(binary.LittleEndian.Uint16(c.b[i:i+DateSize]))*daySeconds, 0).In(time.UTC))
	}
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Date) ReadAllP(value *[]*time.Time) {
	for i := 0; i < c.totalByte; i += DateSize {
		if c.colNullable.b[i/DateSize] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint16(c.b[i:i+DateSize]))*daySeconds, 0).In(time.UTC)
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Date) Row(row int) time.Time {
	i := row * DateSize
	return time.Unix(int64(binary.LittleEndian.Uint16(c.b[i:i+DateSize]))*daySeconds, 0).In(time.UTC)
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *Date) RowP(row int) *time.Time {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * DateSize
	val := time.Unix(int64(binary.LittleEndian.Uint16(c.b[i:i+DateSize]))*daySeconds, 0).In(time.UTC)
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *Date) Value() time.Time {
	return time.Unix(int64(binary.LittleEndian.Uint16(c.b[c.i-DateSize:c.i]))*daySeconds, 0).In(time.UTC)
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Date) ValueP() *time.Time {
	if c.colNullable.b[(c.i-DateSize)/(DateSize)] == 1 {
		return nil
	}
	val := time.Unix(int64(binary.LittleEndian.Uint16(c.b[c.i-DateSize:c.i]))*daySeconds, 0).In(time.UTC)
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Date) Fill(value []time.Time) {
	for i := range value {
		value[i] = time.Unix(int64(binary.LittleEndian.Uint16(c.b[c.i:c.i+DateSize]))*daySeconds, 0).In(time.UTC)
		c.i += DateSize
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Date) FillP(value []*time.Time) {
	for i := range value {
		if c.colNullable.b[c.i/DateSize] == 1 {
			value[i] = nil
			c.i += DateSize
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint16(c.b[c.i:c.i+DateSize]))*daySeconds, 0).In(time.UTC)
		value[i] = &val
		c.i += DateSize
	}
}

// Append value for insert
func (c *Date) Append(v time.Time) {
	c.numRow++
	if v.Unix() <= 0 {
		c.writerData = append(c.writerData, emptyByte[:c.size]...)
		return
	}
	_, offset := v.Zone()
	timestamp := (v.Unix() + int64(offset)) / daySeconds
	c.writerData = append(c.writerData,
		byte(timestamp),
		byte(timestamp>>8),
	)
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *Date) AppendP(v *time.Time) {
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
func (c *Date) AppendDict(v time.Time) {
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
func (c *Date) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

// AppendDictP add value to the dictionary (if doesn't exist on dictionary)
// and append key of the dictionary to keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *Date) AppendDictP(v *time.Time) {
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
func (c *Date) getKeys() []int {
	return c.keys
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *Date) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[time.Time]int)
}
