package column

import (
	"encoding/binary"
	"time"
)

// Date32 use for Date32 ClickHouse DataType
type Date32 struct {
	column
	dict map[time.Time]int
	keys []int
}

// NewDate32 return new Date32 for Date32 ClickHouse DataType
func NewDate32(nullable bool) *Date32 {
	return &Date32{
		dict: make(map[time.Time]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Date32Size,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *Date32) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += Date32Size
	return true
}

// ReadAll read all value in this block and append to the input slice
func (c *Date32) ReadAll(value *[]time.Time) {
	for i := 0; i < c.totalByte; i += Date32Size {
		*value = append(*value,
			time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+Date32Size])*daySeconds), 0))
	}
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Date32) ReadAllP(value *[]*time.Time) {
	for i := 0; i < c.totalByte; i += Date32Size {
		if c.colNullable.b[i/Date32Size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+Date32Size])*daySeconds), 0)
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Date32) Row(row int) time.Time {
	i := row * Date32Size
	return time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+Date32Size])*daySeconds), 0)
}

// Row return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *Date32) RowP(row int) *time.Time {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * Date32Size
	val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+Date32Size])*daySeconds), 0)
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *Date32) Value() time.Time {
	return time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i-Date32Size:c.i])*daySeconds), 0)
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Date32) ValueP() *time.Time {
	if c.colNullable.b[(c.i-Date32Size)/(Date32Size)] == 1 {
		return nil
	}
	val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i-Date32Size:c.i])*daySeconds), 0)
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Date32) Fill(value []time.Time) {
	for i := range value {
		value[i] = time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i:c.i+Date32Size])*daySeconds), 0)
		c.i += Date32Size
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Date32) FillP(value []*time.Time) {
	for i := range value {
		if c.colNullable.b[c.i/Date32Size] == 1 {
			value[i] = nil
			c.i += Date32Size
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i:c.i+Date32Size])*daySeconds), 0)
		value[i] = &val
		c.i += Date32Size
	}
}

// Append value for insert
func (c *Date32) Append(v time.Time) {
	c.numRow++
	if v.Unix() <= -1420071572 {
		c.writerData = append(c.writerData, emptyByte[:c.size]...)
		return
	}
	_, offset := v.Zone()
	timestamp := (v.Unix() + int64(offset)) / daySeconds
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
func (c *Date32) AppendP(v *time.Time) {
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
func (c *Date32) AppendDict(v time.Time) {
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
func (c *Date32) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

// AppendDictP add value to the dictionary (if doesn't exist on dictionary)
// and append key of the dictionary to keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *Date32) AppendDictP(v *time.Time) {
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
func (c *Date32) getKeys() []int {
	return c.keys
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *Date32) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[time.Time]int)
}
