package column

import (
	"encoding/binary"
)

// Int64 use for Int64 ClickHouse DataType
type Int64 struct {
	column
	val  int64
	dict map[int64]int
	keys []int
}

// NewInt64 return new Int64 for Int64 ClickHouse DataType
func NewInt64(nullable bool) *Int64 {
	return &Int64{
		dict: make(map[int64]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Int64Size,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *Int64) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = int64(binary.LittleEndian.Uint64(c.b[c.i-c.size : c.i]))
	return true
}

// Value of current pointer
//
// Use with Next()
func (c *Int64) Value() int64 {
	return c.val
}

// ReadAll read all value in this block and append to the input slice
func (c *Int64) ReadAll(value *[]int64) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			int64(binary.LittleEndian.Uint64(c.b[i:i+c.size])))
	}
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Int64) Fill(value []int64) {
	for i := range value {
		value[i] = int64(binary.LittleEndian.Uint64(c.b[c.i : c.i+c.size]))
		c.i += c.size
	}
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Int64) ValueP() *int64 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Int64) ReadAllP(value *[]*int64) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := int64(binary.LittleEndian.Uint64(c.b[i : i+c.size]))
		*value = append(*value, &val)
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Int64) FillP(value []*int64) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := int64(binary.LittleEndian.Uint64(c.b[c.i : c.i+c.size]))
		value[i] = &val
		c.i += c.size
	}
}

// Append value for insert
func (c *Int64) Append(v int64) {
	c.numRow++
	c.writerData = append(c.writerData,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
		byte(v>>32),
		byte(v>>40),
		byte(v>>48),
		byte(v>>56),
	)
}

// AppendEmpty append empty value for insert
func (c *Int64) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *Int64) AppendP(v *int64) {
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
func (c *Int64) AppendDict(v int64) {
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
func (c *Int64) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

// AppendDictP add value to the dictionary (if doesn't exist on dictionary)
// and append key of the dictionary to keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *Int64) AppendDictP(v *int64) {
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
func (c *Int64) Keys() []int {
	return c.keys
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *Int64) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[int64]int)
}
