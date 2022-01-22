package column

import (
	"encoding/binary"
)

// Int32 use for Int32 ClickHouse DataType
type Int32 struct {
	column
	val  int32
	dict map[int32]int
	keys []int
}

// NewInt32 return new Int32 for Int32 ClickHouse DataType
func NewInt32(nullable bool) *Int32 {
	return &Int32{
		dict: make(map[int32]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Int32Size,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *Int32) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = int32(binary.LittleEndian.Uint32(c.b[c.i-c.size : c.i]))
	return true
}

// Value of current pointer
//
// Use with Next()
func (c *Int32) Value() int32 {
	return c.val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Int32) Fill(value []int32) {
	for i := range value {
		value[i] = int32(binary.LittleEndian.Uint32(c.b[c.i : c.i+c.size]))
		c.i += c.size
	}
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Int32) ValueP() *int32 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Int32) ReadAllP(value *[]*int32) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := int32(binary.LittleEndian.Uint32(c.b[i : i+c.size]))
		*value = append(*value, &val)
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Int32) FillP(value []*int32) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := int32(binary.LittleEndian.Uint32(c.b[c.i : c.i+c.size]))
		value[i] = &val
		c.i += c.size
	}
}

// Append value for insert
func (c *Int32) Append(v int32) {
	c.numRow++
	c.writerData = append(c.writerData,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
	)
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *Int32) AppendP(v *int32) {
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
func (c *Int32) AppendDict(v int32) {
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
func (c *Int32) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

// AppendDictP add value to the dictionary (if doesn't exist on dictionary)
// and append key of the dictionary to keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *Int32) AppendDictP(v *int32) {
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
func (c *Int32) getKeys() []int {
	return c.keys
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *Int32) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[int32]int)
}
