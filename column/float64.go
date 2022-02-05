package column

import "math"

// Float64 use for Float64 ClickHouse DataType
type Float64 struct {
	column
	dict map[float64]int
	keys []int
}

// NewFloat64 return new Float64 for Float64 ClickHouse DataType
func NewFloat64(nullable bool) *Float64 {
	return &Float64{
		dict: make(map[float64]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Float64Size,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *Float64) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += Float64Size
	return true
}

// Append value for insert
func (c *Float64) Append(v float64) {
	c.numRow++
	castVal := math.Float64bits(v)
	c.writerData = append(c.writerData,
		byte(castVal),
		byte(castVal>>8),
		byte(castVal>>16),
		byte(castVal>>24),
		byte(castVal>>32),
		byte(castVal>>40),
		byte(castVal>>48),
		byte(castVal>>56),
	)
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *Float64) AppendP(v *float64) {
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
func (c *Float64) AppendDict(v float64) {
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
func (c *Float64) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

// AppendDictP add value to the dictionary (if doesn't exist on dictionary)
// and append key of the dictionary to keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *Float64) AppendDictP(v *float64) {
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
func (c *Float64) getKeys() []int {
	return c.keys
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *Float64) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[float64]int)
}
