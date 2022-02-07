package column

// Uint8 use for UInt8 ClickHouse DataType
type Uint8 struct {
	column
	val  uint8
	dict map[uint8]int
	keys []int
}

// NewUint8 return new Uint8 for UInt8 ClickHouse DataType
func NewUint8(nullable bool) *Uint8 {
	return &Uint8{
		dict: make(map[uint8]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Uint8Size,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *Uint8) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.val = c.b[c.i]
	c.i += c.size
	return true
}

// Value of current pointer
//
// Use with Next()
func (c *Uint8) Value() uint8 {
	return c.val
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Uint8) Row(row int) uint8 {
	return c.b[row]
}

// Row[ return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *Uint8) RowP(row int) *uint8 {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	return &c.b[row]
}

// ReadAll read all value in this block and append to the input slice
func (c *Uint8) ReadAll(value *[]uint8) {
	*value = append(*value, c.b...)
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Uint8) Fill(value []uint8) {
	copy(value, c.b[c.i:len(value)+c.i])
	c.i += len(value)
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Uint8) ValueP() *uint8 {
	if c.colNullable.b[c.i-c.size] == 1 {
		return nil
	}
	val := c.val
	return &val
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Uint8) ReadAllP(value *[]*uint8) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := c.b[i]
		*value = append(*value, &val)
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Uint8) FillP(value []*uint8) {
	for i := range value {
		if c.colNullable.b[c.i] == 1 {
			c.i += c.size
			value[i] = nil
			continue
		}
		value[i] = &c.b[c.i]
		c.i += c.size
	}
}

// Append value for insert
func (c *Uint8) Append(v uint8) {
	c.numRow++
	c.writerData = append(c.writerData,
		v,
	)
}

// AppendEmpty append empty value for insert
func (c *Uint8) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData,
		0,
	)
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *Uint8) AppendP(v *uint8) {
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
func (c *Uint8) AppendDict(v uint8) {
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
func (c *Uint8) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

// AppendDictP add value to dictionary and append keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *Uint8) AppendDictP(v *uint8) {
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
func (c *Uint8) getKeys() []int {
	return c.keys
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *Uint8) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[uint8]int)
}
