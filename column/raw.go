package column

// Raw use for any fixed size ClickHouse DataType
type Raw struct {
	column
	val  []byte
	dict map[string]int
	keys []int
}

// NewRaw return new Raw any fixed size ClickHouse DataType
func NewRaw(size int, nullable bool) *Raw {
	return &Raw{
		dict: make(map[string]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        size,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *Raw) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.val = c.b[c.i : c.i+c.size]
	c.i += c.size
	return true
}

// Value of current pointer
//
// Use with Next()
func (c *Raw) Value() []byte {
	return c.val
}

// ReadAll read all value in this block and append to the input slice
func (c *Raw) ReadAll(value *[][]byte) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value, c.b[i:i+c.size])
	}
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Raw) Fill(value [][]byte) {
	for i := range value {
		value[i] = c.b[c.i : c.i+c.size]
		c.i += c.size
	}
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Raw) ValueP() *[]byte {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Raw) ReadAllP(value *[]*[]byte) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := c.b[i : i+c.size]
		*value = append(*value, &val)
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Raw) FillP(value []*[]byte) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := c.b[c.i : c.i+c.size]
		value[i] = &val
		c.i += c.size
	}
}

// Append value for insert
func (c *Raw) Append(v []byte) {
	c.numRow++
	c.writerData = append(c.writerData, v[:c.size]...)
}

// AppendEmpty append empty value for insert
func (c *Raw) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *Raw) AppendP(v *[]byte) {
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
func (c *Raw) AppendDict(v []byte) {
	key, ok := c.dict[string(v)]
	if !ok {
		key = len(c.dict)
		c.dict[string(v)] = key
		c.Append(v)
	}
	if c.nullable {
		c.keys = append(c.keys, key+1)
	} else {
		c.keys = append(c.keys, key)
	}
}

// AppendDictNil add nil key for LowCardinality nullable data type
func (c *Raw) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

// AppendDictP add value to the dictionary (if doesn't exist on dictionary)
// and append key of the dictionary to keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *Raw) AppendDictP(v *[]byte) {
	if v == nil {
		c.keys = append(c.keys, 0)
		return
	}
	key, ok := c.dict[string(*v)]
	if !ok {
		key = len(c.dict)
		c.dict[string(*v)] = key
		c.Append(*v)
	}
	c.keys = append(c.keys, key+1)
}

// Keys current keys for LowCardinality data type
func (c *Raw) Keys() []int {
	return c.keys
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *Raw) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[string]int)
}
