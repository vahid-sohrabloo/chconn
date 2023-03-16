package column

// Nothing use for Nothing ClickHouse DataType
type Nothing struct {
	column
	val  int8
	dict map[int8]int
	keys []int
}

// NewNothing return new Nothing for Nothing ClickHouse DataType
func NewNothing(nullable bool) *Nothing {
	return &Nothing{
		dict: make(map[int8]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Int8Size,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *Nothing) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.val = 0
	c.i += c.size
	return true
}

// Value of current pointer
//
// Use with Next()
func (c *Nothing) Value() int8 {
	return c.val
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Nothing) Row(row int) int8 {
	return 0
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
func (c *Nothing) RowP(row int) *int8 {
	return nil
}

// ReadAll read all value in this block and append to the input slice
func (c *Nothing) ReadAll(value *[]int8) {
	for range c.b {
		*value = append(*value, 0)
	}
}

func (c *Nothing) RowIsNil(i int) bool {
	return c.nullable
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Nothing) Fill(value []int8) {
	for i := range value {
		value[i] = 0
		c.i += c.size
	}
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Nothing) ValueP() *int8 {
	if c.colNullable.b[c.i-c.size] == 1 {
		return nil
	}
	val := c.val
	return &val
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Nothing) ReadAllP(value *[]*int8) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value, nil)
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Nothing) FillP(value []*int8) {
	for i := range value {
		value[i] = nil
		c.i += c.size
	}
}

// Append value for insert
func (c *Nothing) Append(v int8) {
	c.numRow++
	c.writerData = append(c.writerData,
		byte(v),
	)
}

// AppendEmpty append empty value for insert
func (c *Nothing) AppendEmpty() {
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
func (c *Nothing) AppendP(v *int8) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

// AppendDict add value to dictionary and append keys
//
// Only use for LowCardinality data type
func (c *Nothing) AppendDict(v int8) {
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
func (c *Nothing) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

// AppendDictP add value to dictionary and append keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *Nothing) AppendDictP(v *int8) {
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
func (c *Nothing) getKeys() []int {
	return c.keys
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *Nothing) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[int8]int)
}
