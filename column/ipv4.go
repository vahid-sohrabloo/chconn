package column

import "net"

// IPv4 use for IPv4 ClickHouse DataType
type IPv4 struct {
	column
	val  net.IP
	dict map[string]int
	keys []int
}

// NewIPv4 return new IPv4 for IPv4 ClickHouse DataType
func NewIPv4(nullable bool) *IPv4 {
	return &IPv4{
		dict: make(map[string]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        IPv4Size,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *IPv4) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	b := c.b[c.i-c.size : c.i]
	c.val = net.IPv4(b[3], b[2], b[1], b[0]).To4()
	return true
}

// Value of current pointer
//
// Use with Next()
func (c *IPv4) Value() net.IP {
	return c.val
}

// ReadAll read all value in this block and append to the input slice
func (c *IPv4) ReadAll(value *[]net.IP) {
	for i := 0; i < c.totalByte; i += c.size {
		b := c.b[i : i+c.size]
		*value = append(*value,
			net.IPv4(b[3], b[2], b[1], b[0]).To4())
	}
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *IPv4) Fill(value []net.IP) {
	for i := range value {
		b := c.b[c.i : c.i+c.size]
		value[i] = net.IPv4(b[3], b[2], b[1], b[0]).To4()
		c.i += c.size
	}
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *IPv4) ValueP() *net.IP {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *IPv4) ReadAllP(value *[]*net.IP) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		b := c.b[i : i+c.size]
		val := net.IPv4(b[3], b[2], b[1], b[0]).To4()
		*value = append(*value, &val)
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *IPv4) FillP(value []*net.IP) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		b := c.b[c.i : c.i+c.size]
		val := net.IPv4(b[3], b[2], b[1], b[0]).To4()
		value[i] = &val
		c.i += c.size
	}
}

// Append value for insert
func (c *IPv4) Append(v net.IP) {
	c.numRow++
	c.writerData = append(c.writerData, v[3], v[2], v[1], v[0])
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *IPv4) AppendP(v *net.IP) {
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
func (c *IPv4) AppendDict(v net.IP) {
	key, ok := c.dict[string(v.To4())]
	if !ok {
		key = len(c.dict)
		c.dict[string(v.To4())] = key
		c.Append(v)
	}
	if c.nullable {
		c.keys = append(c.keys, key+1)
	} else {
		c.keys = append(c.keys, key)
	}
}

// AppendDictNil add nil key for LowCardinality nullable data type
func (c *IPv4) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

// AppendDictP add value to the dictionary (if doesn't exist on dictionary)
// and append key of the dictionary to keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *IPv4) AppendDictP(v *net.IP) {
	if v == nil {
		c.keys = append(c.keys, 0)
		return
	}
	key, ok := c.dict[string(v.To4())]
	if !ok {
		key = len(c.dict)
		c.dict[string(v.To4())] = key
		c.Append(*v)
	}
	c.keys = append(c.keys, key+1)
}

// Keys current keys for LowCardinality data type
func (c *IPv4) getKeys() []int {
	return c.keys
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *IPv4) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[string]int)
}
