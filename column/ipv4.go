package column

import "net"

// IPv4 use for IPv4 ClickHouse DataType
type IPv4 struct {
	column
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
	c.i += IPv4Size
	return true
}

// ReadAll read all value in this block and append to the input slice
func (c *IPv4) ReadAll(value *[]net.IP) {
	for i := 0; i < c.totalByte; i += IPv4Size {
		b := c.b[i : i+IPv4Size]
		*value = append(*value,
			net.IPv4(b[3], b[2], b[1], b[0]).To4())
	}
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *IPv4) ReadAllP(value *[]*net.IP) {
	for i := 0; i < c.totalByte; i += IPv4Size {
		if c.colNullable.b[i/IPv4Size] != 0 {
			*value = append(*value, nil)
			continue
		}
		b := c.b[i : i+IPv4Size]
		val := net.IPv4(b[3], b[2], b[1], b[0]).To4()
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *IPv4) Row(row int) net.IP {
	i := row * IPv4Size
	b := c.b[i : i+IPv4Size]
	return net.IPv4(b[3], b[2], b[1], b[0]).To4()
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *IPv4) RowP(row int) *net.IP {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * IPv4Size
	b := c.b[i : i+IPv4Size]
	val := net.IPv4(b[3], b[2], b[1], b[0]).To4()
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *IPv4) Value() net.IP {
	b := c.b[c.i-IPv4Size : c.i]
	return net.IPv4(b[3], b[2], b[1], b[0]).To4()
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *IPv4) ValueP() *net.IP {
	if c.colNullable.b[(c.i-IPv4Size)/(IPv4Size)] == 1 {
		return nil
	}
	b := c.b[c.i-IPv4Size : c.i]
	val := net.IPv4(b[3], b[2], b[1], b[0]).To4()
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *IPv4) Fill(value []net.IP) {
	for i := range value {
		b := c.b[c.i : c.i+IPv4Size]
		value[i] = net.IPv4(b[3], b[2], b[1], b[0]).To4()
		c.i += IPv4Size
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *IPv4) FillP(value []*net.IP) {
	for i := range value {
		if c.colNullable.b[c.i/IPv4Size] == 1 {
			value[i] = nil
			c.i += IPv4Size
			continue
		}
		b := c.b[c.i : c.i+IPv4Size]
		val := net.IPv4(b[3], b[2], b[1], b[0]).To4()
		value[i] = &val
		c.i += IPv4Size
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
