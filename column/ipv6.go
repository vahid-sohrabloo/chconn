package column

import "net"

type IPv6 struct {
	column
	val  net.IP
	dict map[string]int
	keys []int
}

func NewIPv6(nullable bool) *IPv6 {
	return &IPv6{
		dict: make(map[string]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        IPv6Size,
		},
	}
}

func (c *IPv6) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = net.IP(c.b[c.i-c.size : c.i])
	return true
}

func (c *IPv6) Value() net.IP {
	return c.val
}

func (c *IPv6) ReadAll(value *[]net.IP) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			net.IP(c.b[i:i+c.size]))
	}
}

func (c *IPv6) Fill(value []net.IP) {
	for i := range value {
		value[i] = net.IP(c.b[c.i : c.i+c.size])
		c.i += c.size
	}
}

func (c *IPv6) ValueP() *net.IP {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *IPv6) ReadAllP(value *[]*net.IP) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := net.IP(c.b[i : i+c.size])
		*value = append(*value, &val)
	}
}

func (c *IPv6) FillP(value []*net.IP) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := net.IP(c.b[c.i : c.i+c.size])
		value[i] = &val
		c.i += c.size
	}
}

func (c *IPv6) Append(v net.IP) {
	c.numRow++
	c.writerData = append(c.writerData, v[:16]...)
}

func (c *IPv6) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *IPv6) AppendP(v *net.IP) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *IPv6) AppendDict(v net.IP) {
	key, ok := c.dict[string(v.To16())]
	if !ok {
		key = len(c.dict)
		c.dict[string(v.To16())] = key
		c.Append(v)
	}
	if c.nullable {
		c.keys = append(c.keys, key+1)
	} else {
		c.keys = append(c.keys, key)
	}
}

func (c *IPv6) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *IPv6) AppendDictP(v *net.IP) {
	if v == nil {
		c.keys = append(c.keys, 0)
		return
	}
	key, ok := c.dict[string(v.To16())]
	if !ok {
		key = len(c.dict)
		c.dict[string(v.To16())] = key
		c.Append(*v)
	}
	c.keys = append(c.keys, key+1)
}

func (c *IPv6) Keys() []int {
	return c.keys
}

func (c *IPv6) resetDict() {
	c.keys = c.keys[:0]
	c.dict = make(map[string]int)
}
