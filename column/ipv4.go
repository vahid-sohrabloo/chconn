package column

import "net"

type IPv4 struct {
	column
	val  net.IP
	dict map[string]int
	keys []int
}

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

func (c *IPv4) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	b := c.b[c.i-c.size : c.i]
	c.val = net.IPv4(b[3], b[2], b[1], b[0]).To4()
	return true
}

func (c *IPv4) Value() net.IP {
	return c.val
}

func (c *IPv4) ReadAll(value *[]net.IP) {
	for i := 0; i < c.totalByte; i += c.size {
		b := c.b[i : i+c.size]
		*value = append(*value,
			net.IPv4(b[3], b[2], b[1], b[0]).To4())
	}
}

func (c *IPv4) Fill(value []net.IP) {
	for i := range value {
		b := c.b[c.i : c.i+c.size]
		value[i] = net.IPv4(b[3], b[2], b[1], b[0]).To4()
		c.i += c.size
	}
}

func (c *IPv4) ValueP() *net.IP {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

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

func (c *IPv4) Append(v net.IP) {
	c.numRow++
	c.writerData = append(c.writerData, v[3], v[2], v[1], v[0])
}

func (c *IPv4) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *IPv4) AppendP(v *net.IP) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

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

func (c *IPv4) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

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

func (c *IPv4) Keys() []int {
	return c.keys
}

func (c *IPv4) resetDict() {
	c.keys = c.keys[:0]
	c.dict = make(map[string]int)
}
