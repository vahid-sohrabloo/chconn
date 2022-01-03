package column

type Uint8 struct {
	column
	val  uint8
	dict map[uint8]int
	keys []int
}

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

func (c *Uint8) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.val = c.b[c.i]
	c.i += c.size
	return true
}

func (c *Uint8) Value() uint8 {
	return c.val
}

func (c *Uint8) ReadAll(value *[]uint8) {
	*value = append(*value, c.b...)
}

func (c *Uint8) Fill(value []uint8) {
	copy(value, c.b[c.i:len(value)+c.i])
	c.i += len(value)
}

func (c *Uint8) ValueP() *uint8 {
	if c.colNullable.b[c.i-c.size] == 1 {
		return nil
	}
	val := c.val
	return &val
}

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

func (c *Uint8) Append(v uint8) {
	c.numRow++
	c.writerData = append(c.writerData,
		v,
	)
}

func (c *Uint8) AppendP(v *uint8) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Uint8) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData,
		0,
	)
}

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

func (c *Uint8) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

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

func (c *Uint8) Keys() []int {
	return c.keys
}

func (c *Uint8) resetDict() {
	c.keys = c.keys[:0]
	c.dict = make(map[uint8]int)
}
