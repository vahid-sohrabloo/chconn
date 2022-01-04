package column

type Int8 struct {
	column
	val  int8
	dict map[int8]int
	keys []int
}

func NewInt8(nullable bool) *Int8 {
	return &Int8{
		dict: make(map[int8]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Int8Size,
		},
	}
}

func (c *Int8) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.val = int8(c.b[c.i])
	c.i += c.size
	return true
}

func (c *Int8) Value() int8 {
	return c.val
}

func (c *Int8) ReadAll(value *[]int8) {
	for _, v := range c.b {
		*value = append(*value, int8(v))
	}
}

func (c *Int8) Fill(value []int8) {
	for i := range value {
		value[i] = int8(c.b[c.i])
		c.i += c.size
	}
}

func (c *Int8) ValueP() *int8 {
	if c.colNullable.b[c.i-c.size] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Int8) ReadAllP(value *[]*int8) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := int8(c.b[i])
		*value = append(*value, &val)
	}
}

func (c *Int8) FillP(value []*int8) {
	for i := range value {
		if c.colNullable.b[c.i] == 1 {
			c.i += c.size
			value[i] = nil
			continue
		}
		val := int8(c.b[c.i])
		value[i] = &val
		c.i += c.size
	}
}

func (c *Int8) Append(v int8) {
	c.numRow++
	c.writerData = append(c.writerData,
		byte(v),
	)
}

func (c *Int8) AppendP(v *int8) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Int8) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData,
		0,
	)
}

func (c *Int8) AppendDict(v int8) {
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

func (c *Int8) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *Int8) AppendDictP(v *int8) {
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

func (c *Int8) Keys() []int {
	return c.keys
}

func (c *Int8) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[int8]int)
}
