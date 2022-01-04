package column

type Raw struct {
	column
	val  []byte
	dict map[string]int
	keys []int
}

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

func (c *Raw) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.val = c.b[c.i : c.i+c.size]
	c.i += c.size
	return true
}

func (c *Raw) Value() []byte {
	return c.val
}

func (c *Raw) ReadAll(value *[][]byte) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value, c.b[i:i+c.size])
	}
}

func (c *Raw) Fill(value [][]byte) {
	for i := range value {
		value[i] = c.b[c.i : c.i+c.size]
		c.i += c.size
	}
}

func (c *Raw) ValueP() *[]byte {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

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

func (c *Raw) Append(v []byte) {
	c.numRow++
	c.writerData = append(c.writerData, v[:c.size]...)
}

func (c *Raw) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Raw) AppendP(v *[]byte) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

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

func (c *Raw) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

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

func (c *Raw) Keys() []int {
	return c.keys
}

func (c *Raw) Reset() {
	c.column.Reset()
}
