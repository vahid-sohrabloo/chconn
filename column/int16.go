package column

import (
	"encoding/binary"
)

type Int16 struct {
	column
	val  int16
	dict map[int16]int
	keys []int
}

func NewInt16(nullable bool) *Int16 {
	return &Int16{
		dict: make(map[int16]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Int16Size,
		},
	}
}

func (c *Int16) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = int16(binary.LittleEndian.Uint16(c.b[c.i-c.size : c.i]))
	return true
}

func (c *Int16) Value() int16 {
	return c.val
}

func (c *Int16) ReadAll(value *[]int16) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			int16(binary.LittleEndian.Uint16(c.b[i:i+c.size])))
	}
}

func (c *Int16) Fill(value []int16) {
	for i := range value {
		value[i] = int16(binary.LittleEndian.Uint16(c.b[c.i : c.i+c.size]))
		c.i += c.size
	}
}

func (c *Int16) ValueP() *int16 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Int16) ReadAllP(value *[]*int16) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := int16(binary.LittleEndian.Uint16(c.b[i : i+c.size]))
		*value = append(*value, &val)
	}
}

func (c *Int16) FillP(value []*int16) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := int16(binary.LittleEndian.Uint16(c.b[c.i : c.i+c.size]))
		value[i] = &val
		c.i += c.size
	}
}

func (c *Int16) Append(v int16) {
	c.numRow++
	c.writerData = append(c.writerData,
		byte(v),
		byte(v>>8),
	)
}

func (c *Int16) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Int16) AppendP(v *int16) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Int16) AppendDict(v int16) {
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

func (c *Int16) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *Int16) AppendDictP(v *int16) {
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

func (c *Int16) Keys() []int {
	return c.keys
}

func (c *Int16) resetDict() {
	c.keys = c.keys[:0]
	c.dict = make(map[int16]int)
}
