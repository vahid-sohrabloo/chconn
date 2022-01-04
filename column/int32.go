package column

import (
	"encoding/binary"
)

type Int32 struct {
	column
	val  int32
	dict map[int32]int
	keys []int
}

func NewInt32(nullable bool) *Int32 {
	return &Int32{
		dict: make(map[int32]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Int32Size,
		},
	}
}

func (c *Int32) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = int32(binary.LittleEndian.Uint32(c.b[c.i-c.size : c.i]))
	return true
}

func (c *Int32) Value() int32 {
	return c.val
}

func (c *Int32) ReadAll(value *[]int32) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			int32(binary.LittleEndian.Uint32(c.b[i:i+c.size])))
	}
}

func (c *Int32) Fill(value []int32) {
	for i := range value {
		value[i] = int32(binary.LittleEndian.Uint32(c.b[c.i : c.i+c.size]))
		c.i += c.size
	}
}

func (c *Int32) ValueP() *int32 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Int32) ReadAllP(value *[]*int32) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := int32(binary.LittleEndian.Uint32(c.b[i : i+c.size]))
		*value = append(*value, &val)
	}
}

func (c *Int32) FillP(value []*int32) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := int32(binary.LittleEndian.Uint32(c.b[c.i : c.i+c.size]))
		value[i] = &val
		c.i += c.size
	}
}

func (c *Int32) Append(v int32) {
	c.numRow++
	c.writerData = append(c.writerData,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
	)
}

func (c *Int32) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Int32) AppendP(v *int32) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Int32) AppendDict(v int32) {
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

func (c *Int32) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *Int32) AppendDictP(v *int32) {
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

func (c *Int32) Keys() []int {
	return c.keys
}

func (c *Int32) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[int32]int)
}
