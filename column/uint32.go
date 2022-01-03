package column

import (
	"encoding/binary"
)

type Uint32 struct {
	column
	val  uint32
	dict map[uint32]int
	keys []int
}

func NewUint32(nullable bool) *Uint32 {
	return &Uint32{
		dict: make(map[uint32]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Uint32Size,
		},
	}
}

func (c *Uint32) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = binary.LittleEndian.Uint32(c.b[c.i-c.size : c.i])
	return true
}

func (c *Uint32) Value() uint32 {
	return c.val
}

func (c *Uint32) ReadAll(value *[]uint32) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			binary.LittleEndian.Uint32(c.b[i:i+c.size]))
	}
}

func (c *Uint32) Fill(value []uint32) {
	for i := range value {
		value[i] = binary.LittleEndian.Uint32(c.b[c.i : c.i+c.size])
		c.i += c.size
	}
}

func (c *Uint32) ValueP() *uint32 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Uint32) ReadAllP(value *[]*uint32) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := binary.LittleEndian.Uint32(c.b[i : i+c.size])
		*value = append(*value, &val)
	}
}

func (c *Uint32) FillP(value []*uint32) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := binary.LittleEndian.Uint32(c.b[c.i : c.i+c.size])
		value[i] = &val
		c.i += c.size
	}
}

func (c *Uint32) Append(v uint32) {
	c.numRow++
	c.writerData = append(c.writerData,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
	)
}

func (c *Uint32) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Uint32) AppendP(v *uint32) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Uint32) AppendDict(v uint32) {
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

func (c *Uint32) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *Uint32) AppendDictP(v *uint32) {
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

func (c *Uint32) Keys() []int {
	return c.keys
}

func (c *Uint32) resetDict() {
	c.keys = c.keys[:0]
	c.dict = make(map[uint32]int)
}
