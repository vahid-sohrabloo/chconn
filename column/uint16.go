package column

import (
	"encoding/binary"
)

type Uint16 struct {
	column
	val  uint16
	dict map[uint16]int
	keys []int
}

func NewUint16(nullable bool) *Uint16 {
	return &Uint16{
		dict: make(map[uint16]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Uint16Size,
		},
	}
}

func (c *Uint16) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = binary.LittleEndian.Uint16(c.b[c.i-c.size : c.i])
	return true
}

func (c *Uint16) Value() uint16 {
	return c.val
}

func (c *Uint16) ReadAll(value *[]uint16) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			binary.LittleEndian.Uint16(c.b[i:i+c.size]))
	}
}

func (c *Uint16) Fill(value []uint16) {
	for i := range value {
		value[i] = binary.LittleEndian.Uint16(c.b[c.i : c.i+c.size])
		c.i += c.size
	}
}

func (c *Uint16) ValueP() *uint16 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Uint16) ReadAllP(value *[]*uint16) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := binary.LittleEndian.Uint16(c.b[i : i+c.size])
		*value = append(*value, &val)
	}
}

func (c *Uint16) FillP(value []*uint16) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := binary.LittleEndian.Uint16(c.b[c.i : c.i+c.size])
		value[i] = &val
		c.i += c.size
	}
}

func (c *Uint16) Append(v uint16) {
	c.numRow++
	c.writerData = append(c.writerData,
		byte(v),
		byte(v>>8),
	)
}

func (c *Uint16) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Uint16) AppendP(v *uint16) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Uint16) AppendDict(v uint16) {
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

func (c *Uint16) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *Uint16) AppendDictP(v *uint16) {
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

func (c *Uint16) Keys() []int {
	return c.keys
}

func (c *Uint16) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[uint16]int)
}
