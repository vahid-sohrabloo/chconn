package column

import (
	"encoding/binary"
)

type Uint64 struct {
	column
	val  uint64
	dict map[uint64]int
	keys []int
}

func NewUint64(nullable bool) *Uint64 {
	return &Uint64{
		dict: make(map[uint64]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Uint64Size,
		},
	}
}

func (c *Uint64) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = binary.LittleEndian.Uint64(c.b[c.i-c.size : c.i])
	return true
}

func (c *Uint64) Value() uint64 {
	return c.val
}

func (c *Uint64) ReadAll(value *[]uint64) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			binary.LittleEndian.Uint64(c.b[i:i+c.size]))
	}
}

func (c *Uint64) Fill(value []uint64) {
	for i := range value {
		value[i] = binary.LittleEndian.Uint64(c.b[c.i : c.i+c.size])
		c.i += c.size
	}
}

func (c *Uint64) ValueP() *uint64 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Uint64) ReadAllP(value *[]*uint64) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := binary.LittleEndian.Uint64(c.b[i : i+c.size])
		*value = append(*value, &val)
	}
}

func (c *Uint64) FillP(value []*uint64) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := binary.LittleEndian.Uint64(c.b[c.i : c.i+c.size])
		value[i] = &val
		c.i += c.size
	}
}

func (c *Uint64) Append(v uint64) {
	c.numRow++
	c.writerData = append(c.writerData,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
		byte(v>>32),
		byte(v>>40),
		byte(v>>48),
		byte(v>>56),
	)
}

func (c *Uint64) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Uint64) AppendP(v *uint64) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Uint64) AppendDict(v uint64) {
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

func (c *Uint64) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *Uint64) AppendDictP(v *uint64) {
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

func (c *Uint64) Keys() []int {
	return c.keys
}

func (c *Uint64) resetDict() {
	c.keys = c.keys[:0]
	c.dict = make(map[uint64]int)
}
