package column

import (
	"encoding/binary"
)

type Int64 struct {
	column
	val  int64
	dict map[int64]int
	keys []int
}

func NewInt64(nullable bool) *Int64 {
	return &Int64{
		dict: make(map[int64]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Int64Size,
		},
	}
}

func (c *Int64) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = int64(binary.LittleEndian.Uint64(c.b[c.i-c.size : c.i]))
	return true
}

func (c *Int64) Value() int64 {
	return c.val
}

func (c *Int64) ReadAll(value *[]int64) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			int64(binary.LittleEndian.Uint64(c.b[i:i+c.size])))
	}
}

func (c *Int64) Fill(value []int64) {
	for i := range value {
		value[i] = int64(binary.LittleEndian.Uint64(c.b[c.i : c.i+c.size]))
		c.i += c.size
	}
}

func (c *Int64) ValueP() *int64 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Int64) ReadAllP(value *[]*int64) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := int64(binary.LittleEndian.Uint64(c.b[i : i+c.size]))
		*value = append(*value, &val)
	}
}

func (c *Int64) FillP(value []*int64) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := int64(binary.LittleEndian.Uint64(c.b[c.i : c.i+c.size]))
		value[i] = &val
		c.i += c.size
	}
}

func (c *Int64) Append(v int64) {
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

func (c *Int64) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Int64) AppendP(v *int64) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Int64) AppendDict(v int64) {
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

func (c *Int64) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *Int64) AppendDictP(v *int64) {
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

func (c *Int64) Keys() []int {
	return c.keys
}

func (c *Int64) resetDict() {
	c.keys = c.keys[:0]
	c.dict = make(map[int64]int)
}
