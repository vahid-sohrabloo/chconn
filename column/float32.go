package column

import (
	"encoding/binary"
	"math"
)

type Float32 struct {
	column
	val  float32
	dict map[float32]int
	keys []int
}

func NewFloat32(nullable bool) *Float32 {
	return &Float32{
		dict: make(map[float32]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Float32Size,
		},
	}
}

func (c *Float32) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = math.Float32frombits(binary.LittleEndian.Uint32(c.b[c.i-c.size : c.i]))
	return true
}

func (c *Float32) Value() float32 {
	return c.val
}

func (c *Float32) ReadAll(value *[]float32) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			math.Float32frombits(binary.LittleEndian.Uint32(c.b[i:i+c.size])))
	}
}

func (c *Float32) Fill(value []float32) {
	for i := range value {
		value[i] = math.Float32frombits(binary.LittleEndian.Uint32(c.b[c.i : c.i+c.size]))
		c.i += c.size
	}
}

func (c *Float32) ValueP() *float32 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Float32) ReadAllP(value *[]*float32) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := math.Float32frombits(binary.LittleEndian.Uint32(c.b[i : i+c.size]))
		*value = append(*value, &val)
	}
}

func (c *Float32) FillP(value []*float32) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := math.Float32frombits(binary.LittleEndian.Uint32(c.b[c.i : c.i+c.size]))
		value[i] = &val
		c.i += c.size
	}
}

func (c *Float32) Append(v float32) {
	c.numRow++
	castVal := math.Float32bits(v)
	c.writerData = append(c.writerData,
		byte(castVal),
		byte(castVal>>8),
		byte(castVal>>16),
		byte(castVal>>24),
	)
}

func (c *Float32) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Float32) AppendP(v *float32) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Float32) AppendDict(v float32) {
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

func (c *Float32) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *Float32) AppendDictP(v *float32) {
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

func (c *Float32) Keys() []int {
	return c.keys
}

func (c *Float32) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[float32]int)
}
