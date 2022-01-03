package column

import (
	"encoding/binary"
	"math"
)

type Float64 struct {
	column
	val  float64
	dict map[float64]int
	keys []int
}

func NewFloat64(nullable bool) *Float64 {
	return &Float64{
		dict: make(map[float64]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Float64Size,
		},
	}
}

func (c *Float64) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = math.Float64frombits(binary.LittleEndian.Uint64(c.b[c.i-c.size : c.i]))
	return true
}

func (c *Float64) Value() float64 {
	return c.val
}

func (c *Float64) ReadAll(value *[]float64) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			math.Float64frombits(binary.LittleEndian.Uint64(c.b[i:i+c.size])))
	}
}

func (c *Float64) Fill(value []float64) {
	for i := range value {
		value[i] = math.Float64frombits(binary.LittleEndian.Uint64(c.b[c.i : c.i+c.size]))
		c.i += c.size
	}
}

func (c *Float64) ValueP() *float64 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Float64) ReadAllP(value *[]*float64) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := math.Float64frombits(binary.LittleEndian.Uint64(c.b[i : i+c.size]))
		*value = append(*value, &val)
	}
}

func (c *Float64) FillP(value []*float64) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := math.Float64frombits(binary.LittleEndian.Uint64(c.b[c.i : c.i+c.size]))
		value[i] = &val
		c.i += c.size
	}
}

func (c *Float64) Append(v float64) {
	c.numRow++
	castVal := math.Float64bits(v)
	c.writerData = append(c.writerData,
		byte(castVal),
		byte(castVal>>8),
		byte(castVal>>16),
		byte(castVal>>24),
		byte(castVal>>32),
		byte(castVal>>40),
		byte(castVal>>48),
		byte(castVal>>56),
	)
}

func (c *Float64) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Float64) AppendP(v *float64) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Float64) AppendDict(v float64) {
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

func (c *Float64) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *Float64) AppendDictP(v *float64) {
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

func (c *Float64) Keys() []int {
	return c.keys
}

func (c *Float64) resetDict() {
	c.keys = c.keys[:0]
	c.dict = make(map[float64]int)
}
