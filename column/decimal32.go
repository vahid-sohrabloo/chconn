package column

import (
	"encoding/binary"
)

type Decimal32 struct {
	column
	factor float64
	val    float64
}

func NewDecimal32(scale int, nullable bool) *Decimal32 {
	return &Decimal32{
		factor: factors10[scale],
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Decimal32Size,
		},
	}
}

func (c *Decimal32) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = float64(int32(binary.LittleEndian.Uint32(c.b[c.i-c.size:c.i]))) / c.factor
	return true
}

func (c *Decimal32) Value() float64 {
	return c.val
}

func (c *Decimal32) ReadAll(value *[]float64) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			float64(int32(binary.LittleEndian.Uint32(c.b[i:i+c.size])))/c.factor)
	}
}

func (c *Decimal32) Fill(value []float64) {
	for i := range value {
		c.i += c.size
		value[i] = float64(int32(binary.LittleEndian.Uint32(c.b[c.i-c.size:c.i]))) / c.factor
	}
}

func (c *Decimal32) ValueP() *float64 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Decimal32) ReadAllP(value *[]*float64) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := float64(int32(binary.LittleEndian.Uint32(c.b[i:i+c.size]))) / c.factor
		*value = append(*value, &val)
	}
}

func (c *Decimal32) FillP(value []*float64) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := float64(int32(binary.LittleEndian.Uint32(c.b[c.i:c.i+c.size]))) / c.factor
		value[i] = &val
		c.i += c.size
	}
}

func (c *Decimal32) Append(v float64) {
	c.numRow++
	castVal := int32(v * c.factor)
	c.writerData = append(c.writerData,
		byte(castVal),
		byte(castVal>>8),
		byte(castVal>>16),
		byte(castVal>>24),
	)
}

func (c *Decimal32) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Decimal32) AppendP(v *float64) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}
