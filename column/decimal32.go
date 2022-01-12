package column

import (
	"encoding/binary"
)

// Decimal32 use for Decimal32 ClickHouse DataType
type Decimal32 struct {
	column
	factor float64
	val    float64
}

// NewDecimal32 return new Decimal32 for Decimal32 ClickHouse DataType
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

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *Decimal32) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = float64(int32(binary.LittleEndian.Uint32(c.b[c.i-c.size:c.i]))) / c.factor
	return true
}

// Value of current pointer
//
// Use with Next()
func (c *Decimal32) Value() float64 {
	return c.val
}

// ReadAll read all value in this block and append to the input slice
func (c *Decimal32) ReadAll(value *[]float64) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			float64(int32(binary.LittleEndian.Uint32(c.b[i:i+c.size])))/c.factor)
	}
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Decimal32) Fill(value []float64) {
	for i := range value {
		value[i] = float64(int32(binary.LittleEndian.Uint32(c.b[c.i:c.i+c.size]))) / c.factor
		c.i += c.size
	}
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Decimal32) ValueP() *float64 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
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

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
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

// Append value for insert
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

// AppendEmpty append empty value for insert
func (c *Decimal32) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *Decimal32) AppendP(v *float64) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *Decimal32) Reset() {
	c.column.Reset()
}
