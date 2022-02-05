package column

import (
	"encoding/binary"
)

// Decimal64 use for Decimal64 ClickHouse DataType
type Decimal64 struct {
	column
	factor float64
	Scale  int
}

// NewDecimal64 return new Decimal64 for Decimal64 ClickHouse DataType
func NewDecimal64(scale int, nullable bool) *Decimal64 {
	return &Decimal64{
		factor: factors10[scale],
		Scale:  scale,
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Decimal64Size,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *Decimal64) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	return true
}

// Value of current pointer
//
// Use with Next()
func (c *Decimal64) Value() float64 {
	return float64(int64(binary.LittleEndian.Uint64(c.b[c.i-c.size:c.i]))) / c.factor
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Decimal64) ValueP() *float64 {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := float64(int64(binary.LittleEndian.Uint64(c.b[c.i-c.size:c.i]))) / c.factor
	return &val
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Decimal64) Row(row int) float64 {
	i := row * Decimal64Size
	return float64(int64(binary.LittleEndian.Uint64(c.b[i:i+Decimal64Size]))) / c.factor
}

// ReadAll read all value in this block and append to the input slice
func (c *Decimal64) ReadAll(value *[]float64) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			float64(int64(binary.LittleEndian.Uint64(c.b[i:i+c.size])))/c.factor)
	}
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Decimal64) Fill(value []float64) {
	for i := range value {
		value[i] = float64(int64(binary.LittleEndian.Uint64(c.b[c.i:c.i+c.size]))) / c.factor
		c.i += c.size
	}
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Decimal64) ReadAllP(value *[]*float64) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := float64(int64(binary.LittleEndian.Uint64(c.b[i:i+c.size]))) / c.factor
		*value = append(*value, &val)
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Decimal64) FillP(value []*float64) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := float64(int64(binary.LittleEndian.Uint64(c.b[c.i:c.i+c.size]))) / c.factor
		value[i] = &val
		c.i += c.size
	}
}

// Append value for insert
func (c *Decimal64) Append(v float64) {
	c.numRow++
	castVal := int64(v * c.factor)
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

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *Decimal64) AppendP(v *float64) {
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
func (c *Decimal64) Reset() {
	c.column.Reset()
}
