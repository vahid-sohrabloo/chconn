//go:build !(386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) || purego
// +build !386,!amd64,!amd64p32,!arm,!arm64,!mipsle,!mips64le,!mips64p32le,!ppc64le,!riscv,!riscv64 purego

package column

import (
	"encoding/binary"
	"math"
)

// ReadAll read all value in this block and append to the input slice
func (c *Float64) ReadAll(value *[]float64) {
	for i := 0; i < c.totalByte; i += Float64Size {
		*value = append(*value,
			math.Float64frombits(binary.LittleEndian.Uint64(c.b[i:i+Float64Size])))
	}
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Float64) ReadAllP(value *[]*float64) {
	for i := 0; i < c.totalByte; i += Float64Size {
		if c.colNullable.b[i/Float64Size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := math.Float64frombits(binary.LittleEndian.Uint64(c.b[i : i+Float64Size]))
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Float64) Row(row int) float64 {
	i := row * Float64Size
	return math.Float64frombits(binary.LittleEndian.Uint64(c.b[i : i+Float64Size]))
}

// Row[ return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *Float64) RowP(row int) *float64 {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * Float64Size
	val := math.Float64frombits(binary.LittleEndian.Uint64(c.b[i : i+Float64Size]))
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *Float64) Value() float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(c.b[c.i-Float64Size : c.i]))
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Float64) ValueP() *float64 {
	if c.colNullable.b[(c.i-Float64Size)/(Float64Size)] == 1 {
		return nil
	}
	val := math.Float64frombits(binary.LittleEndian.Uint64(c.b[c.i-Float64Size : c.i]))
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Float64) Fill(value []float64) {
	for i := range value {
		value[i] = math.Float64frombits(binary.LittleEndian.Uint64(c.b[c.i : c.i+Float64Size]))
		c.i += Float64Size
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Float64) FillP(value []*float64) {
	for i := range value {
		if c.colNullable.b[c.i/Float64Size] == 1 {
			value[i] = nil
			c.i += Float64Size
			continue
		}
		val := math.Float64frombits(binary.LittleEndian.Uint64(c.b[c.i : c.i+Float64Size]))
		value[i] = &val
		c.i += Float64Size
	}
}
