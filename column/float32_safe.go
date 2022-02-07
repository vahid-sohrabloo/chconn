//go:build !(386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) || purego
// +build !386,!amd64,!amd64p32,!arm,!arm64,!mipsle,!mips64le,!mips64p32le,!ppc64le,!riscv,!riscv64 purego

package column

import (
	"encoding/binary"
	"math"
)

// ReadAll read all value in this block and append to the input slice
func (c *Float32) ReadAll(value *[]float32) {
	for i := 0; i < c.totalByte; i += Float32Size {
		*value = append(*value,
			math.Float32frombits(binary.LittleEndian.Uint32(c.b[i:i+Float32Size])))
	}
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Float32) ReadAllP(value *[]*float32) {
	for i := 0; i < c.totalByte; i += Float32Size {
		if c.colNullable.b[i/Float32Size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := math.Float32frombits(binary.LittleEndian.Uint32(c.b[i : i+Float32Size]))
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Float32) Row(row int) float32 {
	i := row * Float32Size
	return math.Float32frombits(binary.LittleEndian.Uint32(c.b[i : i+Float32Size]))
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *Float32) RowP(row int) *float32 {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * Float32Size
	val := math.Float32frombits(binary.LittleEndian.Uint32(c.b[i : i+Float32Size]))
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *Float32) Value() float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(c.b[c.i-Float32Size : c.i]))
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Float32) ValueP() *float32 {
	if c.colNullable.b[(c.i-Float32Size)/(Float32Size)] == 1 {
		return nil
	}
	val := math.Float32frombits(binary.LittleEndian.Uint32(c.b[c.i-Float32Size : c.i]))
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Float32) Fill(value []float32) {
	for i := range value {
		value[i] = math.Float32frombits(binary.LittleEndian.Uint32(c.b[c.i : c.i+Float32Size]))
		c.i += Float32Size
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Float32) FillP(value []*float32) {
	for i := range value {
		if c.colNullable.b[c.i/Float32Size] == 1 {
			value[i] = nil
			c.i += Float32Size
			continue
		}
		val := math.Float32frombits(binary.LittleEndian.Uint32(c.b[c.i : c.i+Float32Size]))
		value[i] = &val
		c.i += Float32Size
	}
}
