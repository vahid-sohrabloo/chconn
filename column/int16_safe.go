//go:build !(386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) || purego
// +build !386,!amd64,!amd64p32,!arm,!arm64,!mipsle,!mips64le,!mips64p32le,!ppc64le,!riscv,!riscv64 purego

package column

import "encoding/binary"

// ReadAll read all value in this block and append to the input slice
func (c *Int16) ReadAll(value *[]int16) {
	for i := 0; i < c.totalByte; i += Int16Size {
		*value = append(*value,
			int16(binary.LittleEndian.Uint16(c.b[i:i+Int16Size])))
	}
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Int16) ReadAllP(value *[]*int16) {
	for i := 0; i < c.totalByte; i += Int16Size {
		if c.colNullable.b[i/Int16Size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := int16(binary.LittleEndian.Uint16(c.b[i : i+Int16Size]))
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Int16) Row(row int) int16 {
	i := row * Int16Size
	return int16(binary.LittleEndian.Uint16(c.b[i : i+Int16Size]))
}

// Row[ return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *Int16) RowP(row int) *int16 {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * Int16Size
	val := int16(binary.LittleEndian.Uint16(c.b[i : i+Int16Size]))
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *Int16) Value() int16 {
	return int16(binary.LittleEndian.Uint16(c.b[c.i-Int16Size : c.i]))
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Int16) ValueP() *int16 {
	if c.colNullable.b[(c.i-Int16Size)/(Int16Size)] == 1 {
		return nil
	}
	val := int16(binary.LittleEndian.Uint16(c.b[c.i-Int16Size : c.i]))
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Int16) Fill(value []int16) {
	for i := range value {
		value[i] = int16(binary.LittleEndian.Uint16(c.b[c.i : c.i+Int16Size]))
		c.i += Int16Size
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Int16) FillP(value []*int16) {
	for i := range value {
		if c.colNullable.b[c.i/Int16Size] == 1 {
			value[i] = nil
			c.i += Int16Size
			continue
		}
		val := int16(binary.LittleEndian.Uint16(c.b[c.i : c.i+Int16Size]))
		value[i] = &val
		c.i += Int16Size
	}
}
