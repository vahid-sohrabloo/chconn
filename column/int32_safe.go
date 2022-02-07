//go:build !(386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) || purego
// +build !386,!amd64,!amd64p32,!arm,!arm64,!mipsle,!mips64le,!mips64p32le,!ppc64le,!riscv,!riscv64 purego

package column

import "encoding/binary"

// ReadAll read all value in this block and append to the input slice
func (c *Int32) ReadAll(value *[]int32) {
	for i := 0; i < c.totalByte; i += Int32Size {
		*value = append(*value,
			int32(binary.LittleEndian.Uint32(c.b[i:i+Int32Size])))
	}
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Int32) ReadAllP(value *[]*int32) {
	for i := 0; i < c.totalByte; i += Int32Size {
		if c.colNullable.b[i/Int32Size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := int32(binary.LittleEndian.Uint32(c.b[i : i+Int32Size]))
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Int32) Row(row int) int32 {
	i := row * Int32Size
	return int32(binary.LittleEndian.Uint32(c.b[i : i+Int32Size]))
}

// Row[ return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *Int32) RowP(row int) *int32 {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * Int32Size
	val := int32(binary.LittleEndian.Uint32(c.b[i : i+Int32Size]))
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *Int32) Value() int32 {
	return int32(binary.LittleEndian.Uint32(c.b[c.i-Int32Size : c.i]))
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Int32) ValueP() *int32 {
	if c.colNullable.b[(c.i-Int32Size)/(Int32Size)] == 1 {
		return nil
	}
	val := int32(binary.LittleEndian.Uint32(c.b[c.i-Int32Size : c.i]))
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Int32) Fill(value []int32) {
	for i := range value {
		value[i] = int32(binary.LittleEndian.Uint32(c.b[c.i : c.i+Int32Size]))
		c.i += Int32Size
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Int32) FillP(value []*int32) {
	for i := range value {
		if c.colNullable.b[c.i/Int32Size] == 1 {
			value[i] = nil
			c.i += Int32Size
			continue
		}
		val := int32(binary.LittleEndian.Uint32(c.b[c.i : c.i+Int32Size]))
		value[i] = &val
		c.i += Int32Size
	}
}
