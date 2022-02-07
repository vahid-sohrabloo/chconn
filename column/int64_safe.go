//go:build !(386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) || purego
// +build !386,!amd64,!amd64p32,!arm,!arm64,!mipsle,!mips64le,!mips64p32le,!ppc64le,!riscv,!riscv64 purego

package column

import "encoding/binary"

// ReadAll read all value in this block and append to the input slice
func (c *Int64) ReadAll(value *[]int64) {
	for i := 0; i < c.totalByte; i += Int64Size {
		*value = append(*value,
			int64(binary.LittleEndian.Uint64(c.b[i:i+Int64Size])))
	}
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Int64) ReadAllP(value *[]*int64) {
	for i := 0; i < c.totalByte; i += Int64Size {
		if c.colNullable.b[i/Int64Size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := int64(binary.LittleEndian.Uint64(c.b[i : i+Int64Size]))
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Int64) Row(row int) int64 {
	i := row * Int64Size
	return int64(binary.LittleEndian.Uint64(c.b[i : i+Int64Size]))
}

// Row[ return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *Int64) RowP(row int) *int64 {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * Int64Size
	val := int64(binary.LittleEndian.Uint64(c.b[i : i+Int64Size]))
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *Int64) Value() int64 {
	return int64(binary.LittleEndian.Uint64(c.b[c.i-Int64Size : c.i]))
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Int64) ValueP() *int64 {
	if c.colNullable.b[(c.i-Int64Size)/(Int64Size)] == 1 {
		return nil
	}
	val := int64(binary.LittleEndian.Uint64(c.b[c.i-Int64Size : c.i]))
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Int64) Fill(value []int64) {
	for i := range value {
		value[i] = int64(binary.LittleEndian.Uint64(c.b[c.i : c.i+Int64Size]))
		c.i += Int64Size
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Int64) FillP(value []*int64) {
	for i := range value {
		if c.colNullable.b[c.i/Int64Size] == 1 {
			value[i] = nil
			c.i += Int64Size
			continue
		}
		val := int64(binary.LittleEndian.Uint64(c.b[c.i : c.i+Int64Size]))
		value[i] = &val
		c.i += Int64Size
	}
}
