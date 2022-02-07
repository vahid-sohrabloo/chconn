//go:build (386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) && !purego
// +build 386 amd64 amd64p32 arm arm64 mipsle mips64le mips64p32le ppc64le riscv riscv64
// +build !purego

package column

import (
	"unsafe"
)

// GetAllUnsafe get all the data in current block as a slice.
//
// NOTE: this function is unsafe and only can use in little-endian system cpu architecture.
// This data only valid in current block, if you want to use it after, you should copy it. or use ReadAll
func (c *Uint32) GetAllUnsafe() []uint32 {
	value := *(*[]uint32)(unsafe.Pointer(&c.b))
	return value[:c.numRow]
}

// ReadAll reads all the data in current block and append to column.
func (c *Uint32) ReadAll(value *[]uint32) {
	v := *(*[]uint32)(unsafe.Pointer(&c.b))
	*value = append(*value, v[:c.numRow]...)
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Uint32) ReadAllP(value *[]*uint32) {
	for i := 0; i < c.totalByte; i += Uint32Size {
		if c.colNullable.b[i/Uint32Size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := *(*uint32)(unsafe.Pointer(&c.b[i]))
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Uint32) Row(row int) uint32 {
	i := row * Uint32Size
	return *(*uint32)(unsafe.Pointer(&c.b[i]))
}

// Row[ return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *Uint32) RowP(row int) *uint32 {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * Uint32Size
	val := *(*uint32)(unsafe.Pointer(&c.b[i]))
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *Uint32) Value() uint32 {
	return *(*uint32)(unsafe.Pointer(&c.b[c.i-Uint32Size]))
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Uint32) ValueP() *uint32 {
	if c.colNullable.b[(c.i-Uint32Size)/(Uint32Size)] == 1 {
		return nil
	}
	val := *(*uint32)(unsafe.Pointer(&c.b[c.i-Uint32Size]))
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Uint32) Fill(value []uint32) {
	if len(value) == 0 {
		return
	}
	d := c.b[c.i:]
	copy(value, *(*[]uint32)(unsafe.Pointer(&d)))
	c.i += Uint32Size * len(value)
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Uint32) FillP(value []*uint32) {
	for i := range value {
		if c.colNullable.b[c.i/Uint32Size] == 1 {
			value[i] = nil
			c.i += Uint32Size
			continue
		}
		val := *(*uint32)(unsafe.Pointer(&c.b[c.i]))
		value[i] = &val
		c.i += Uint32Size
	}
}
