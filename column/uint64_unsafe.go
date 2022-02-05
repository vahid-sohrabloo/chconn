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
func (c *Uint64) GetAllUnsafe() []uint64 {
	value := *(*[]uint64)(unsafe.Pointer(&c.b))
	return value[:c.numRow]
}

// ReadAll reads all the data in current block and append to column.
func (c *Uint64) ReadAll(value *[]uint64) {
	v := *(*[]uint64)(unsafe.Pointer(&c.b))
	*value = append(*value, v[:c.numRow]...)
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *Uint64) ReadAllP(value *[]*uint64) {
	for i := 0; i < c.totalByte; i += Uint64Size {
		if c.colNullable.b[i/Uint64Size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := *(*uint64)(unsafe.Pointer(&c.b[i]))
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Uint64) Row(row int) uint64 {
	i := row * Uint64Size
	return *(*uint64)(unsafe.Pointer(&c.b[i]))
}

// Row return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *Uint64) RowP(row int) *uint64 {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * Uint64Size
	val := *(*uint64)(unsafe.Pointer(&c.b[i]))
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *Uint64) Value() uint64 {
	return *(*uint64)(unsafe.Pointer(&c.b[c.i-Uint64Size]))
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *Uint64) ValueP() *uint64 {
	if c.colNullable.b[(c.i-Uint64Size)/(Uint64Size)] == 1 {
		return nil
	}
	val := *(*uint64)(unsafe.Pointer(&c.b[c.i-Uint64Size]))
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Uint64) Fill(value []uint64) {
	if len(value) == 0 {
		return
	}
	d := c.b[c.i:]
	copy(value, *(*[]uint64)(unsafe.Pointer(&d)))
	c.i += Uint64Size * len(value)
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Uint64) FillP(value []*uint64) {
	for i := range value {
		if c.colNullable.b[c.i/Uint64Size] == 1 {
			value[i] = nil
			c.i += Uint64Size
			continue
		}
		val := *(*uint64)(unsafe.Pointer(&c.b[c.i]))
		value[i] = &val
		c.i += Uint64Size
	}
}
