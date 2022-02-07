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
func (c *Int16) GetAllUnsafe() []int16 {
	value := *(*[]int16)(unsafe.Pointer(&c.b))
	return value[:c.numRow]
}

// ReadAll reads all the data in current block and append to column.
func (c *Int16) ReadAll(value *[]int16) {
	v := *(*[]int16)(unsafe.Pointer(&c.b))
	*value = append(*value, v[:c.numRow]...)
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
		val := *(*int16)(unsafe.Pointer(&c.b[i]))
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Int16) Row(row int) int16 {
	i := row * Int16Size
	return *(*int16)(unsafe.Pointer(&c.b[i]))
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
	val := *(*int16)(unsafe.Pointer(&c.b[i]))
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *Int16) Value() int16 {
	return *(*int16)(unsafe.Pointer(&c.b[c.i-Int16Size]))
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
	val := *(*int16)(unsafe.Pointer(&c.b[c.i-Int16Size]))
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Int16) Fill(value []int16) {
	if len(value) == 0 {
		return
	}
	d := c.b[c.i:]
	copy(value, *(*[]int16)(unsafe.Pointer(&d)))
	c.i += Int16Size * len(value)
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
		val := *(*int16)(unsafe.Pointer(&c.b[c.i]))
		value[i] = &val
		c.i += Int16Size
	}
}
