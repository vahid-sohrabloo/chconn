//go:build !(386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) || purego
// +build !386,!amd64,!amd64p32,!arm,!arm64,!mipsle,!mips64le,!mips64p32le,!ppc64le,!riscv,!riscv64 purego

package column

import "encoding/binary"

// ReadAll read all value in this block and append to the input slice
func (c *Uint64) ReadAll(value *[]uint64) {
	for i := 0; i < c.totalByte; i += Uint64Size {
		*value = append(*value,
			binary.LittleEndian.Uint64(c.b[i:i+Uint64Size]))
	}
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
		val := binary.LittleEndian.Uint64(c.b[i : i+Uint64Size])
		*value = append(*value, &val)
	}
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Uint64) Row(row int) uint64 {
	i := row * Uint64Size
	return binary.LittleEndian.Uint64(c.b[i : i+Uint64Size])
}

// RowP return the value of given row for nullable data
// NOTE: Row number start from zero
//
// As an alternative (for better performance), you can use `Row()` to get a value and `ValueIsNil()` to check if it is null.
//
func (c *Uint64) RowP(row int) *uint64 {
	if c.colNullable.b[row] == 1 {
		return nil
	}
	i := row * Uint64Size
	val := binary.LittleEndian.Uint64(c.b[i : i+Uint64Size])
	return &val
}

// Value of current pointer
//
// Use with Next()
func (c *Uint64) Value() uint64 {
	return binary.LittleEndian.Uint64(c.b[c.i-Uint64Size : c.i])
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
	val := binary.LittleEndian.Uint64(c.b[c.i-Uint64Size : c.i])
	return &val
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *Uint64) Fill(value []uint64) {
	for i := range value {
		value[i] = binary.LittleEndian.Uint64(c.b[c.i : c.i+Uint64Size])
		c.i += Uint64Size
	}
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
		val := binary.LittleEndian.Uint64(c.b[c.i : c.i+Uint64Size])
		value[i] = &val
		c.i += Uint64Size
	}
}
