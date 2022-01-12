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
func (c *Float32) GetAllUnsafe() []float32 {
	value := *(*[]float32)(unsafe.Pointer(&c.b))
	return value[:c.numRow]
}

// ReadAllUnsafe reads all the data in current block and append to column.
//
// NOTE: this function is unsafe and only can use in little-endian system  cpu architecture.
func (c *Float32) ReadAllUnsafe(value *[]float32) {
	v := *(*[]float32)(unsafe.Pointer(&c.b))
	*value = append(*value, v[:c.numRow]...)
}
