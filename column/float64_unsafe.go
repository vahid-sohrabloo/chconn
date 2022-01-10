//go:build (386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) && !purego

package column

import (
	"unsafe"
)

// GetAllUnsafe get all the data in current block as a slice.
// NOTE: this function is unsafe and only can use in little-endian system cpu architecture.
func (c *Float64) GetAllUnsafe() []float64 {
	value := *(*[]float64)(unsafe.Pointer(&c.b))
	return value[:c.numRow]
}

// ReadAllUnsafe reads all the data in current block and append to column.
// NOTE: this function is unsafe and only can use in little-endian system  cpu architecture.
func (c *Float64) ReadAllUnsafe(value *[]float64) {
	v := *(*[]float64)(unsafe.Pointer(&c.b))
	*value = append(*value, v[:c.numRow]...)
}
