//go:build (386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) && !purego

package column

import (
	"unsafe"
)

// ReadAllUnsafe reads all the data and append to column.
// NOTE: this function is unsafe and only can use in lttle-endian system  cpu architecture.
func (c *Uint32) ReadAllUnsafe(value *[]uint32) {
	*value = *(*[]uint32)(unsafe.Pointer(&c.b))
	*value = (*value)[:c.numRow]
}
