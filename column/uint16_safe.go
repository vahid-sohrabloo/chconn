//go:build !(386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) || purego
// +build !386,!amd64,!amd64p32,!arm,!arm64,!mipsle,!mips64le,!mips64p32le,!ppc64le,!riscv,!riscv64 purego

package column

import "encoding/binary"

// ReadAll read all value in this block and append to the input slice
func (c *Uint16) ReadAll(value *[]uint16) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			binary.LittleEndian.Uint16(c.b[i:i+c.size]))
	}
}
