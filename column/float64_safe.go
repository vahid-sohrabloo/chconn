//go:build !(386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) || purego
// +build !386,!amd64,!amd64p32,!arm,!arm64,!mipsle,!mips64le,!mips64p32le,!ppc64le,!riscv,!riscv64 purego

package column

import (
	"encoding/binary"
	"math"
)

// ReadAll read all value in this block and append to the input slice
func (c *Float64) ReadAll(value *[]float64) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			math.Float64frombits(binary.LittleEndian.Uint64(c.b[i:i+c.size])))
	}
}
