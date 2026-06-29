//go:build !(386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64)
// +build !386,!amd64,!amd64p32,!arm,!arm64,!mipsle,!mips64le,!mips64p32le,!ppc64le,!riscv,!riscv64

package column

import (
	"io"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
)

func (c *Base[T]) readBufferHook() {
	dst := helper.ConvertToByte(c.values, c.size)
	for i := 0; i < len(dst); i += c.size {
		reverseBuffer(dst[i : i+c.size])
	}
}

func reverseBuffer(s []byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func (c *Base[T]) WriteTo(w io.Writer) (int64, error) {
	s := helper.ConvertToByte(c.values, c.size)
	for i := 0; i < len(s); i += c.size {
		reverseBuffer(s[i : i+c.size])
	}
	var n int64
	nw, err := w.Write(s)
	return int64(nw) + n, err
}
