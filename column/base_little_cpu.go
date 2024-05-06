//go:build 386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64
// +build 386 amd64 amd64p32 arm arm64 mipsle mips64le mips64p32le ppc64le riscv riscv64

package column

import (
	"io"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
)

func (c *Base[T]) readBufferHook() {
}

func (c *Base[T]) WriteTo(w io.Writer) (int64, error) {
	s := helper.ConvertToByte(c.values, c.size)
	var n int64
	nw, err := w.Write(s)
	return int64(nw) + n, err
}
