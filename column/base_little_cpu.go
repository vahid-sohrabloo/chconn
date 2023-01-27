//go:build 386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64
// +build 386 amd64 amd64p32 arm arm64 mipsle mips64le mips64p32le ppc64le riscv riscv64

package column

import (
	"io"
	"unsafe"
)

func (c *Base[T]) readyBufferHook() {
}

// slice is the runtime representation of a slice.
// It cannot be used safely or portably and its representation may
// change in a later release.
// Moreover, the Data field is not sufficient to guarantee the data
// it references will not be garbage collected, so programs must keep
// a separate, correctly typed pointer to the underlying data.
type slice struct {
	Data uintptr
	Len  int
	Cap  int
}

func (c *Base[T]) WriteTo(w io.Writer) (int64, error) {
	s := *(*slice)(unsafe.Pointer(&c.values))
	s.Len *= c.size
	s.Cap *= c.size
	var n int64
	src := *(*[]byte)(unsafe.Pointer(&s))
	nw, err := w.Write(src)
	return int64(nw) + n, err
}
