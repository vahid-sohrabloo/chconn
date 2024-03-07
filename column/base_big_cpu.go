//go:build !(386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64)
// +build !386,!amd64,!amd64p32,!arm,!arm64,!mipsle,!mips64le,!mips64p32le,!ppc64le,!riscv,!riscv64

package column

import (
	"io"
	"unsafe"
)

// ReadAll read all value in this block and append to the input slice
func (c *Base[T]) readyBufferHook() {
	for i := 0; i < c.totalByte; i += c.size {
		reverseBuffer(c.b[i : i+c.size])
	}
}

func reverseBuffer(s []byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
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
	b := *(*[]byte)(unsafe.Pointer(&s))
	for i := 0; i < len(b); i += c.size {
		reverseBuffer(b[i : i+c.size])
	}
	var n int64
	nw, err := w.Write(*(*[]byte)(unsafe.Pointer(&s)))
	return int64(nw) + n, err
}
