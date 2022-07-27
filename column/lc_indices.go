package column

import (
	"io"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
)

type indicesColumnI interface {
	ReadRaw(num int, r *readerwriter.Reader) error
	WriteTo(io.Writer) (int64, error)
	appendInts([]int)
	readInt(value *[]int)
	Reset()
}

type indicatedTypes interface {
	uint8 | uint16 | uint32 | uint64
}

type indicesColumn[T indicatedTypes] struct {
	Base[T]
}

func newIndicesColumn[T indicatedTypes]() *indicesColumn[T] {
	var tmpValue T
	size := int(unsafe.Sizeof(tmpValue))
	return &indicesColumn[T]{
		Base: Base[T]{
			size: size,
		},
	}
}

func (c *indicesColumn[T]) readInt(value *[]int) {
	for _, v := range c.Data() {
		*value = append(*value,
			int(v),
		)
	}
}

func (c *indicesColumn[T]) appendInts(values []int) {
	for _, v := range values {
		c.values = append(c.values, T(v))
	}
}
