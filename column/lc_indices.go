package column

import (
	"io"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

type indicesColumnI interface {
	ReadRaw(num int) error
	WriteTo(io.Writer) (int64, error)
	appendInts([]int)
	readInt(value *[]int)
	Remove(int)
	Reset()
}

type indicatedTypes interface {
	uint8 | uint16 | uint32 | uint64
}

type indicesColumn[T indicatedTypes] struct {
	Base[T]
}

func newIndicesColumn[T indicatedTypes](r *readerwriter.Reader) *indicesColumn[T] {
	var tmpValue T
	size := int(unsafe.Sizeof(tmpValue))
	return &indicesColumn[T]{
		Base: Base[T]{
			strict: true,
			size:   size,
			column: column{
				r: r,
			},
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
