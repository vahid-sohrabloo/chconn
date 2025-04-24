package column

import (
	"io"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

type indicesColumnI interface {
	ReadRaw(num int) error
	WriteTo(io.Writer) (int64, error)
	setKeys([]uint32)
	readInt(value *[]uint32)
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

func (c *indicesColumn[T]) readInt(value *[]uint32) {
	for _, v := range c.Data() {
		*value = append(*value,
			uint32(v),
		)
	}
}

func (c *indicesColumn[T]) setKeys(values []uint32) {
	c.Reset()
	if len(values) > cap(c.values) {
		c.values = make([]T, len(values))
	} else {
		c.values = c.values[:len(values)]
	}
	for i, v := range values {
		c.values[i] = T(v)
	}
}
