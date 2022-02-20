package column

import (
	"encoding/binary"
	"io"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

type indicesColumn interface {
	Next() bool
	ReadRaw(num int, r *readerwriter.Reader) error
	WriteTo(io.Writer) (int64, error)
	valueInt() int
	readAllInt(*[]int)
	fillInt([]int)
	appendInts([]int)
	rowInt(int) int
	Reset()
}

// uint8 indices
func (c *Uint8) valueInt() int {
	return int(c.Value())
}

func (c *Uint8) rowInt(i int) int {
	return int(c.Row(i))
}

func (c *Uint8) readAllInt(value *[]int) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			int(c.b[i]),
		)
	}
}

func (c *Uint8) fillInt(value []int) {
	for i := range value {
		value[i] = int(c.b[c.i])
		c.i += c.size
	}
}

func (c *Uint8) appendInts(values []int) {
	for _, v := range values {
		c.writerData = append(c.writerData, uint8(v))
	}
}

// uint16 indices
func (c *Uint16) valueInt() int {
	return int(c.Value())
}

func (c *Uint16) rowInt(i int) int {
	return int(c.Row(i))
}

func (c *Uint16) readAllInt(value *[]int) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			int(binary.LittleEndian.Uint16(c.b[i:i+c.size])),
		)
	}
}

func (c *Uint16) fillInt(value []int) {
	for i := range value {
		value[i] = int(binary.LittleEndian.Uint16(c.b[c.i : c.i+c.size]))
		c.i += c.size
	}
}

func (c *Uint16) appendInts(ints []int) {
	for _, v := range ints {
		c.writerData = append(c.writerData,
			byte(v),
			byte(v>>8),
		)
	}
}

// uint32 indices
func (c *Uint32) valueInt() int {
	return int(c.Value())
}

func (c *Uint32) rowInt(i int) int {
	return int(c.Row(i))
}

func (c *Uint32) readAllInt(value *[]int) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			int(binary.LittleEndian.Uint32(c.b[i:i+c.size])),
		)
	}
}

func (c *Uint32) fillInt(value []int) {
	for i := range value {
		value[i] = int(binary.LittleEndian.Uint32(c.b[c.i : c.i+c.size]))
		c.i += c.size
	}
}

func (c *Uint32) appendInts(values []int) {
	for _, v := range values {
		c.writerData = append(c.writerData,
			byte(v),
			byte(v>>8),
			byte(v>>16),
			byte(v>>24),
		)
	}
}
