package column

import (
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

type Column interface {
	ReadRaw(num int, r *readerwriter.Reader) error
	NumRow() int
	WriteTo(io.Writer) (int64, error)
	HeaderWriter(*readerwriter.Writer)
	HeaderReader(*readerwriter.Reader) error
	isNullable() bool
	setNullable(nullable bool)
	AppendEmpty()
	resetDict()
}

type column struct {
	r           *readerwriter.Reader
	b           []byte
	i           int
	iNull       int
	numRow      int
	totalByte   int
	size        int
	writerData  []byte
	nullable    bool
	colNullable *nullable
}

func (c *column) ReadRaw(num int, r *readerwriter.Reader) error {
	c.reset()
	c.r = r
	c.numRow = num
	c.totalByte = num * c.size
	if c.nullable {
		err := c.colNullable.ReadRaw(num, r)
		if err != nil {
			return err
		}
	}
	return c.readBuffer()
}

func (c *column) reset() {
	c.i = 0
	c.numRow = 0
	c.writerData = c.writerData[:0]
	if c.nullable {
		c.colNullable.reset()
	}
}

func (c *column) readBuffer() error {
	if c.size == 0 {
		return nil
	}
	if cap(c.b) < c.totalByte {
		c.b = make([]byte, c.totalByte)
	} else {
		c.b = c.b[:c.totalByte]
	}
	_, err := c.r.Read(c.b)
	return err
}

func (c *column) NumRow() int {
	return c.numRow
}
func (c *column) AppendIsNil(v bool) {
	if v {
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
}

func (c *column) HeaderWriter(w *readerwriter.Writer) {
}
func (c *column) HeaderReader(*readerwriter.Reader) error {
	return nil
}

func (c *column) WriteTo(w io.Writer) (int64, error) {
	var n int64
	if c.nullable {
		var err error
		n, err = c.colNullable.WriteTo(w)
		if err != nil {
			return n, fmt.Errorf("write nullable data: %w", err)
		}
	}
	nw, err := w.Write(c.writerData)
	c.reset()
	return int64(nw) + n, err
}

func (c *column) isNullable() bool {
	return c.nullable
}

func (c *column) setNullable(nullable bool) {
	c.nullable = nullable
}

func (c *column) resetDict() {
}

func (c *column) ReadAllNil(value *[]uint8) {
	*value = append(*value, c.colNullable.b...)
}

func (c *column) FillNil(value []uint8) {
	copy(value, c.colNullable.b[c.iNull:c.iNull+len(value)])
	c.iNull += len(value)
}
