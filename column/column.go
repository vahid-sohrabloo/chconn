package column

import (
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

// Column is a interface for column
type Column interface {
	ReadRaw(num int, r *readerwriter.Reader) error
	NumRow() int
	WriteTo(io.Writer) (int64, error)
	Reset()
	HeaderWriter(*readerwriter.Writer)
	HeaderReader(*readerwriter.Reader) error
	isNullable() bool
	setNullable(nullable bool)
	AppendEmpty()
	Name() []byte
	Type() []byte
	setParent(parent Column)
	getParent() Column
}

type column struct {
	r             *readerwriter.Reader
	b             []byte
	i             int
	iNull         int
	numRow        int
	totalByte     int
	size          int
	writerData    []byte
	nullable      bool
	colNullable   *nullable
	name          []byte
	chType        []byte
	parent        Column
	ownReadBuffer bool
}

// ReadRaw read raw data from the reader. it runs automatically when you call `NextColumn()`
func (c *column) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
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

func (c *column) readColumn() error {
	if c.parent != nil {
		return nil
	}
	strLen, err := c.r.Uvarint()
	if err != nil {
		return err
	}
	if cap(c.name) < int(strLen) {
		c.name = make([]byte, strLen)
	} else {
		c.name = c.name[:strLen]
	}
	_, err = c.r.Read(c.name)
	if err != nil {
		return err
	}

	strLen, err = c.r.Uvarint()
	if err != nil {
		return err
	}
	if cap(c.chType) < int(strLen) {
		c.chType = make([]byte, strLen)
	} else {
		c.chType = c.chType[:strLen]
	}
	_, err = c.r.Read(c.chType)
	if err != nil {
		return err
	}

	return err
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *column) Reset() {
	c.i = 0
	c.numRow = 0
	c.iNull = 0
	c.writerData = c.writerData[:0]
	if c.nullable {
		c.colNullable.Reset()
	}
}

func (c *column) readBuffer() error {
	if c.ownReadBuffer {
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

// NumRow return number of row for this block
func (c *column) NumRow() int {
	return c.numRow
}

// AppendIsNil determine if current append value is null or not (for nullable columns)
//
// Use with `Append` and `AppendEmpty` for nullable columns
func (c *column) AppendIsNil(v bool) {
	if v {
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *column) HeaderWriter(w *readerwriter.Writer) {
}

// HeaderReader reads header data from read
// it uses internally
func (c *column) HeaderReader(r *readerwriter.Reader) error {
	c.r = r
	return c.readColumn()
}

// WriteTo write data clickhouse
// it uses internally
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
	return int64(nw) + n, err
}

func (c *column) isNullable() bool {
	return c.nullable
}

func (c *column) setNullable(nullable bool) {
	c.nullable = nullable
}

// ValueIsNil check if the current value is nil or not
func (c *column) ValueIsNil() bool {
	return c.colNullable.b[(c.i-c.size)/(c.size)] == 1
}

// ReadAll read all nils state in this block and append to the input slice
// NOTE: only use for nullable columns
func (c *column) ReadAllNil(value *[]uint8) {
	*value = append(*value, c.colNullable.b...)
}

// Fill slice with state and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass and only use.
func (c *column) FillNil(value []uint8) {
	copy(value, c.colNullable.b[c.iNull:c.iNull+len(value)])
	c.iNull += len(value)
}

// Name get name of the column
func (c *column) Name() []byte {
	return c.name
}

// Type get clickhouse type
func (c *column) Type() []byte {
	return c.chType
}

func (c *column) setParent(parent Column) {
	c.parent = parent
}

func (c *column) getParent() Column {
	return c.parent
}

// AppendEmpty append empty value for insert
func (c *column) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}
