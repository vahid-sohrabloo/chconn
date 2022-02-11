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
	HeaderReader(*readerwriter.Reader, bool) error
	IsNullable() bool
	setNullable(nullable bool)
	AppendEmpty()
	GetEmpty() []byte
	Name() []byte
	Type() []byte
	SetName(v []byte)
	SetType(v []byte)
	setParent(parent Column)
	HasParent() bool
	ValueIsNil() bool
	RowIsNil(int) bool
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
			return fmt.Errorf("read nullable data: %w", err)
		}
	}
	err := c.readBuffer()
	if err != nil {
		err = fmt.Errorf("read data: %w", err)
	}
	return err
}

func (c *column) readColumn(readColumn bool) error {
	if c.parent != nil || !readColumn {
		return nil
	}
	strLen, err := c.r.Uvarint()
	if err != nil {
		return fmt.Errorf("read column name length: %w", err)
	}
	if cap(c.name) < int(strLen) {
		c.name = make([]byte, strLen)
	} else {
		c.name = c.name[:strLen]
	}
	_, err = c.r.Read(c.name)
	if err != nil {
		return fmt.Errorf("read column name: %w", err)
	}

	strLen, err = c.r.Uvarint()
	if err != nil {
		return fmt.Errorf("read column type length: %w", err)
	}
	if cap(c.chType) < int(strLen) {
		c.chType = make([]byte, strLen)
	} else {
		c.chType = c.chType[:strLen]
	}
	_, err = c.r.Read(c.chType)
	if err != nil {
		return fmt.Errorf("read column type: %w", err)
	}
	return nil
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
func (c *column) HeaderReader(r *readerwriter.Reader, readColumn bool) error {
	c.r = r
	return c.readColumn(readColumn)
}

// WriteTo write data clickhouse
// it uses internally
func (c *column) WriteTo(w io.Writer) (int64, error) {
	var n int64
	if c.nullable {
		if len(c.colNullable.writerData) != c.NumRow() {
			return 0,
				//nolint:goerr113
				fmt.Errorf("mismatch write data: nullable row %d != column row %d", c.colNullable.NumRow(), c.NumRow())
		}
		var err error
		n, err = c.colNullable.WriteTo(w)
		if err != nil {
			return n, fmt.Errorf("write nullable data: %w", err)
		}
	}
	nw, err := w.Write(c.writerData)
	return int64(nw) + n, err
}

// IsNullable check if the column is nullable or not
func (c *column) IsNullable() bool {
	return c.nullable
}

func (c *column) setNullable(nullable bool) {
	c.nullable = nullable
}

// ValueIsNil check if the current value is nil or not
func (c *column) ValueIsNil() bool {
	return c.colNullable.b[(c.i-c.size)/(c.size)] == 1
}

// ValueIsNil check if the current value is nil or not
func (c *column) RowIsNil(i int) bool {
	return c.nullable && c.colNullable.b[i] == 1
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

// SetName set name of the column
func (c *column) SetName(v []byte) {
	c.name = v
}

// SetType set clickhouse type
func (c *column) SetType(v []byte) {
	c.chType = v
}

func (c *column) setParent(parent Column) {
	c.parent = parent
}

// AppendEmpty append empty value for insert
func (c *column) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

// GetEmpty return empty value for insert
func (c *column) GetEmpty() []byte {
	return emptyByte[:c.size]
}

// HasParent check if the column has parent or not
func (c *column) HasParent() bool {
	return c.parent != nil
}
