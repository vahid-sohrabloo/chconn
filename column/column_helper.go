package column

import (
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
)

type ColumnBasic interface {
	ReadRaw(num int, r *readerwriter.Reader) error
	HeaderReader(*readerwriter.Reader, bool) error
	HeaderWriter(*readerwriter.Writer)
	WriteTo(io.Writer) (int64, error)
	NumRow() int
	Reset()
	SetType(v []byte)
	Type() []byte
	SetName(v []byte)
	Name() []byte
	Validate() error
	columnType() string
	SetWriteBufferSize(int)
}

type Column[T any] interface {
	ColumnBasic
	Data() []T
	Read([]T) []T
	Row(int) T
	Append(...T)
}

type NullableColumn[T any] interface {
	Column[T]
	DataP() []*T
	ReadP([]*T) []*T
	RowP(int) *T
	AppendP(...*T)
}

type column struct {
	r         *readerwriter.Reader
	b         []byte
	totalByte int
	name      []byte
	chType    []byte
	parent    ColumnBasic
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
