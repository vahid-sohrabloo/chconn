package column

import (
	"fmt"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

type ColumnBasic interface {
	ReadRaw(num int, r *readerwriter.Reader) error
	HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error
	HeaderWriter(*readerwriter.Writer)
	Write(*readerwriter.Writer)
	NumRow() int
	Reset()
	SetType(v []byte)
	Type() []byte
	SetName(v []byte)
	Name() []byte
	Validate() error
	ColumnType() string
	SetWriteBufferSize(int)
	RowI(int) any
	Scan(row int, dest any) error
	FullType() string
}

type Column[T any] interface {
	ColumnBasic
	Data() []T
	Read([]T) []T
	Row(int) T
	Append(T)
	AppendMulti(...T)
}

type NullableColumn[T any] interface {
	Column[T]
	DataP() []*T
	ReadP([]*T) []*T
	RowP(int) *T
	AppendP(*T)
	AppendMultiP(...*T)
}

type column struct {
	r         *readerwriter.Reader
	b         []byte
	totalByte int
	name      []byte
	chType    []byte
	parent    ColumnBasic
}

func (c *column) readColumn(readColumn bool, revision uint64) error {
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

	if revision >= helper.DbmsMinProtocolWithCustomSerialization {
		hasCustomSerialization, err := c.r.ReadByte()
		if err != nil {
			return fmt.Errorf("read custom serialization: %w", err)
		}
		// todo check with json object
		if hasCustomSerialization == 1 {
			return fmt.Errorf("custom serialization not supported")
		}
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
