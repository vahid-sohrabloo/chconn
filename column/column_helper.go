package column

import (
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

type ColumnBasic interface {
	ReadRaw(num int, r *readerwriter.Reader) error
	HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error
	HeaderWriter(*readerwriter.Writer)
	WriteTo(io.Writer) (int64, error)
	NumRow() int
	Reset()
	SetType(v []byte)
	Type() []byte
	SetName(v []byte)
	Name() []byte
	Validate(forInsert bool) error
	structType() string
	chconnType() string
	SetWriteBufferSize(int)
	RowAny(int) any
	Scan(row int, dest any) error
	AppendAny(any) error
	canAppend(any) bool
	FullType() string
	Remove(n int)
	ToJSON(row int, stringQuotes bool, b []byte) []byte
	setLocationInParent(locationInParent int)
	setVariantParent(p *Variant)
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
	RowIsNil(row int) bool
}

type column struct {
	r                *readerwriter.Reader
	b                []byte
	totalByte        int
	name             []byte
	chType           []byte
	LocationInParent uint8
	variantParent    *Variant
	hasVariantParent bool
}

func (c *column) readColumn(readColumn bool, revision uint64) error {
	if !readColumn {
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

func (c *column) setVariantParent(p *Variant) {
	c.variantParent = p
	c.hasVariantParent = true
}

func (c *column) setLocationInParent(locationInParent int) {
	c.LocationInParent = uint8(locationInParent)
}

func (c *column) preHookAppend() {
	if c.hasVariantParent {
		c.variantParent.AppendDiscriminators(c.LocationInParent)
	}
}

// todo find a more efficient way
func (c *column) preHookAppendMulti(n int) {
	if c.hasVariantParent {
		for i := 0; i < n; i++ {
			c.variantParent.AppendDiscriminators(c.LocationInParent)
		}
	}
}
