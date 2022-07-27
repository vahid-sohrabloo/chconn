package column

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/vahid-sohrabloo/chconn/v2/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
)

// ArrayBase is a column of Array(T) ClickHouse data type
//
// ArrayBase is a base class for other arrays or use for none generic use
type ArrayBase struct {
	column
	offsetColumn *Base[uint64]
	dataColumn   ColumnBasic
	offset       uint64
	resetHook    func()
}

// NewArray create a new array column of Array(T) ClickHouse data type
func NewArrayBase(dataColumn ColumnBasic) *ArrayBase {
	a := &ArrayBase{
		dataColumn:   dataColumn,
		offsetColumn: New[uint64](),
	}
	return a
}

// AppendLen Append len of array for insert
func (c *ArrayBase) AppendLen(v int) {
	c.offset += uint64(v)
	c.offsetColumn.Append(c.offset)
}

// NumRow return number of row for this block
func (c *ArrayBase) NumRow() int {
	return c.offsetColumn.NumRow()
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *ArrayBase) Reset() {
	c.offsetColumn.Reset()
	c.dataColumn.Reset()
	c.offset = 0
}

// Offsets return all the offsets in current block
// Note: Only available in the current block
func (c *ArrayBase) Offsets() []uint64 {
	return c.offsetColumn.Data()
}

// TotalRows return total rows on this block of array data
func (c *ArrayBase) TotalRows() int {
	if c.offsetColumn.totalByte == 0 {
		return 0
	}
	return int(binary.LittleEndian.Uint64(c.offsetColumn.b[c.offsetColumn.totalByte-8 : c.offsetColumn.totalByte]))
}

// SetWriteBuffer set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *ArrayBase) SetWriteBuffer(row int) {
	c.offsetColumn.SetWriteBuffer(row)
	c.dataColumn.SetWriteBuffer(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *ArrayBase) ReadRaw(num int, r *readerwriter.Reader) error {
	c.offsetColumn.Reset()
	err := c.offsetColumn.ReadRaw(num, r)
	if err != nil {
		return fmt.Errorf("array: read offset column: %w", err)
	}
	err = c.dataColumn.ReadRaw(c.TotalRows(), r)
	if err != nil {
		return fmt.Errorf("array: read data column: %w", err)
	}

	if c.resetHook != nil {
		c.resetHook()
	}
	return nil
}

// HeaderReader reads header data from reader
// it uses internally
func (c *ArrayBase) HeaderReader(r *readerwriter.Reader, readColumn bool) error {
	c.r = r
	err := c.readColumn(readColumn)
	if err != nil {
		return err
	}

	// never return error
	//nolint:errcheck
	c.offsetColumn.HeaderReader(r, false)

	return c.dataColumn.HeaderReader(r, false)
}

func (c *ArrayBase) Validate() error {
	chType := helper.FilterSimpleAggregate(c.chType)

	if helper.IsRing(chType) {
		chType = helper.RingMainTypeStr
	} else if helper.IsPolygon(chType) {
		chType = helper.PolygonMainTypeStr
	} else if helper.IsMultiPolygon(chType) {
		chType = helper.MultiPolygonMainTypeStr
	}

	if !helper.IsArray(chType) {
		return ErrInvalidType{
			column: c,
		}
	}
	c.dataColumn.SetType(chType[helper.LenArrayStr : len(chType)-1])
	if c.dataColumn.Validate() != nil {
		return ErrInvalidType{
			column: c,
		}
	}
	return nil
}

func (c *ArrayBase) columnType() string {
	return strings.Replace(helper.ArrayTypeStr, "<type>", c.dataColumn.columnType(), -1)
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *ArrayBase) WriteTo(w io.Writer) (int64, error) {
	nw, err := c.offsetColumn.WriteTo(w)
	if err != nil {
		return 0, fmt.Errorf("write len data: %w", err)
	}
	n, errDataColumn := c.dataColumn.WriteTo(w)

	return nw + n, errDataColumn
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *ArrayBase) HeaderWriter(w *readerwriter.Writer) {
	c.dataColumn.HeaderWriter(w)
}
