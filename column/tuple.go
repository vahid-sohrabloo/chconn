package column

import (
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/v2/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
)

// Tuple is a column of Tuple(T1,T2,.....,Tn) ClickHouse data type
//
// this is actually a group of columns. it doesn't have any method for read or write data
//
// You MUST use this on Select and Insert methods and for append and read data use the sub columns
type Tuple struct {
	column
	columns []ColumnBasic
}

// NewTuple create a new tuple of Tuple(T1,T2,.....,Tn) ClickHouse data type
//
// this is actually a group of columns. it doesn't have any method for read or write data
//
// You MUST use this on Select and Insert methods and for append and read data use the sub columns
func NewTuple(columns ...ColumnBasic) *Tuple {
	if len(columns) < 1 {
		panic("tuple must have at least one column")
	}
	return &Tuple{
		columns: columns,
	}
}

// NumRow return number of row for this block
func (c *Tuple) NumRow() int {
	return c.columns[0].NumRow()
}

// Array return a Array type for this column
func (c *Tuple) Array() *ArrayBase {
	return NewArrayBase(c)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *Tuple) Reset() {
	for _, col := range c.columns {
		col.Reset()
	}
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *Tuple) SetWriteBufferSize(row int) {
	for _, col := range c.columns {
		col.SetWriteBufferSize(row)
	}
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *Tuple) ReadRaw(num int, r *readerwriter.Reader) error {
	for i, col := range c.columns {
		err := col.ReadRaw(num, r)
		if err != nil {
			return fmt.Errorf("tuple: read column index %d: %w", i, err)
		}
	}
	return nil
}

// HeaderReader reads header data from reader.
// it uses internally
func (c *Tuple) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	c.r = r
	err := c.readColumn(readColumn, revision)
	if err != nil {
		return err
	}

	for i, col := range c.columns {
		err = col.HeaderReader(r, false, revision)
		if err != nil {
			return fmt.Errorf("tuple: read column header index %d: %w", i, err)
		}
	}

	return nil
}

// Column returns the all sub columns
func (c *Tuple) Columns() []ColumnBasic {
	return c.columns
}

func (c *Tuple) Validate() error {
	chType := helper.FilterSimpleAggregate(c.chType)
	if helper.IsPoint(chType) {
		chType = helper.PointMainTypeStr
	}

	if !helper.IsTuple(chType) {
		return ErrInvalidType{
			column: c,
		}
	}

	columnsTuple, err := helper.TypesInParentheses(chType[helper.LenTupleStr : len(chType)-1])
	if err != nil {
		return fmt.Errorf("tuple invalid types %w", err)
	}
	if len(columnsTuple) != len(c.columns) {
		//nolint:goerr113
		return fmt.Errorf("columns number for %s (%s) is not equal to tuple columns number: %d != %d",
			string(c.name),
			string(c.Type()),
			len(columnsTuple),
			len(c.columns),
		)
	}

	for i, col := range c.columns {
		col.SetType(columnsTuple[i].ChType)
		col.SetName(columnsTuple[i].Name)
		if col.Validate() != nil {
			return ErrInvalidType{
				column: c,
			}
		}
	}
	return nil
}

func (c *Tuple) ColumnType() string {
	str := helper.TupleStr
	for _, col := range c.columns {
		str += col.ColumnType() + ","
	}
	return str[:len(str)-1] + ")"
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *Tuple) WriteTo(w io.Writer) (int64, error) {
	var n int64
	for i, col := range c.columns {
		nw, err := col.WriteTo(w)
		if err != nil {
			return n, fmt.Errorf("tuple: write column index %d: %w", i, err)
		}
		n += nw
	}
	return n, nil
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *Tuple) HeaderWriter(w *readerwriter.Writer) {
	for _, col := range c.columns {
		col.HeaderWriter(w)
	}
}

func (c *Tuple) Elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}
