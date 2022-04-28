package column

import (
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

// Tuple use for Tuple ClickHouse DataTypes
type Tuple struct {
	column
	columns       []Column
	needSetChtype bool
}

// NewTuple return new Tuple for Tuple ClickHouse DataTypes
func NewTuple(columns ...Column) *Tuple {
	m := &Tuple{
		column: column{
			size: 0,
		},
		needSetChtype: true,
		columns:       columns,
	}
	for _, col := range columns {
		col.setParent(m)
	}

	return m
}

// ReadRaw read raw data from the reader. it runs automatically when you call `ReadColumns()`
func (c *Tuple) ReadRaw(num int, r *readerwriter.Reader) error {
	var err error
	for _, col := range c.columns {
		err = col.ReadRaw(num, r)
		if err != nil {
			return err
		}
	}
	return err
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *Tuple) HeaderWriter(w *readerwriter.Writer) {
	for _, col := range c.columns {
		col.HeaderWriter(w)
	}
}

// HeaderReader reads header data from read
// it uses internally
func (c *Tuple) HeaderReader(r *readerwriter.Reader, readColumn bool) error {
	err := c.column.HeaderReader(r, readColumn)
	if err != nil {
		return err
	}
	var columnsTuple [][]byte
	if c.needSetChtype {
		var openFunc int
		cur := 0
		// for between `Tuple(` and `)`
		idx := 1
		tupleTypes := c.chType[6 : len(c.chType)-1]

		for i, char := range tupleTypes {
			if char == ',' {
				if openFunc == 0 {
					columnsTuple = append(columnsTuple, tupleTypes[cur:i])
					idx++
					cur = i + 2
				}
				continue
			}
			if char == '(' {
				openFunc++
				continue
			}
			if char == ')' {
				openFunc--
				continue
			}
		}
		columnsTuple = append(columnsTuple, tupleTypes[cur:])
		if len(columnsTuple) != len(c.columns) {
			//nolint:goerr113
			return fmt.Errorf("columns number is not equal to tuple columns number: %d != %d", len(columnsTuple), len(c.columns))
		}
	}

	for i, col := range c.columns {
		err = col.HeaderReader(r, readColumn)
		if err != nil {
			return err
		}
		if c.needSetChtype {
			col.SetType(columnsTuple[i])
		}
	}
	c.needSetChtype = false

	return err
}

// WriteTo write data clickhouse
// it uses internally
func (c *Tuple) WriteTo(w io.Writer) (int64, error) {
	var n int64
	for _, col := range c.columns {
		nw, err := col.WriteTo(w)
		n += nw
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// NumRow return number of keys for this block
func (c *Tuple) NumRow() int {
	// todo: find a way to validate all columns number
	return c.columns[0].NumRow()
}

// Columns return all columns of tuple
func (c *Tuple) Columns() []Column {
	return c.columns
}
