package column

import (
	"io"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

// Tuple use for Tuple ClickHouse DataTypes
type Tuple struct {
	column
	columns []Column
}

// NewTuple return new Tuple for Tuple ClickHouse DataTypes
func NewTuple(columns ...Column) *Tuple {
	m := &Tuple{
		column: column{
			size: 0,
		},
		columns: columns,
	}
	for _, col := range columns {
		col.setParent(m)
	}

	return m
}

// ReadRaw read raw data from the reader. it runs automatically when you call `NextColumn()`
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
func (c *Tuple) HeaderReader(r *readerwriter.Reader) error {
	err := c.column.HeaderReader(r)
	if err != nil {
		return err
	}
	for _, col := range c.columns {
		err = col.HeaderReader(r)
		if err != nil {
			return err
		}
	}
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
