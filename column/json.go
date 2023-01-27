package column

import (
	"io"

	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

type JSONString struct {
	column
	numRow  int
	columns []ColumnBasic
	encoded uint8
}

func NewJSONString() *JSONString {
	a := &JSONString{}
	return a
}

func (c *JSONString) AppendString(data string) {
	if c.columns == nil {
		c.columns = []ColumnBasic{
			NewString(),
		}
	}
	c.encoded = 1
	c.columns[0].(*String).Append(data)
}

func (c *JSONString) AppendBytes(data []byte) {
	if c.columns == nil {
		c.columns = []ColumnBasic{
			NewString(),
		}
	}
	c.encoded = 1
	c.columns[0].(*String).AppendBytes(data)
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *JSONString) Reset() {
	c.numRow = 0
	c.encoded = 0
	for _, v := range c.columns {
		v.Reset()
	}
}

// SetWriteBufferSize set write buffer (number of bytes)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *JSONString) SetWriteBufferSize(b int) {
	// todo
}

// todo
func (c *JSONString) Validate() error {
	return nil
}

// todo
func (c *JSONString) RowI(row int) any {
	return nil
}

// todo
func (c *JSONString) Scan(row int, dest any) error {
	return nil
}

func (c *JSONString) ReadRaw(num int, r *readerwriter.Reader) error {
	// todo
	c.Reset()
	c.r = r
	c.numRow = num
	return nil
}

// NumRow return number of row for this block
func (c *JSONString) NumRow() int {
	return c.columns[0].NumRow()
}

// HeaderReader reads header data from read
// it uses internally
func (c *JSONString) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	// todo
	_, err := r.ReadByte()
	if err != nil {
		return err
	}
	c.r = r
	return c.readColumn(readColumn, revision)
}

// HeaderWriter write header
// it uses internally
func (c *JSONString) HeaderWriter(w *readerwriter.Writer) {
	w.Uint8(c.encoded)
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *JSONString) WriteTo(w io.Writer) (int64, error) {
	var nw int64
	for _, v := range c.columns {
		n, err := v.WriteTo(w)
		nw += n
		if err != nil {
			return nw, err
		}
	}
	return nw, nil
}

func (c *JSONString) ColumnType() string {
	return "Object('JSON')"
}

func (c *JSONString) FullType() string {
	return "Object('JSON')"
}
