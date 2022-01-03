package column

import (
	"fmt"
	"io"
	"math"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

const (
	// Need to read additional keys.
	// Additional keys are stored before indexes as value N and N keys
	// after them.
	hasAdditionalKeysBit = 1 << 9
	// Need to update dictionary.
	// It means that previous granule has different dictionary.
	needUpdateDictionary = 1 << 10

	serializationType = hasAdditionalKeysBit | needUpdateDictionary
)

type lcDictColumn interface {
	Column
	Keys() []int
}

type LC struct {
	r          *readerwriter.Reader
	dictColumn lcDictColumn
	numRow     int
	indices    indicesColumn
	scratch    [8]byte
}

var _ Column = &LC{}

func NewLC(dictColumn lcDictColumn) *LC {
	if dictColumn.isNullable() {
		dictColumn.AppendEmpty()
	}
	return &LC{
		dictColumn: dictColumn,
	}
}

func (c *LC) ReadRaw(num int, r *readerwriter.Reader) error {
	c.r = r
	c.numRow = num

	serializationType, err := c.r.Uint64()
	if err != nil {
		return err
	}
	intType := serializationType & 0xf

	dictionarySize, err := c.r.Uint64()
	if err != nil {
		return err
	}
	nullable := c.dictColumn.isNullable()
	// disable nullable for low cardinality dictionary
	c.dictColumn.setNullable(false)
	err = c.dictColumn.ReadRaw(int(dictionarySize), r)
	c.dictColumn.setNullable(nullable)
	if err != nil {
		return err
	}

	indicesSize, err := r.Uint64()
	c.numRow = int(indicesSize)
	if err != nil {
		return err
	}

	switch intType {
	case 0:
		c.indices = NewUint8(false)
	case 1:
		c.indices = NewUint16(false)
	case 2:
		c.indices = NewUint32(false)
	case 3:
		panic("cannot handle this amout of data fo lc")
	}

	return c.indices.ReadRaw(c.numRow, c.r)
}

func (c *LC) Next() bool {
	return c.indices.Next()
}

func (c *LC) Value() int {
	return c.indices.valueInt()
}

func (c *LC) ReadAll(value *[]int) {
	c.indices.readAllInt(value)
}

func (c *LC) Fill(value []int) {
	c.indices.fillInt(value)
}

func (c *LC) NumRow() int {
	return len(c.dictColumn.Keys())
}

func (c *LC) HeaderReader(r *readerwriter.Reader) error {
	// write KeysSerializationVersion. for more information see clickhouse docs
	_, err := r.Uint64()
	return err
}
func (c *LC) HeaderWriter(w *readerwriter.Writer) {
	// write KeysSerializationVersion. for more information see clickhouse docs
	w.Int64(1)
}

func (c *LC) WriteTo(w io.Writer) (int64, error) {
	var n int64
	dictionarySize := c.dictColumn.NumRow()
	intType := int(math.Log2(float64(dictionarySize)) / 8)
	stype := serializationType | intType

	nw, err := c.writeUint64(w, uint64(stype))
	n += int64(nw)
	if err != nil {
		return n, fmt.Errorf("error writing stype: %w", err)
	}

	nw, err = c.writeUint64(w, uint64(dictionarySize))
	n += int64(nw)
	if err != nil {
		return n, fmt.Errorf("error writing dictionarySize: %w", err)
	}

	nwd, err := c.dictColumn.WriteTo(w)
	n += nwd
	if err != nil {
		return n, fmt.Errorf("error writing dictionary: %w", err)
	}
	keys := c.dictColumn.Keys()
	nw, err = c.writeUint64(w, uint64(len(keys)))
	n += int64(nw)
	if err != nil {
		return n, fmt.Errorf("error writing keys: %w", err)
	}

	switch intType {
	case 0:
		c.indices = NewUint8(false)
	case 1:
		c.indices = NewUint16(false)
	case 2:
		c.indices = NewUint32(false)
	case 3:
		panic("cannot handle this amout of data fo lc")
	}
	c.indices.appendInts(keys)
	c.dictColumn.resetDict()
	nwt, err := c.indices.WriteTo(w)
	if err != nil {
		return n, fmt.Errorf("error writing indices: %w", err)
	}
	return n + nwt, err
}

// Uint64 write Uint64 value
func (c *LC) writeUint64(w io.Writer, v uint64) (int, error) {
	c.scratch[0] = byte(v)
	c.scratch[1] = byte(v >> 8)
	c.scratch[2] = byte(v >> 16)
	c.scratch[3] = byte(v >> 24)
	c.scratch[4] = byte(v >> 32)
	c.scratch[5] = byte(v >> 40)
	c.scratch[6] = byte(v >> 48)
	c.scratch[7] = byte(v >> 56)
	return w.Write(c.scratch[:8])
}

func (c *LC) isNullable() bool {
	// low cardinality column cannot be nullable
	return false
}
func (c *LC) setNullable(nullable bool) {
	// low cardinality column cannot be nullable
}

func (c *LC) AppendEmpty() {
}

func (c *LC) resetDict() {
}
