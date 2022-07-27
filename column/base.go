package column

import (
	"fmt"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
)

// Column use for most (fixed size) ClickHouse Columns type
type Base[T comparable] struct {
	column
	size   int
	numRow int
	values []T
	params []interface{}
}

// New create a new column
func New[T comparable]() *Base[T] {
	var tmpValue T
	size := int(unsafe.Sizeof(tmpValue))
	return &Base[T]{
		size: size,
	}
}

// Data get all the data in current block as a slice.
//
// NOTE: the return slice only valid in current block, if you want to use it after, you should copy it. or use Read
func (c *Base[T]) Data() []T {
	value := *(*[]T)(unsafe.Pointer(&c.b))
	return value[:c.numRow]
}

// Read reads all the data in current block and append to the input.
func (c *Base[T]) Read(value []T) []T {
	v := *(*[]T)(unsafe.Pointer(&c.b))
	return append(value, v[:c.numRow]...)
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Base[T]) Row(row int) T {
	i := row * c.size
	return *(*T)(unsafe.Pointer(&c.b[i]))
}

// Append value for insert
func (c *Base[T]) Append(v T) {
	c.numRow++
	c.values = append(c.values, v)
}

// AppendSlice append slice of value for insert
func (c *Base[T]) AppendSlice(v []T) {
	if len(v) == 0 {
		return
	}
	c.values = append(c.values, v...)
	c.numRow += len(v)
}

// NumRow return number of row for this block
func (c *Base[T]) NumRow() int {
	return c.numRow
}

// Array return a Array type for this column
func (c *Base[T]) Array() *Array[T] {
	return NewArray[T](c)
}

// Nullable return a nullable type for this column
func (c *Base[T]) Nullable() *Nullable[T] {
	return NewNullable[T](c)
}

// LC return a low cardinality type for this column
func (c *Base[T]) LC() *LowCardinality[T] {
	return NewLC[T](c)
}

// LowCardinality return a low cardinality type for this column
func (c *Base[T]) LowCardinality() *LowCardinality[T] {
	return NewLowCardinality[T](c)
}

// appendEmpty append empty value for insert
// this use internally for nullable and low cardinality nullable column
func (c *Base[T]) appendEmpty() {
	var emptyValue T
	c.Append(emptyValue)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *Base[T]) Reset() {
	c.numRow = 0
	c.values = c.values[:0]
}

// SetWriteBuffer set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *Base[T]) SetWriteBuffer(row int) {
	if cap(c.values) < row {
		c.values = make([]T, 0, row)
	}
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *Base[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
	c.r = r
	c.numRow = num
	c.totalByte = num * c.size
	err := c.readBuffer()
	if err != nil {
		err = fmt.Errorf("read data: %w", err)
	}
	c.readyBufferHook()
	return err
}

func (c *Base[T]) readBuffer() error {
	if cap(c.b) < c.totalByte {
		c.b = make([]byte, c.totalByte)
	} else {
		c.b = c.b[:c.totalByte]
	}
	_, err := c.r.Read(c.b)
	return err
}

// HeaderReader reads header data from reader
// it uses internally
func (c *Base[T]) HeaderReader(r *readerwriter.Reader, readColumn bool) error {
	c.r = r
	return c.readColumn(readColumn)
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *Base[T]) HeaderWriter(w *readerwriter.Writer) {
}

func (c *Base[T]) Elem(arrayLevel int, nullable, lc bool) ColumnBasic {
	if nullable {
		return c.Nullable().elem(arrayLevel, lc)
	}
	if lc {
		return c.LowCardinality().elem(arrayLevel)
	}
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}
