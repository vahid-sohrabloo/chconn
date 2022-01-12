package readerwriter

import (
	"bytes"
	"encoding/binary"
	"io"
	"reflect"
	"unsafe"
)

// Writer is a helper to write data into bytes.Buffer
type Writer struct {
	output  *bytes.Buffer
	scratch [binary.MaxVarintLen64]byte
}

// NewWriter get new writer
func NewWriter() *Writer {
	return &Writer{
		output: &bytes.Buffer{},
	}
}

// Uvarint write a variable uint64 value into writer
func (w *Writer) Uvarint(v uint64) {
	ln := binary.PutUvarint(w.scratch[:binary.MaxVarintLen64], v)
	w.Write(w.scratch[:ln])
}

// Bool write bool value
func (w *Writer) Bool(v bool) {
	if v {
		w.Uint8(1)
		return
	}
	w.Uint8(0)
}

// Int32 write Int32 value
func (w *Writer) Int32(v int32) {
	w.Uint32(uint32(v))
}

// Int64 write Int64 value
func (w *Writer) Int64(v int64) {
	w.Uint64(uint64(v))
}

// Uint8 write Uint8 value
func (w *Writer) Uint8(v uint8) {
	w.output.WriteByte(v)
}

// Uint16 write Uint16 value
func (w *Writer) Uint16(v uint16) {
	w.scratch[0] = byte(v)
	w.scratch[1] = byte(v >> 8)
	w.Write(w.scratch[:2])
}

// Uint32 write Uint32 value
func (w *Writer) Uint32(v uint32) {
	w.scratch[0] = byte(v)
	w.scratch[1] = byte(v >> 8)
	w.scratch[2] = byte(v >> 16)
	w.scratch[3] = byte(v >> 24)
	w.Write(w.scratch[:4])
}

// Uint64 write Uint64 value
func (w *Writer) Uint64(v uint64) {
	w.scratch[0] = byte(v)
	w.scratch[1] = byte(v >> 8)
	w.scratch[2] = byte(v >> 16)
	w.scratch[3] = byte(v >> 24)
	w.scratch[4] = byte(v >> 32)
	w.scratch[5] = byte(v >> 40)
	w.scratch[6] = byte(v >> 48)
	w.scratch[7] = byte(v >> 56)
	w.Write(w.scratch[:8])
}

// String write string
func (w *Writer) String(v string) {
	str := str2Bytes(v)
	w.Uvarint(uint64(len(str)))
	w.Write(str)
}

// Write write raw []byte data
func (w *Writer) Write(b []byte) {
	w.output.Write(b)
}

// WriteTo implement WriteTo
func (w *Writer) WriteTo(wt io.Writer) (int64, error) {
	return w.output.WriteTo(wt)
}

// Reset reset all data
func (w *Writer) Reset() {
	w.output.Reset()
}

// Output get raw *bytes.Buffer
func (w *Writer) Output() *bytes.Buffer {
	return w.output
}

func str2Bytes(str string) []byte {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&str))
	header.Len = len(str)
	header.Cap = header.Len
	return *(*[]byte)(unsafe.Pointer(header))
}
