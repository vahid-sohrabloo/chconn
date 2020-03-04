package chconn

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"reflect"
	"unsafe"
)

func NewWriter() *Writer {
	return &Writer{
		output: &bytes.Buffer{},
	}
}

type Writer struct {
	output  *bytes.Buffer
	offset  uint64
	scratch [binary.MaxVarintLen64]byte
}

// Uvarint write a uint64 into writer and get error
func (w *Writer) Uvarint(v uint64) error {
	ln := binary.PutUvarint(w.scratch[:binary.MaxVarintLen64], v)
	return w.Write(w.scratch[:ln])
}

// Varint write a int64 into writer and get error
func (w *Writer) Varint(v int64) error {
	ln := binary.PutVarint(w.scratch[:binary.MaxVarintLen64], v)
	return w.Write(w.scratch[:ln])
}

func (w *Writer) Bool(v bool) error {
	if v {
		return w.Uint8(1)
	}
	return w.Uint8(0)
}

func (w *Writer) Int8(v int8) error {
	return w.Uint8(uint8(v))
}

func (w *Writer) Int16(v int16) error {
	return w.Uint16(uint16(v))
}

func (w *Writer) Int32(v int32) error {
	return w.Uint32(uint32(v))
}

func (w *Writer) Int64(v int64) error {
	return w.Uint64(uint64(v))
}

func (w *Writer) Uint8(v uint8) error {
	w.scratch[0] = v
	return w.Write(w.scratch[:1])
}

func (w *Writer) Uint16(v uint16) error {
	w.scratch[0] = byte(v)
	w.scratch[1] = byte(v >> 8)
	return w.Write(w.scratch[:2])
}

func (w *Writer) Uint32(v uint32) error {
	w.scratch[0] = byte(v)
	w.scratch[1] = byte(v >> 8)
	w.scratch[2] = byte(v >> 16)
	w.scratch[3] = byte(v >> 24)
	return w.Write(w.scratch[:4])
}

func (w *Writer) Uint64(v uint64) error {
	w.scratch[0] = byte(v)
	w.scratch[1] = byte(v >> 8)
	w.scratch[2] = byte(v >> 16)
	w.scratch[3] = byte(v >> 24)
	w.scratch[4] = byte(v >> 32)
	w.scratch[5] = byte(v >> 40)
	w.scratch[6] = byte(v >> 48)
	w.scratch[7] = byte(v >> 56)
	return w.Write(w.scratch[:8])
}

func (w *Writer) Float32(v float32) error {
	return w.Uint32(math.Float32bits(v))
}

func (w *Writer) Float64(v float64) error {
	return w.Uint64(math.Float64bits(v))
}

func (w *Writer) AddOffset(v uint64) error {
	w.offset += v
	return w.Uint64(w.offset)
}

func (w *Writer) String(v string) error {
	str := str2Bytes(v)
	if err := w.Uvarint(uint64(len(str))); err != nil {
		return err
	}
	return w.Write(str)
}

func (w *Writer) Buffer(str []byte) error {
	if err := w.Uvarint(uint64(len(str))); err != nil {
		return err
	}
	return w.Write(str)
}

func (w *Writer) Write(b []byte) error {
	_, err := w.output.Write(b)
	return err
}

func (w *Writer) WriteTo(wt io.Writer) (int64, error) {
	return w.output.WriteTo(wt)
}

func (w *Writer) Reset() {
	w.output.Reset()
}

func str2Bytes(str string) []byte {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&str))
	header.Len = len(str)
	header.Cap = header.Len
	return *(*[]byte)(unsafe.Pointer(header))
}
