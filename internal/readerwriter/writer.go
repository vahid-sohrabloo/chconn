package readerwriter

import (
	"encoding/binary"
)

// Writer is a helper to write data into bytes.Buffer
type Writer struct {
	Output  []byte
	scratch [binary.MaxVarintLen64]byte
}

// NewWriter get new writer
func NewWriter() *Writer {
	return &Writer{}
}

// Uvarint write a variable uint64 value into writer
func (w *Writer) Uvarint(v uint64) {
	ln := binary.PutUvarint(w.scratch[:binary.MaxVarintLen64], v)
	w.Output = append(w.Output, w.scratch[:ln]...)
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
	w.Output = append(w.Output, v)
}

// Uint32 write Uint32 value
func (w *Writer) Uint32(v uint32) {
	w.scratch[0] = byte(v)
	w.scratch[1] = byte(v >> 8)
	w.scratch[2] = byte(v >> 16)
	w.scratch[3] = byte(v >> 24)
	w.Output = append(w.Output,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
	)
}

// Uint64 write Uint64 value
func (w *Writer) Uint64(v uint64) {
	w.Output = append(w.Output,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
		byte(v>>32),
		byte(v>>40),
		byte(v>>48),
		byte(v>>56),
	)
}

// String write string
func (w *Writer) String(v string) {
	w.Uvarint(uint64(len(v)))
	w.Output = append(w.Output, v...)
}

// ByteString write []byte
func (w *Writer) ByteString(v []byte) {
	w.Uvarint(uint64(len(v)))
	w.Output = append(w.Output, v...)
}

// Reset reset all data
func (w *Writer) Reset() {
	w.Output = w.Output[:0]
}
