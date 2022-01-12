package readerwriter

import (
	"encoding/binary"
	"io"
)

// Reader is a helper to read data from reader
type Reader struct {
	mainReader     io.Reader
	input          io.Reader
	compressReader io.Reader
	scratch        [binary.MaxVarintLen64]byte
}

// NewReader get new Reader
func NewReader(input io.Reader) *Reader {
	return &Reader{
		input:          input,
		mainReader:     input,
		compressReader: NewCompressReader(input),
	}
}

// SetCompress set compress status
func (r *Reader) SetCompress(c bool) {
	if c {
		r.input = r.compressReader
		return
	}
	r.input = r.mainReader
}

// Bool read bool value
func (r *Reader) Bool() (bool, error) {
	v, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	return v == 1, nil
}

// Uvarint read variable uint64 value
func (r *Reader) Uvarint() (uint64, error) {
	return binary.ReadUvarint(r)
}

// Int32 read Int32 value
func (r *Reader) Int32() (int32, error) {
	v, err := r.Uint32()
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

// Uint32 read Uint32 value
func (r *Reader) Uint32() (uint32, error) {
	if _, err := io.ReadFull(r.input, r.scratch[:4]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(r.scratch[:4]), nil
}

// Uint64 read Uint64 value
func (r *Reader) Uint64() (uint64, error) {
	if _, err := io.ReadFull(r.input, r.scratch[:8]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(r.scratch[:8]), nil
}

// FixedString read FixedString value
func (r *Reader) FixedString(strlen int) ([]byte, error) {
	buf := make([]byte, strlen)

	_, err := io.ReadFull(r, buf)
	return buf, err
}

// String read String value
func (r *Reader) String() (string, error) {
	strlen, err := r.Uvarint()
	if err != nil {
		return "", err
	}
	str, err := r.FixedString(int(strlen))
	if err != nil {
		return "", err
	}
	return string(str), nil
}

// ByteString read string  value as []byte
func (r *Reader) ByteString() ([]byte, error) {
	strlen, err := r.Uvarint()
	if err != nil {
		return nil, err
	}
	if strlen == 0 {
		return []byte{}, nil
	}
	return r.FixedString(int(strlen))
}

// ReadByte read a single byte
func (r *Reader) ReadByte() (byte, error) {
	if _, err := r.input.Read(r.scratch[:1]); err != nil {
		return 0, err
	}
	return r.scratch[0], nil
}

// Read  implement Read
func (r *Reader) Read(buf []byte) (int, error) {
	return io.ReadFull(r.input, buf)
}
