package readerwriter

import (
	"encoding/binary"
	"io"
	"math"
)

// Reader is a helper to read data from reader
type Reader struct {
	mainReader     io.Reader
	input          io.Reader
	compressReader io.Reader
	offset         uint64
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

// Bool read variable uint64 value
func (r *Reader) Uvarint() (uint64, error) {
	return binary.ReadUvarint(r)
}

// Int8 read Int8 value
func (r *Reader) Int8() (int8, error) {
	v, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	return int8(v), nil
}

// Int16 read Int16 value
func (r *Reader) Int16() (int16, error) {
	v, err := r.Uint16()
	if err != nil {
		return 0, err
	}
	return int16(v), nil
}

// Int32 read Int32 value
func (r *Reader) Int32() (int32, error) {
	v, err := r.Uint32()
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

// Int64 read Int64 value
func (r *Reader) Int64() (int64, error) {
	v, err := r.Uint64()
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

// Uint8 read Uint8 value
func (r *Reader) Uint8() (uint8, error) {
	v, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	return v, nil
}

// Uint16 read Uint16 value
func (r *Reader) Uint16() (uint16, error) {
	if _, err := io.ReadFull(r.input, r.scratch[:2]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(r.scratch[:2]), nil
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

// Float32 read Float32 value
func (r *Reader) Float32() (float32, error) {
	v, err := r.Uint32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(v), nil
}

// Float64 read Float64 value
func (r *Reader) Float64() (float64, error) {
	v, err := r.Uint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
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

// ByteArray read string  value as []byte
func (r *Reader) ByteArray() ([]byte, error) {
	strlen, err := r.Uvarint()
	if err != nil {
		return nil, err
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

// Len read current len of array and get last offeset
func (r *Reader) Len() (arrayLen int, lastOffset uint64, err error) {
	offset, err := r.Uint64()
	if err != nil {
		return 0, 0, err
	}
	arrLen := int(offset - r.offset)
	r.offset = offset
	return arrLen, offset, nil
}

// ResetOffset reset offset of array len offset
func (r *Reader) ResetOffset() {
	r.offset = 0
}
