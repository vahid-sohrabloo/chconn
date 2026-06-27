package format

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// BytesReader reads Native-format blocks directly from an in-memory buffer
// (typically an mmap the caller owns), aliasing column data with zero copies.
// The zero-copy path assumes each column has an empty serialization header,
// which is true for Base[T] and String columns. It is therefore suitable only
// for those types; columns whose HeaderWriter emits bytes are not supported —
// use OpenFile for such blocks. Returned columns and the strings they yield are
// valid only while the backing buffer is alive and unmodified.
type BytesReader struct {
	data []byte
	off  int
}

// OpenBytes wraps data for zero-copy reads. The caller owns data's lifetime.
func OpenBytes(data []byte) *BytesReader { return &BytesReader{data: data} }

// ReadBlock reads the next block, aliasing each column into the buffer.
// Returns io.EOF when the buffer is exhausted.
func (br *BytesReader) ReadBlock() (int, []column.ColumnCore, error) {
	if br.off >= len(br.data) {
		return 0, nil, io.EOF
	}
	numCols, err := br.uvarint()
	if err != nil {
		return 0, nil, err
	}
	if numCols > maxColumns {
		return 0, nil, fmt.Errorf("native: implausible column count %d", numCols)
	}
	numRows, err := br.uvarint()
	if err != nil {
		return 0, nil, fmt.Errorf("native: read num_rows: %w", err)
	}

	cols := make([]column.ColumnCore, 0, numCols)
	for i := uint64(0); i < numCols; i++ {
		name, err := br.bstring()
		if err != nil {
			return 0, nil, err
		}
		chType, err := br.bstring()
		if err != nil {
			return 0, nil, err
		}
		chTypeCopy := append([]byte(nil), chType...)
		col, err := column.ColumnByType(chTypeCopy, 0, false, false, "")
		if err != nil {
			return 0, nil, fmt.Errorf("native: column %q: %w", name, err)
		}
		zc, ok := col.(column.ZeroCopyColumn)
		if !ok {
			return 0, nil, fmt.Errorf("native: column %q (%s) does not support zero-copy reads; use OpenFile",
				name, chType)
		}
		col.SetName(append([]byte(nil), name...))
		consumed, err := zc.ReadFromBytes(int(numRows), br.data[br.off:])
		if err != nil {
			return 0, nil, fmt.Errorf("native: zero-copy read %q: %w", name, err)
		}
		br.off += consumed
		cols = append(cols, col)
	}
	return int(numRows), cols, nil
}

func (br *BytesReader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(br.data[br.off:])
	if n <= 0 {
		return 0, fmt.Errorf("native: bad varint at offset %d", br.off)
	}
	br.off += n
	return v, nil
}

func (br *BytesReader) bstring() ([]byte, error) {
	l, err := br.uvarint()
	if err != nil {
		return nil, err
	}
	if l > uint64(len(br.data)-br.off) {
		return nil, io.ErrUnexpectedEOF
	}
	end := br.off + int(l)
	if end > len(br.data) {
		return nil, io.ErrUnexpectedEOF
	}
	s := br.data[br.off:end]
	br.off = end
	return s, nil
}
