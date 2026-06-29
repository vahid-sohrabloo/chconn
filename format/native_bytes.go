package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

// BytesReader reads Native-format blocks directly from an in-memory buffer
// (typically a heap buffer from decompression), aliasing column data with zero copies
// for Base[T] and String columns (which implement ZeroCopyColumn). For all other
// column types (LowCardinality, Nullable, Array, Map, Tuple, etc.) it falls back to
// a streaming read over the same buffer without copying it. Returned columns and the
// strings they yield are valid only while the backing buffer is alive and unmodified.
type BytesReader struct {
	data []byte
	off  int
}

// OpenBytes wraps data for zero-copy reads. The caller owns data's lifetime.
func OpenBytes(data []byte) *BytesReader { return &BytesReader{data: data} }

// ReadBlock reads the next block, aliasing each ZeroCopyColumn into the buffer
// and streaming-reading all other column types without copying the buffer.
// Returns io.EOF when the buffer is exhausted.
func (br *BytesReader) ReadBlock() (int, []column.ColumnCore, error) {
	if br.off >= len(br.data) {
		return 0, nil, io.EOF
	}
	numRows, cols, consumed, err := readBlockFromBytes(br.data[br.off:], nil)
	if err != nil {
		return 0, nil, err
	}
	br.off += consumed
	return numRows, cols, nil
}

// readBlockFromBytes parses one Native-format block from the beginning of plain.
//
// If into is nil, columns are built from the type strings in the block header. For
// columns implementing ZeroCopyColumn (Base[T], String) the returned column aliases
// plain directly (zero copy). For all others a bytes.Reader over the remaining slice
// is used for streaming without copying the whole buffer.
//
// If into is non-nil, its elements are reused in order (SetColumnHeader is called
// from the stream; count must match). The same ZeroCopy / fallback dispatch applies.
//
// Returns (numRows, cols, bytesConsumed, err). cols is nil when into is non-nil
// (the caller already holds the populated slice). On error, consumed may be 0.
func readBlockFromBytes(plain []byte, into []column.ColumnCore) (numRows int, cols []column.ColumnCore, consumed int, err error) {
	br := &BytesReader{data: plain}

	numCols, err := br.uvarint()
	if err != nil {
		return 0, nil, 0, err
	}
	if numCols > maxColumns {
		return 0, nil, 0, fmt.Errorf("native: implausible column count %d", numCols)
	}
	if into != nil && int(numCols) != len(into) {
		return 0, nil, 0, fmt.Errorf("native: expected %d columns, got %d in stream", len(into), numCols)
	}

	nRows, err := br.uvarint()
	if err != nil {
		return 0, nil, 0, fmt.Errorf("native: read num_rows: %w", err)
	}
	if nRows > uint64(math.MaxInt) {
		return 0, nil, 0, fmt.Errorf("native: row count %d exceeds int range", nRows)
	}

	serverInfo := shared.EmptyServerInfo()

	var buildCols []column.ColumnCore
	if into == nil {
		buildCols = make([]column.ColumnCore, 0, numCols)
	}

	for i := range numCols {
		name, err := br.bstring()
		if err != nil {
			return 0, nil, 0, err
		}
		chType, err := br.bstring()
		if err != nil {
			return 0, nil, 0, err
		}
		nameCopy := append([]byte(nil), name...)
		chTypeCopy := append([]byte(nil), chType...)

		var col column.ColumnCore
		if into != nil {
			col = into[i]
		} else {
			col, err = column.ColumnByType(chTypeCopy, 0, false, false, "")
			if err != nil {
				return 0, nil, 0, fmt.Errorf("native: column %q: %w", string(name), err)
			}
		}

		// Always set the column header: for into!=nil this validates the type match;
		// for into==nil this ensures name and inner-type metadata are consistent.
		if err := col.SetColumnHeader(column.ColumnHeader{Name: nameCopy, ChType: chTypeCopy}); err != nil {
			return 0, nil, 0, fmt.Errorf("native: set column header %q: %w", string(nameCopy), err)
		}

		if zc, ok := col.(column.ZeroCopyColumn); ok {
			// Zero-copy alias: Base[T] and String columns alias plain directly.
			consumed, err := zc.ReadFromBytes(int(nRows), plain[br.off:])
			if err != nil {
				return 0, nil, 0, fmt.Errorf("native: zero-copy read %q: %w", string(nameCopy), err)
			}
			br.off += consumed
		} else {
			// Streaming fallback for LowCardinality, Nullable, Array, Map, etc.
			// Wrap the remaining slice without copying the whole buffer.
			// readerwriter.Reader has no read-ahead, so bytesRdr.Len() is exact.
			remaining := plain[br.off:]
			bytesRdr := bytes.NewReader(remaining)
			r := readerwriter.NewReader(bytesRdr)
			if err := col.ReadHeader(r, serverInfo); err != nil {
				return 0, nil, 0, fmt.Errorf("native: read header for column %q: %w", string(nameCopy), err)
			}
			if nRows > 0 {
				if err := col.ReadRaw(int(nRows)); err != nil {
					return 0, nil, 0, fmt.Errorf("native: read data for column %q: %w", string(nameCopy), err)
				}
			}
			br.off += len(remaining) - bytesRdr.Len()
		}

		if into == nil {
			buildCols = append(buildCols, col)
		}
	}

	return int(nRows), buildCols, br.off, nil
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
