package readerwriter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

var ErrHeaderDecompressEOF = errors.New("lz4 decompression header EOF")
var ErrDecompressSizeError = errors.New("decompress read size not match")

type invalidCompressErr struct {
	method byte
}

func (e *invalidCompressErr) Error() string {
	return fmt.Sprintf("unknown compression method: 0x%02x ", e.method)
}

type compressReader struct {
	reader io.Reader
	// data uncompressed
	data []byte
	// data position
	pos int
	// data compressed
	zdata []byte
	// lz4 headers
	header []byte
}

// NewCompressReader wrap the io.Reader
func NewCompressReader(r io.Reader) *compressReader {
	p := &compressReader{
		reader: r,
		header: make([]byte, HeaderSize),
	}
	p.data = make([]byte, BlockMaxSize)

	zlen := lz4.CompressBlockBound(BlockMaxSize) + HeaderSize
	p.zdata = make([]byte, zlen)

	p.pos = len(p.data)
	return p
}

func (cr *compressReader) Read(buf []byte) (n int, err error) {
	var bytesRead = 0
	n = len(buf)

	if cr.pos < len(cr.data) {
		copyedSize := copy(buf, cr.data[cr.pos:])

		bytesRead += copyedSize
		cr.pos += copyedSize
	}

	for bytesRead < n {
		if err := cr.readCompressedData(); err != nil {
			return bytesRead, err
		}
		copyedSize := copy(buf[bytesRead:], cr.data)

		bytesRead += copyedSize
		cr.pos = copyedSize
	}
	return n, nil
}

func (cr *compressReader) readCompressedData() (err error) {
	cr.pos = 0
	var n int
	n, err = cr.reader.Read(cr.header)
	if err != nil {
		return
	}
	if n != len(cr.header) {
		return ErrHeaderDecompressEOF
	}

	compressedSize := int(binary.LittleEndian.Uint32(cr.header[17:])) - 9
	decompressedSize := int(binary.LittleEndian.Uint32(cr.header[21:]))
	if compressedSize > cap(cr.zdata) {
		cr.zdata = make([]byte, compressedSize)
	}
	if decompressedSize > cap(cr.data) {
		cr.data = make([]byte, decompressedSize)
	}

	cr.zdata = cr.zdata[:compressedSize]
	cr.data = cr.data[:decompressedSize]

	// @TODO checksum
	if cr.header[16] == LZ4 {
		n, err = cr.reader.Read(cr.zdata)
		if err != nil {
			return
		}

		if n != len(cr.zdata) {
			return ErrDecompressSizeError
		}

		_, err = lz4.UncompressBlock(cr.zdata, cr.data)
		if err != nil {
			return
		}
	} else {
		return &invalidCompressErr{cr.header[16]}
	}

	return nil
}
