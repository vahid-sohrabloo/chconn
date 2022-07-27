package readerwriter

// copy from https://github.com/ClickHouse/ch-go/blob/4cde4e4bec24211c0bcdc6f385f4212d0ad522d9/compress/writer.go
// some changes to compatible with chconn

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/go-faster/city"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type compressWriter struct {
	writer io.Writer
	// data uncompressed
	data []byte
	// data position
	pos int
	// data compressed
	zdata []byte
	// compression method
	method CompressMethod

	lz4  *lz4.Compressor
	zstd *zstd.Encoder
}

// NewCompressWriter wrap the io.Writer
func NewCompressWriter(w io.Writer, method byte) io.Writer {
	p := &compressWriter{
		writer: w,
		method: CompressMethod(method),
		data:   make([]byte, maxBlockSize),
	}
	return p
}

func (cw *compressWriter) Write(buf []byte) (int, error) {
	var n int
	for len(buf) > 0 {
		// Accumulate the data to be compressed.
		m := copy(cw.data[cw.pos:], buf)
		cw.pos += m
		buf = buf[m:]
		if cw.pos == len(cw.data) {
			err := cw.Flush()
			if err != nil {
				return n, err
			}
		}
		n += m
	}

	return n, nil
}

// Compress buf into Data.
func (w *compressWriter) Flush() error {
	if w.pos == 0 {
		return nil
	}
	maxSize := lz4.CompressBlockBound(len(w.data[:w.pos]))
	w.zdata = append(w.zdata[:0], make([]byte, maxSize+headerSize)...)
	_ = w.zdata[:headerSize]
	w.zdata[hMethod] = byte(w.method)

	var n int
	switch w.method {
	case CompressLZ4:
		if w.lz4 == nil {
			w.lz4 = &lz4.Compressor{}
		}
		compressedSize, err := w.lz4.CompressBlock(w.data[:w.pos], w.zdata[headerSize:])
		if err != nil {
			return fmt.Errorf("lz4 compress error: %v", err)
		}
		n = compressedSize
	case CompressZSTD:
		if w.zstd == nil {
			zw, err := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.SpeedDefault),
				zstd.WithEncoderConcurrency(1),
				zstd.WithLowerEncoderMem(true),
			)
			if err != nil {
				return fmt.Errorf("zstd new error: %v", err)
			}
			w.zstd = zw
		}
		w.zdata = w.zstd.EncodeAll(w.data[:w.pos], w.zdata[:headerSize])
		n = len(w.zdata) - headerSize
	case CompressChecksum:
		n = copy(w.zdata[headerSize:], w.data[:w.pos])
	}

	w.zdata = w.zdata[:n+headerSize]

	binary.LittleEndian.PutUint32(w.zdata[hRawSize:], uint32(n+compressHeaderSize))
	binary.LittleEndian.PutUint32(w.zdata[hDataSize:], uint32(w.pos))
	h := city.CH128(w.zdata[hMethod:])
	binary.LittleEndian.PutUint64(w.zdata[0:8], h.Low)
	binary.LittleEndian.PutUint64(w.zdata[8:16], h.High)

	_, err := w.writer.Write(w.zdata)
	w.pos = 0
	return err
}
