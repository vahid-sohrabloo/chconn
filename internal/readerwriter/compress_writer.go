package readerwriter

// copy from https://github.com/ClickHouse/ch-go/blob/4cde4e4bec24211c0bcdc6f385f4212d0ad522d9/compress/writer.go
// some changes to compatible with chconn

import (
	"encoding/binary"
	"fmt"

	"github.com/go-faster/city"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type CompressWriter struct {
	// data uncompressed
	Data []byte
	// compression method
	method CompressMethod

	lz4  *lz4.Compressor
	zstd *zstd.Encoder
}

// NewCompressWriter wrap the io.Writer
func NewCompressWriter(method byte) *CompressWriter {
	p := &CompressWriter{
		method: CompressMethod(method),
	}
	if p.method == CompressLZ4 {
		p.lz4 = &lz4.Compressor{}
	} else if p.method == CompressZSTD {
		p.zstd, _ = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(1),
			zstd.WithLowerEncoderMem(true),
		)
	}
	return p
}

// Compress buf into Data.
func (cw *CompressWriter) Compress(buf []byte) error {
	maxSize := lz4.CompressBlockBound(len(buf))
	cw.Data = append(cw.Data[:0], make([]byte, maxSize+headerSize)...)
	_ = cw.Data[:headerSize]
	cw.Data[hMethod] = byte(cw.method)

	var n int

	switch cw.method {
	case CompressLZ4:
		compressedSize, err := cw.lz4.CompressBlock(buf, cw.Data[headerSize:])
		if err != nil {
			return fmt.Errorf("lz4 compress error: %w", err)
		}
		n = compressedSize
	case CompressZSTD:
		cw.Data = cw.zstd.EncodeAll(buf, cw.Data[:headerSize])
		n = len(cw.Data) - headerSize
	case CompressNone:
		n = copy(cw.Data[headerSize:], buf)
	}

	cw.Data = cw.Data[:n+headerSize]

	binary.LittleEndian.PutUint32(cw.Data[hRawSize:], uint32(n+compressHeaderSize))
	binary.LittleEndian.PutUint32(cw.Data[hDataSize:], uint32(len(buf)))
	h := city.CH128(cw.Data[hMethod:])
	binary.LittleEndian.PutUint64(cw.Data[0:8], h.Low)
	binary.LittleEndian.PutUint64(cw.Data[8:16], h.High)

	return nil
}
