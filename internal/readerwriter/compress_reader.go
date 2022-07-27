package readerwriter

// copy from https://github.com/ClickHouse/ch-go/blob/4cde4e4bec24211c0bcdc6f385f4212d0ad522d9/compress/reader.go
// some changes to compatible with chconn
import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/go-faster/city"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type invalidCompressErr struct {
	method CompressMethod
}

func (e *invalidCompressErr) Error() string {
	return fmt.Sprintf("unknown compression method: 0x%02x ", e.method)
}

type compressReader struct {
	reader io.Reader
	data   []byte
	pos    int64
	raw    []byte
	header []byte
	zstd   *zstd.Decoder
}

// NewCompressReader wrap the io.Reader
func NewCompressReader(r io.Reader) io.Reader {
	return &compressReader{
		zstd:   nil, // lazily initialized
		reader: r,
		header: make([]byte, headerSize),
	}
}

func (r *compressReader) Read(buf []byte) (n int, err error) {
	if r.pos >= int64(len(r.data)) {
		if err := r.readBlock(); err != nil {
			return 0, fmt.Errorf("read block: %w", err)
		}
	}
	n = copy(buf, r.data[r.pos:])
	r.pos += int64(n)
	return n, nil
}

// readBlock reads next compressed data into raw and decompresses into data.
func (r *compressReader) readBlock() error {
	r.pos = 0

	_ = r.header[headerSize-1]
	if _, err := io.ReadFull(r.reader, r.header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	var (
		rawSize  = int(binary.LittleEndian.Uint32(r.header[hRawSize:])) - compressHeaderSize
		dataSize = int(binary.LittleEndian.Uint32(r.header[hDataSize:]))
	)
	if dataSize < 0 || dataSize > maxDataSize {
		return fmt.Errorf("data size should be %d < %d < %d", 0, dataSize, maxDataSize)
	}
	if rawSize < 0 || rawSize > maxBlockSize {
		return fmt.Errorf("raw size should be %d < %d < %d", 0, rawSize, maxBlockSize)
	}

	r.data = append(r.data[:0], make([]byte, dataSize)...)
	r.raw = append(r.raw[:0], r.header...)
	r.raw = append(r.raw, make([]byte, rawSize)...)
	_ = r.raw[:rawSize+headerSize-1]

	if _, err := io.ReadFull(r.reader, r.raw[headerSize:]); err != nil {
		return fmt.Errorf("read raw: %w", err)
	}
	hGot := city.U128{
		Low:  binary.LittleEndian.Uint64(r.raw[0:8]),
		High: binary.LittleEndian.Uint64(r.raw[8:16]),
	}
	h := city.CH128(r.raw[hMethod:])
	if hGot != h {
		return &CorruptedDataErr{
			Actual:    h,
			Reference: hGot,
			RawSize:   rawSize,
			DataSize:  dataSize,
		}
	}
	//nolint:exhaustive
	switch m := CompressMethod(r.header[hMethod]); m {
	case CompressLZ4:
		n, err := lz4.UncompressBlock(r.raw[headerSize:], r.data)
		if err != nil {
			return fmt.Errorf("lz4 decompress: %w", err)
		}
		if n != dataSize {
			return fmt.Errorf("unexpected uncompressed data size: %d (actual) != %d (got in header)",
				n, dataSize,
			)
		}
	case CompressZSTD:
		if r.zstd == nil {
			// Lazily initializing to prevent spawning goroutines in NewReader.
			// See https://github.com/golang/go/issues/47056#issuecomment-997436820
			zstdReader, err := zstd.NewReader(nil,
				zstd.WithDecoderConcurrency(1),
				zstd.WithDecoderLowmem(true),
			)
			if err != nil {
				return fmt.Errorf("zstd new: %w", err)
			}
			r.zstd = zstdReader
		}
		data, err := r.zstd.DecodeAll(r.raw[headerSize:], r.data[:0])
		if err != nil {
			return fmt.Errorf("zstd decompress: %w", err)
		}
		if len(data) != dataSize {
			return fmt.Errorf("unexpected uncompressed data size: %d (actual) != %d (got in header)",
				len(data), dataSize,
			)
		}
		r.data = data
	case CompressChecksum:
		copy(r.data, r.raw[headerSize:])
	default:
		return &invalidCompressErr{m}
	}

	return nil
}
