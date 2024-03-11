package readerwriter

import (
	"fmt"

	"github.com/go-faster/city"
)

// Method is compression codec.
type CompressMethod byte

const (
	// ChecksumSize is 128bits for cityhash102 checksum
	ChecksumSize = 16
	// CompressHeaderSize magic + compressed_size + uncompressed_size
	CompressHeaderSize = 1 + 4 + 4

	// HeaderSize for compress header
	HeaderSize = ChecksumSize + CompressHeaderSize
	// BlockMaxSize 128MB
	BlockMaxSize = 1024 * 1024 * 128
)

// Possible compression methods.
const (
	CompressNone     CompressMethod = 0x00
	CompressChecksum CompressMethod = 0x02
	CompressLZ4      CompressMethod = 0x82
	CompressZSTD     CompressMethod = 0x90
)

// Constants for compression encoding.
//
// See https://go-faster.org/docs/clickhouse/compression for reference.
const (
	checksumSize       = 16
	compressHeaderSize = 1 + 4 + 4
	headerSize         = checksumSize + compressHeaderSize

	// Limiting total data/block size to protect from possible OOM.
	maxDataSize  = 1024 * 1024 * 2 // 2MB
	maxBlockSize = maxDataSize

	hRawSize  = 17
	hDataSize = 21
	hMethod   = 16
)

// CorruptedDataErr means that provided hash mismatch with calculated.
type CorruptedDataErr struct {
	Actual    city.U128
	Reference city.U128
	RawSize   int
	DataSize  int
}

func (c *CorruptedDataErr) Error() string {
	return fmt.Sprintf("corrupted data: %d (actual), %d (reference), compressed size: %d, data size: %d",
		c.Actual.High, c.Reference.High, c.RawSize, c.DataSize,
	)
}
