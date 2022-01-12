package readerwriter

const (
	// NONE compression type
	NONE = 0x02
	// LZ4 compression type
	LZ4 = 0x82
	// ZSTD compression type
	ZSTD = 0x90
)

const (
	// ChecksumSize is 128bits for cityhash102 checksum
	ChecksumSize = 16
	// CompressHeaderSize magic + compressed_size + uncompressed_size
	CompressHeaderSize = 1 + 4 + 4

	// HeaderSize for compress header
	HeaderSize = ChecksumSize + CompressHeaderSize
	// BlockMaxSize 1MB
	BlockMaxSize = 1 << 20
)
