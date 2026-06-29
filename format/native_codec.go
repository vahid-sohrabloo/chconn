package format

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	kpzstd "github.com/klauspost/compress/zstd"
	lz4 "github.com/pierrec/lz4/v4"
)

// maxCompressedUnit is a sanity bound for one compressed unit length (8 GiB).
// Streams with a declared length exceeding this are rejected as corrupt or crafted.
const maxCompressedUnit = 1 << 33

// Codec is a pluggable one-shot compression codec. Compress appends the
// compressed form of src to dst; Decompress appends the decompressed form to dst
// (callers pass a dst pre-sized to the recorded uncompressed length). Both MUST
// be safe for concurrent use if the codec is used with parallel dir-mode writes.
type Codec struct {
	Name       string
	Compress   func(dst, src []byte) ([]byte, error)
	Decompress func(dst, src []byte) ([]byte, error)
}

// shared klauspost encoder/decoder; EncodeAll/DecodeAll are concurrency-safe.
var kpEnc, _ = kpzstd.NewWriter(nil, kpzstd.WithEncoderConcurrency(1))
var kpDec, _ = kpzstd.NewReader(nil, kpzstd.WithDecoderConcurrency(1))

func zstdCodec() *Codec {
	return &Codec{
		Name:       "zstd",
		Compress:   func(dst, src []byte) ([]byte, error) { return kpEnc.EncodeAll(src, dst), nil },
		Decompress: func(dst, src []byte) ([]byte, error) { return kpDec.DecodeAll(src, dst) },
	}
}

func lz4Codec() *Codec {
	return &Codec{
		Name: "lz4",
		Compress: func(dst, src []byte) ([]byte, error) {
			var buf bytes.Buffer
			w := lz4.NewWriter(&buf)
			if _, err := w.Write(src); err != nil {
				return nil, err
			}
			if err := w.Close(); err != nil {
				return nil, err
			}
			return append(dst, buf.Bytes()...), nil
		},
		Decompress: func(dst, src []byte) ([]byte, error) {
			r := lz4.NewReader(bytes.NewReader(src))
			var buf bytes.Buffer
			if _, err := io.Copy(&buf, r); err != nil {
				return nil, err
			}
			return append(dst, buf.Bytes()...), nil
		},
	}
}

// compressedMagic prefixes every compressed native file so an uncompressed
// reader fails fast and a codec-name mismatch can be detected.
var compressedMagic = [4]byte{'C', 'N', 'C', '1'}

// writeCompressedHeader writes the magic bytes followed by the codec name as a
// uvarint-length-prefixed string.
func writeCompressedHeader(w io.Writer, name string) error {
	if _, err := w.Write(compressedMagic[:]); err != nil {
		return err
	}
	hdr := binary.AppendUvarint(nil, uint64(len(name)))
	hdr = append(hdr, name...)
	_, err := w.Write(hdr)
	return err
}

// readCompressedHeader verifies the magic bytes and that the stored codec name
// matches wantName.
func readCompressedHeader(r io.Reader, wantName string) error {
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return fmt.Errorf("native: read compressed magic: %w", err)
	}
	if magic != compressedMagic {
		return fmt.Errorf("native: not a chconn compressed native file (bad magic %v)", magic)
	}
	nameLen, err := readUvarint(r)
	if err != nil {
		return fmt.Errorf("native: read codec name length: %w", err)
	}
	name := make([]byte, nameLen)
	if _, err := io.ReadFull(r, name); err != nil {
		return fmt.Errorf("native: read codec name: %w", err)
	}
	if string(name) != wantName {
		return fmt.Errorf("native: codec mismatch: file written with %q, reader configured with %q", string(name), wantName)
	}
	return nil
}

// writeCompressedUnit compresses plaintext and writes a unit:
// uvarint(compLen), uvarint(uncompLen), then compLen compressed bytes.
func writeCompressedUnit(w io.Writer, codec *Codec, plaintext []byte) error {
	comp, err := codec.Compress(nil, plaintext)
	if err != nil {
		return fmt.Errorf("native: compress: %w", err)
	}
	hdr := binary.AppendUvarint(nil, uint64(len(comp)))
	hdr = binary.AppendUvarint(hdr, uint64(len(plaintext)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if _, err := w.Write(comp); err != nil {
		return err
	}
	return nil
}

// readCompressedUnit reads one compressed unit and returns its decompressed
// plaintext. The error from reading the first length is returned raw so io.EOF
// propagates to signal end-of-stream.
func readCompressedUnit(r io.Reader, codec *Codec) ([]byte, error) {
	compLen, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	uncompLen, err := readUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("native: read uncompressed length: %w", err)
	}
	if compLen > maxCompressedUnit || uncompLen > maxCompressedUnit {
		return nil, fmt.Errorf("native: compressed unit length out of range (comp=%d uncomp=%d)", compLen, uncompLen)
	}
	comp := make([]byte, compLen)
	if _, err := io.ReadFull(r, comp); err != nil {
		return nil, fmt.Errorf("native: read compressed unit: %w", err)
	}
	plain, err := codec.Decompress(make([]byte, 0, uncompLen), comp)
	if err != nil {
		return nil, fmt.Errorf("native: decompress: %w", err)
	}
	if uint64(len(plain)) != uncompLen {
		return nil, fmt.Errorf("native: decompressed length %d, expected %d", len(plain), uncompLen)
	}
	return plain, nil
}

// readUvarint reads a base-128 varint one byte at a time so a clean io.EOF on
// the first byte propagates to the caller (used to detect end-of-stream).
func readUvarint(r io.Reader) (uint64, error) {
	var x uint64
	var s uint
	var buf [1]byte
	for i := 0; ; i++ {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			if i > 0 && errors.Is(err, io.EOF) {
				return x, io.ErrUnexpectedEOF
			}
			return x, err
		}
		b := buf[0]
		if b < 0x80 {
			if i > 9 || (i == 9 && b > 1) {
				return x, fmt.Errorf("native: uvarint overflows uint64")
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}
