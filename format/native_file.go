package format

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// FileWriter writes one or more Native-format blocks to an io.Writer.
type FileWriter struct {
	w             io.Writer
	nw            *NativeWriter
	codec         *Codec
	headerWritten bool
}

// NewFileWriter wraps w. WriteBlock may be called repeatedly (multiple blocks).
// Pass WithZSTD()/WithLZ4()/WithCodec() to compress each block as a unit.
func NewFileWriter(w io.Writer, opts ...Option) *FileWriter {
	cfg := resolve(opts)
	fw := &FileWriter{w: w, codec: cfg.codec}
	if cfg.codec == nil {
		fw.nw = NewNativeWriter(w)
	}
	return fw
}

// WriteBlock writes one block; all columns must have equal NumRow.
func (fw *FileWriter) WriteBlock(cols ...column.ColumnCore) error {
	if fw.codec == nil {
		return fw.nw.WriteBlock(cols...)
	}
	if !fw.headerWritten {
		if err := writeCompressedHeader(fw.w, fw.codec.Name); err != nil {
			return err
		}
		fw.headerWritten = true
	}
	var buf bytes.Buffer
	if err := NewNativeWriter(&buf).WriteBlock(cols...); err != nil {
		return err
	}
	return writeCompressedUnit(fw.w, fw.codec, buf.Bytes())
}

// WriteFile writes cols as a single-block Native file at path.
// The write is atomic: data goes to path+".tmp" and is renamed to path only on
// full success. Any error removes the temp file before returning.
// Pass WithZSTD()/WithLZ4()/WithCodec() to write a compressed file.
func WriteFile(path string, cols []column.ColumnCore, opts ...Option) error {
	for _, col := range cols {
		if len(col.Type()) == 0 {
			return fmt.Errorf("native: column %q has no type; call SetType before writing", col.Name())
		}
	}
	cfg := resolve(opts)
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	fail := func(err error) error {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if cfg.codec == nil {
		if err := NewNativeWriter(f).WriteBlock(cols...); err != nil {
			return fail(err)
		}
	} else {
		if err := writeCompressedHeader(f, cfg.codec.Name); err != nil {
			return fail(err)
		}
		var buf bytes.Buffer
		if err := NewNativeWriter(&buf).WriteBlock(cols...); err != nil {
			return fail(err)
		}
		if err := writeCompressedUnit(f, cfg.codec, buf.Bytes()); err != nil {
			return fail(err)
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// FileReader reads Native-format blocks from a file.
type FileReader struct {
	f          *os.File
	r          io.Reader
	nr         *NativeReader
	codec      *Codec
	headerRead bool
}

// OpenFile opens a Native file for streaming reads.
// Pass WithZSTD()/WithLZ4()/WithCodec() to read a compressed file.
func OpenFile(path string, opts ...Option) (*FileReader, error) {
	cfg := resolve(opts)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &FileReader{f: f, r: f, nr: NewNativeReader(), codec: cfg.codec}, nil
}

// newReader is a test/helper constructor over an arbitrary io.Reader.
func newReader(r io.Reader) *FileReader {
	return &FileReader{r: r, nr: NewNativeReader()}
}

// readUnit reads (and on first call validates the header of) the next compressed
// unit, returning its decompressed plaintext. Only valid when codec != nil.
func (fr *FileReader) readUnit() ([]byte, error) {
	if !fr.headerRead {
		if err := readCompressedHeader(fr.r, fr.codec.Name); err != nil {
			return nil, err
		}
		fr.headerRead = true
	}
	return readCompressedUnit(fr.r, fr.codec)
}

// ReadBlock reads the next block, building columns from the file's type strings.
// Returns io.EOF when no blocks remain.
func (fr *FileReader) ReadBlock() (int, []column.ColumnCore, error) {
	var cols []column.ColumnCore
	if fr.codec == nil {
		var err error
		cols, err = fr.nr.ReadBlock(fr.r, nil)
		if errors.Is(err, io.EOF) {
			return 0, nil, io.EOF
		}
		if err != nil {
			return 0, nil, err
		}
	} else {
		plain, err := fr.readUnit()
		if errors.Is(err, io.EOF) {
			return 0, nil, io.EOF
		}
		if err != nil {
			return 0, nil, err
		}
		var nRows int
		nRows, cols, _, err = readBlockFromBytes(plain, nil)
		if err != nil {
			return 0, nil, err
		}
		return nRows, cols, nil
	}
	numRows := 0
	if len(cols) > 0 {
		numRows = cols[0].NumRow()
	}
	return numRows, cols, nil
}

// ReadBlockInto reads the next block into caller-provided columns.
// Returns io.EOF when no blocks remain.
func (fr *FileReader) ReadBlockInto(cols ...column.ColumnCore) (int, error) {
	if fr.codec == nil {
		err := fr.nr.ReadBlockInto(fr.r, nil, cols...)
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
	} else {
		plain, err := fr.readUnit()
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		if _, _, _, err := readBlockFromBytes(plain, cols); err != nil {
			return 0, err
		}
	}
	numRows := 0
	if len(cols) > 0 {
		numRows = cols[0].NumRow()
	}
	return numRows, nil
}

// Close closes the underlying file (nil for reader-backed instances).
func (fr *FileReader) Close() error {
	if fr.f == nil {
		return nil
	}
	return fr.f.Close()
}
