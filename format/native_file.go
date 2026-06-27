package format

import (
	"errors"
	"fmt"
	"io"
	"os"

	lz4 "github.com/pierrec/lz4/v4"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// FileWriter writes one or more Native-format blocks to an io.Writer.
type FileWriter struct {
	nw *NativeWriter
}

// NewFileWriter wraps w. WriteBlock may be called repeatedly (multiple blocks).
func NewFileWriter(w io.Writer) *FileWriter { return &FileWriter{nw: NewNativeWriter(w)} }

// WriteBlock writes one block; all columns must have equal NumRow.
func (fw *FileWriter) WriteBlock(cols ...column.ColumnCore) error {
	return fw.nw.WriteBlock(cols...)
}

// WriteFile writes cols as a single-block Native file at path.
// The write is atomic: data goes to path+".tmp" and is renamed to path only on
// full success. Any error removes the temp file before returning.
// Pass WithLZ4() to write an lz4-compressed stream.
func WriteFile(path string, cols []column.ColumnCore, opts ...Option) error {
	for _, col := range cols {
		if len(col.Type()) == 0 {
			return fmt.Errorf("native: column %q has no type; call SetType before writing", col.Name())
		}
	}
	cfg := resolve(opts)
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	var w io.Writer = f
	var zw io.Closer
	if cfg.lz4 {
		lw := lz4.NewWriter(f)
		w, zw = lw, lw
	}
	if err := NewNativeWriter(w).WriteBlock(cols...); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if zw != nil {
		if err := zw.Close(); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
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
	f  *os.File
	r  io.Reader
	nr *NativeReader
}

// OpenFile opens a Native file for streaming reads.
// Pass WithLZ4() to read an lz4-compressed stream.
func OpenFile(path string, opts ...Option) (*FileReader, error) {
	cfg := resolve(opts)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var r io.Reader = f
	if cfg.lz4 {
		r = lz4.NewReader(f)
	}
	return &FileReader{f: f, r: r, nr: NewNativeReader()}, nil
}

// newReader is a test/helper constructor over an arbitrary io.Reader.
func newReader(r io.Reader) *FileReader {
	return &FileReader{r: r, nr: NewNativeReader()}
}

// ReadBlock reads the next block, building columns from the file's type strings.
// Returns io.EOF when no blocks remain.
func (fr *FileReader) ReadBlock() (int, []column.ColumnCore, error) {
	cols, err := fr.nr.ReadBlock(fr.r, nil)
	if errors.Is(err, io.EOF) {
		return 0, nil, io.EOF
	}
	if err != nil {
		return 0, nil, err
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
	err := fr.nr.ReadBlockInto(fr.r, nil, cols...)
	if errors.Is(err, io.EOF) {
		return 0, io.EOF
	}
	if err != nil {
		return 0, err
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
