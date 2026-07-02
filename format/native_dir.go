package format

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// ColumnMeta describes a column discovered in a dir-mode dataset.
type ColumnMeta struct {
	Index int
	Name  string
	Type  string
	file  string
}

const dirFileSuffix = ".native"
const dirMetaFile = "metadata.bin"
const dirMagic = "CNDM1"

// writeDirMeta writes an uncompressed sidecar file <dir>/metadata.bin with
// the dataset schema and file mapping. Format:
//
//	magic    5 bytes "CNDM1"
//	uvarint  rowCount
//	uvarint  numColumns
//	repeated numColumns:
//	  uvarint+bytes  name
//	  uvarint+bytes  chType
//	  uvarint+bytes  fileName
//
// The write is atomic: a temp file is renamed into place on success.
func writeDirMeta(dir string, metas []ColumnMeta, rowCount int) error {
	var buf []byte
	buf = append(buf, dirMagic...)
	buf = binary.AppendUvarint(buf, uint64(rowCount))
	buf = binary.AppendUvarint(buf, uint64(len(metas)))
	for _, m := range metas {
		buf = binary.AppendUvarint(buf, uint64(len(m.Name)))
		buf = append(buf, m.Name...)
		buf = binary.AppendUvarint(buf, uint64(len(m.Type)))
		buf = append(buf, m.Type...)
		buf = binary.AppendUvarint(buf, uint64(len(m.file)))
		buf = append(buf, m.file...)
	}

	dst := filepath.Join(dir, dirMetaFile)
	f, err := os.CreateTemp(dir, "."+dirMetaFile+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

// readDirMeta reads the uncompressed sidecar file <dir>/metadata.bin.
// Returns rowCount, ordered column metas, or an error if the file is missing
// or corrupt. Bounds: numColumns <= 1<<20, each string field <= 4096 bytes.
func readDirMeta(dir string) (rowCount int, metas []ColumnMeta, err error) {
	f, err := os.Open(filepath.Join(dir, dirMetaFile))
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()

	var magic [5]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return 0, nil, fmt.Errorf("native: read dir metadata magic: %w", err)
	}
	if string(magic[:]) != dirMagic {
		return 0, nil, fmt.Errorf("native: not a chconn dir metadata file (bad magic %v)", magic)
	}

	rc, err := readUvarint(f)
	if err != nil {
		return 0, nil, fmt.Errorf("native: read dir metadata rowCount: %w", err)
	}

	numCols, err := readUvarint(f)
	if err != nil {
		return 0, nil, fmt.Errorf("native: read dir metadata numColumns: %w", err)
	}
	if numCols > 1<<20 {
		return 0, nil, fmt.Errorf("native: dir metadata numColumns %d exceeds limit 1<<20", numCols)
	}

	metas = make([]ColumnMeta, numCols)
	for i := range metas {
		name, err := readBoundedString(f, 4096)
		if err != nil {
			return 0, nil, fmt.Errorf("native: column[%d] name: %w", i, err)
		}
		chType, err := readBoundedString(f, 4096)
		if err != nil {
			return 0, nil, fmt.Errorf("native: column[%d] type: %w", i, err)
		}
		fileName, err := readBoundedString(f, 4096)
		if err != nil {
			return 0, nil, fmt.Errorf("native: column[%d] file: %w", i, err)
		}
		if !validDirColumnFile(fileName) {
			return 0, nil, fmt.Errorf("native: column[%d] invalid file name %q", i, fileName)
		}
		metas[i] = ColumnMeta{Index: i, Name: name, Type: chType, file: fileName}
	}
	return int(rc), metas, nil
}

// readBoundedString reads a uvarint-length-prefixed string from r, rejecting
// strings longer than maxLen bytes.
func readBoundedString(r io.Reader, maxLen int) (string, error) {
	l, err := readUvarint(r)
	if err != nil {
		return "", err
	}
	if l > uint64(maxLen) {
		return "", fmt.Errorf("string length %d exceeds limit %d", l, maxLen)
	}
	if l == 0 {
		return "", nil
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// validDirColumnFile reports whether name is a safe, plain column file name
// confined to the dataset directory (no path separators, no traversal, correct
// suffix). It guards against crafted metadata pointing OpenColumn outside dir.
func validDirColumnFile(name string) bool {
	if name == "" || filepath.IsAbs(name) || filepath.Base(name) != name ||
		strings.ContainsAny(name, `/\`+"\x00") || !strings.HasSuffix(name, dirFileSuffix) {
		return false
	}
	return true
}

// WriteDir writes each column to its own single-column Native file:
// col_<NNNNNN>__<name>.native. Files are self-describing and individually readable.
// A metadata sidecar (metadata.bin) is written after all column files succeed,
// recording the schema without requiring decompression on OpenDir.
// Pass WithZSTD()/WithLZ4()/WithCodec() to compress each column file.
// Pass WithConcurrency(n) to control the number of parallel write workers
// (default: runtime.NumCPU()).
// On any error, already-written files are removed (best-effort) before
// returning, so a failed WriteDir does not leave a half-populated directory.
func WriteDir(dir string, cols []column.ColumnCore, opts ...Option) error {
	rowCount := 0
	if len(cols) > 0 {
		rowCount = cols[0].NumRow()
	}
	seen := make(map[string]struct{}, len(cols))
	for _, col := range cols {
		if len(col.Type()) == 0 {
			return fmt.Errorf("native: column %q has no type; call SetType before writing", col.Name())
		}
		if col.NumRow() != rowCount {
			return fmt.Errorf("native: column %q has %d rows, want %d", col.Name(), col.NumRow(), rowCount)
		}
		name := string(col.Name())
		if _, dup := seen[name]; dup {
			return fmt.Errorf("native: duplicate column name %q", name)
		}
		seen[name] = struct{}{}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// Refuse to overwrite an existing dataset in place: a mid-write failure could
	// leave stale metadata pointing at replaced/removed column files. Callers that
	// want to regenerate should write to a fresh directory.
	if _, err := os.Stat(filepath.Join(dir, dirMetaFile)); err == nil {
		return fmt.Errorf("native: %s already contains a dataset (%s); write to a fresh directory", dir, dirMetaFile)
	} else if !os.IsNotExist(err) {
		return err
	}

	cfg := resolve(opts)
	workers := cfg.workers()
	if workers > len(cols) && len(cols) > 0 {
		workers = len(cols)
	}

	var (
		mu       sync.Mutex
		firstErr error
		written  []string
	)

	metas := make([]ColumnMeta, len(cols))

	sem := make(chan struct{}, max(workers, 1))
	var wg sync.WaitGroup

	for i, col := range cols {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			name := string(col.Name())
			fn := fmt.Sprintf("col_%06d__%s%s", i, sanitize(name), dirFileSuffix)
			path := filepath.Join(dir, fn)

			err := WriteFile(path, []column.ColumnCore{col}, opts...)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("native: write column %q: %w", name, err)
				}
			} else {
				metas[i] = ColumnMeta{Index: i, Name: name, Type: string(col.Type()), file: fn}
				written = append(written, path)
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		for _, p := range written {
			os.Remove(p)
		}
		return firstErr
	}

	if err := writeDirMeta(dir, metas, rowCount); err != nil {
		for _, p := range written {
			os.Remove(p)
		}
		return fmt.Errorf("native: write dir metadata: %w", err)
	}
	return nil
}

// DirReader reads a dir-mode dataset, opening only the columns requested.
type DirReader struct {
	dir      string
	opts     []Option
	cols     []ColumnMeta
	byName   map[string]ColumnMeta
	rowCount int
}

// OpenDir reads the uncompressed metadata sidecar from dir to learn the
// dataset schema (column names, types, file mapping, row count). It does NOT
// read or decompress any column data. The sidecar must have been written by
// WriteDir; dirs without metadata.bin are rejected.
func OpenDir(dir string, opts ...Option) (*DirReader, error) {
	rowCount, metas, err := readDirMeta(dir)
	if err != nil {
		return nil, fmt.Errorf("native: open dir %s: %w", dir, err)
	}
	dr := &DirReader{
		dir:      dir,
		opts:     opts,
		byName:   make(map[string]ColumnMeta, len(metas)),
		rowCount: rowCount,
	}
	for _, meta := range metas {
		if _, exists := dr.byName[meta.Name]; exists {
			return nil, fmt.Errorf("native: duplicate column name %q in %s", meta.Name, dir)
		}
		dr.cols = append(dr.cols, meta)
		dr.byName[meta.Name] = meta
	}
	return dr, nil
}

// Columns returns metadata for every column, in file order.
func (dr *DirReader) Columns() []ColumnMeta { return dr.cols }

// RowCount returns the dataset row count.
func (dr *DirReader) RowCount() int { return dr.rowCount }

// OpenColumn reads ONLY the named column's file and returns its column.
// The DirReader's own options (e.g. WithZSTD()/WithLZ4()/WithCodec()) are applied automatically; any
// opts passed here are appended and may augment or override them.
func (dr *DirReader) OpenColumn(name string, opts ...Option) (column.ColumnCore, error) {
	meta, ok := dr.byName[name]
	if !ok {
		return nil, fmt.Errorf("native: column %q not found in %s", name, dr.dir)
	}
	allOpts := append(append([]Option(nil), dr.opts...), opts...)
	fr, err := OpenFile(filepath.Join(dr.dir, meta.file), allOpts...)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	_, cols, err := fr.ReadBlock()
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("native: column file for %q has no columns", name)
	}
	return cols[0], nil
}

// OpenColumns reads the named columns concurrently (workers from the options
// OpenDir was created with) and returns them in the same order as names.
// Each column is read from its own file, so reads are independent.
// Any opts are passed through to each OpenColumn call and may augment or
// override the DirReader's own options.
func (dr *DirReader) OpenColumns(names []string, opts ...Option) ([]column.ColumnCore, error) {
	if len(names) == 0 {
		return nil, nil
	}

	workers := min(resolve(dr.opts).workers(), len(names))

	type result struct {
		col column.ColumnCore
		err error
	}

	results := make([]result, len(names))

	sem := make(chan struct{}, max(workers, 1))
	var wg sync.WaitGroup

	for i, name := range names {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			col, err := dr.OpenColumn(name, opts...)
			results[i].col = col
			results[i].err = err
		}()
	}
	wg.Wait()

	out := make([]column.ColumnCore, len(names))
	for i, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		out[i] = r.col
	}
	return out, nil
}

// Close is a no-op (per-column files are opened and closed on demand).
func (dr *DirReader) Close() error { return nil }

// sanitize makes a column name safe for a filename.
func sanitize(name string) string {
	return strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || r == 0 {
			return '_'
		}
		return r
	}, name)
}
