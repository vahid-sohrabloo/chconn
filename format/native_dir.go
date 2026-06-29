package format

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

// ColumnMeta describes a column discovered in a dir-mode dataset.
type ColumnMeta struct {
	Index int
	Name  string
	Type  string
	file  string
}

const dirFileSuffix = ".native"

// WriteDir writes each column to its own single-column Native file:
// col_<NNNNNN>__<name>.native. Files are self-describing and individually readable.
// Pass WithZSTD()/WithLZ4()/WithCodec() to compress each column file.
// Pass WithConcurrency(n) to control the number of parallel write workers
// (default: runtime.NumCPU()).
// On any error, already-written column files are removed (best-effort) before
// returning, so a failed WriteDir does not leave a half-populated directory.
func WriteDir(dir string, cols []column.ColumnCore, opts ...Option) error {
	seen := make(map[string]struct{}, len(cols))
	for _, col := range cols {
		if len(col.Type()) == 0 {
			return fmt.Errorf("native: column %q has no type; call SetType before writing", col.Name())
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

	sem := make(chan struct{}, workers)
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

// OpenDir scans dir for single-column Native files and reads each file's header
// to learn name/type/row-count. It does NOT read column data.
// Pass WithZSTD()/WithLZ4()/WithCodec() if the directory was written with compression.
func OpenDir(dir string, opts ...Option) (*DirReader, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), dirFileSuffix) {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files) // col_000000__, col_000001__, ... — gives stable column order

	dr := &DirReader{dir: dir, opts: opts, byName: map[string]ColumnMeta{}}
	for idx, fn := range files {
		meta, rows, err := readDirFileHeader(filepath.Join(dir, fn), opts)
		if err != nil {
			return nil, fmt.Errorf("native: scan %s: %w", fn, err)
		}
		meta.Index = idx
		meta.file = fn
		if idx == 0 {
			dr.rowCount = rows
		} else if rows != dr.rowCount {
			return nil, fmt.Errorf("native: %s has %d rows, expected %d", fn, rows, dr.rowCount)
		}
		if _, exists := dr.byName[meta.Name]; exists {
			return nil, fmt.Errorf("native: duplicate column name %q in %s", meta.Name, dir)
		}
		dr.cols = append(dr.cols, meta)
		dr.byName[meta.Name] = meta
	}
	return dr, nil
}

// readDirFileHeader reads only the block preamble (numCols, numRows) and the
// first column's name+type from path. No column data is read.
func readDirFileHeader(path string, opts []Option) (ColumnMeta, int, error) {
	cfg := resolve(opts)
	f, err := os.Open(path)
	if err != nil {
		return ColumnMeta{}, 0, err
	}
	defer f.Close()
	var rd io.Reader = f
	if cfg.codec != nil {
		if err := readCompressedHeader(f, cfg.codec.Name); err != nil {
			return ColumnMeta{}, 0, err
		}
		plain, err := readCompressedUnit(f, cfg.codec)
		if err != nil {
			return ColumnMeta{}, 0, err
		}
		rd = bytes.NewReader(plain)
	}
	r := readerwriter.NewReader(rd)
	numCols, err := r.Uvarint()
	if err != nil {
		return ColumnMeta{}, 0, fmt.Errorf("read num_columns: %w", err)
	}
	if numCols != 1 {
		return ColumnMeta{}, 0, fmt.Errorf("expected 1 column per dir file, got %d", numCols)
	}
	numRows, err := r.Uvarint()
	if err != nil {
		return ColumnMeta{}, 0, fmt.Errorf("read num_rows: %w", err)
	}
	name, err := r.ByteString()
	if err != nil {
		return ColumnMeta{}, 0, fmt.Errorf("read column name: %w", err)
	}
	chType, err := r.ByteString()
	if err != nil {
		return ColumnMeta{}, 0, fmt.Errorf("read column type: %w", err)
	}
	return ColumnMeta{Name: string(name), Type: string(chType)}, int(numRows), nil
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

	sem := make(chan struct{}, workers)
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
