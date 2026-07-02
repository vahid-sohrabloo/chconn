package format

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	kpzstd "github.com/klauspost/compress/zstd"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func fileRoundTrip(t *testing.T, opts ...Option) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.native.z")
	cols := []column.ColumnCore{
		floatCol("cpm", 1.5, 2.5, 3.5),
		strCol("bidder", "a", "bb", "ccc"),
	}
	if err := WriteFile(path, cols, opts...); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	fr, err := OpenFile(path, opts...)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer fr.Close()
	n, got, err := fr.ReadBlock()
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if n != 3 {
		t.Fatalf("ReadBlock: got %d rows, want 3", n)
	}
	if got[0].(*column.Base[float64]).Row(2) != 3.5 {
		t.Fatalf("cpm[2] = %v, want 3.5", got[0].(*column.Base[float64]).Row(2))
	}
	if got[1].(*column.String).Row(0) != "a" {
		t.Fatalf("bidder[0] = %q, want %q", got[1].(*column.String).Row(0), "a")
	}

	// Prove the file is actually compressed: reading it without the codec must fail.
	plain, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile (no codec): %v", err)
	}
	defer plain.Close()
	if _, _, err := plain.ReadBlock(); err == nil {
		t.Fatal("expected error reading compressed file without codec; compression may be a no-op")
	}
}

func dirRoundTrip(t *testing.T, opts ...Option) {
	t.Helper()
	dir := t.TempDir()
	if err := WriteDir(dir, []column.ColumnCore{
		floatCol("cpm", 1.5, 2.5, 3.5),
		strCol("bidder", "a", "bb", "ccc"),
	}, opts...); err != nil {
		t.Fatalf("WriteDir: %v", err)
	}
	dr, err := OpenDir(dir, opts...)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	defer dr.Close()
	if dr.RowCount() != 3 {
		t.Fatalf("RowCount = %d, want 3", dr.RowCount())
	}
	col, err := dr.OpenColumn("cpm")
	if err != nil {
		t.Fatalf("OpenColumn: %v", err)
	}
	if col.(*column.Base[float64]).Row(0) != 1.5 {
		t.Fatal("compressed dir round-trip mismatch")
	}
	bidder, err := dr.OpenColumn("bidder")
	if err != nil {
		t.Fatalf("OpenColumn bidder: %v", err)
	}
	if bidder.(*column.String).Row(2) != "ccc" {
		t.Fatal("compressed dir round-trip string mismatch")
	}
}

func TestFileZSTDRoundTrip(t *testing.T) { fileRoundTrip(t, WithZSTD()) }
func TestDirZSTDRoundTrip(t *testing.T)  { dirRoundTrip(t, WithZSTD()) }

func TestFileLZ4RoundTrip(t *testing.T) { fileRoundTrip(t, WithLZ4()) }
func TestDirLZ4RoundTrip(t *testing.T)  { dirRoundTrip(t, WithLZ4()) }

// customCodec wraps klauspost zstd under a distinct name to exercise WithCodec.
func customCodec() Codec {
	enc, _ := kpzstd.NewWriter(nil, kpzstd.WithEncoderConcurrency(1))
	dec, _ := kpzstd.NewReader(nil, kpzstd.WithDecoderConcurrency(1))
	return Codec{
		Name:       "custom-x",
		Compress:   func(dst, src []byte) ([]byte, error) { return enc.EncodeAll(src, dst), nil },
		Decompress: func(dst, src []byte) ([]byte, error) { return dec.DecodeAll(src, dst) },
	}
}

func TestFileCustomCodecRoundTrip(t *testing.T) { fileRoundTrip(t, WithCodec(customCodec())) }
func TestDirCustomCodecRoundTrip(t *testing.T)  { dirRoundTrip(t, WithCodec(customCodec())) }

func TestFileWriterMultiBlockCompressed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "multi.native.zst")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	fw := NewFileWriter(f, WithZSTD())
	if err := fw.WriteBlock(floatCol("v", 1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := fw.WriteBlock(floatCol("v", 3, 4, 5)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	fr, err := OpenFile(path, WithZSTD())
	if err != nil {
		t.Fatal(err)
	}
	defer fr.Close()
	total := 0
	for {
		n, _, err := fr.ReadBlock()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected error reading block: %v", err)
			}
			break
		}
		total += n
	}
	if total != 5 {
		t.Fatalf("total rows = %d, want 5", total)
	}
}

func TestCodecNameMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.native.z")
	cols := []column.ColumnCore{floatCol("cpm", 1.5, 2.5, 3.5)}
	if err := WriteFile(path, cols, WithZSTD()); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	fr, err := OpenFile(path, WithLZ4())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer fr.Close()
	if _, _, err := fr.ReadBlock(); err == nil {
		t.Fatal("expected codec mismatch error reading zstd file with WithLZ4")
	}
}

// TestCompressedFileNonZeroCopyColumn verifies that compressed reads handle a mix
// of ZeroCopy columns (Float64, String — aliased) and non-ZeroCopy columns
// (LowCardinality(String) — streamed via the fallback path) correctly.
func TestCompressedFileNonZeroCopyColumn(t *testing.T) {
	lcCol := column.NewString().LowCardinality()
	lcCol.SetName([]byte("category"))
	lcCol.SetType([]byte("LowCardinality(String)"))
	lcCol.Append("foo")
	lcCol.Append("bar")
	lcCol.Append("foo") // repeated value exercises dictionary deduplication

	path := filepath.Join(t.TempDir(), "mixed.native.zst")
	cols := []column.ColumnCore{
		floatCol("cpm", 1.5, 2.5, 3.5),
		strCol("bidder", "a", "bb", "ccc"),
		lcCol,
	}
	if err := WriteFile(path, cols, WithZSTD()); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	fr, err := OpenFile(path, WithZSTD())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer fr.Close()

	n, got, err := fr.ReadBlock()
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if n != 3 || len(got) != 3 {
		t.Fatalf("got numRows=%d cols=%d, want 3,3", n, len(got))
	}

	// Float64 column — alias path
	if got[0].(*column.Base[float64]).Row(0) != 1.5 || got[0].(*column.Base[float64]).Row(2) != 3.5 {
		t.Fatal("float column mismatch")
	}
	// String column — alias path
	if got[1].(*column.String).Row(1) != "bb" {
		t.Fatal("string column mismatch")
	}
	// LowCardinality(String) — streaming fallback path
	lcGot, ok := got[2].(column.Column[string])
	if !ok {
		t.Fatalf("expected Column[string] for LowCardinality, got %T", got[2])
	}
	if lcGot.Row(0) != "foo" || lcGot.Row(1) != "bar" || lcGot.Row(2) != "foo" {
		t.Fatalf("LC column: got %q, %q, %q; want foo, bar, foo",
			lcGot.Row(0), lcGot.Row(1), lcGot.Row(2))
	}
}

// TestCompressedDirNonZeroCopyColumn exercises the OpenColumn → ReadBlock path
// with a non-ZeroCopy column type under compression.
func TestCompressedDirNonZeroCopyColumn(t *testing.T) {
	lcCol := column.NewString().LowCardinality()
	lcCol.SetName([]byte("tag"))
	lcCol.SetType([]byte("LowCardinality(String)"))
	lcCol.Append("x")
	lcCol.Append("y")
	lcCol.Append("x")

	dir := t.TempDir()
	if err := WriteDir(dir, []column.ColumnCore{lcCol}, WithZSTD()); err != nil {
		t.Fatalf("WriteDir: %v", err)
	}

	dr, err := OpenDir(dir, WithZSTD())
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	defer dr.Close()

	col, err := dr.OpenColumn("tag")
	if err != nil {
		t.Fatalf("OpenColumn: %v", err)
	}
	lc, ok := col.(column.Column[string])
	if !ok {
		t.Fatalf("expected Column[string], got %T", col)
	}
	if lc.NumRow() != 3 || lc.Row(0) != "x" || lc.Row(1) != "y" || lc.Row(2) != "x" {
		t.Fatalf("unexpected LC data: rows=%d [%q,%q,%q]", lc.NumRow(), lc.Row(0), lc.Row(1), lc.Row(2))
	}
}

func TestDirCompressedRequiresCodec(t *testing.T) {
	dir := t.TempDir()
	if err := WriteDir(dir, []column.ColumnCore{floatCol("v", 1, 2, 3)}, WithZSTD()); err != nil {
		t.Fatal(err)
	}
	// OpenDir reads the uncompressed sidecar only, so it succeeds without a codec.
	dr, err := OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir without codec should succeed (reads uncompressed sidecar): %v", err)
	}
	defer dr.Close()
	// OpenColumn without codec must fail because the column data file is compressed.
	if _, err := dr.OpenColumn("v"); err == nil {
		t.Fatal("expected OpenColumn without codec to fail on compressed column data")
	}
	// OpenColumn with the correct codec must succeed.
	if _, err := dr.OpenColumn("v", WithZSTD()); err != nil {
		t.Fatalf("OpenColumn with codec: %v", err)
	}
}
