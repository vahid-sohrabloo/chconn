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

func TestDirCompressedRequiresCodec(t *testing.T) {
	dir := t.TempDir()
	if err := WriteDir(dir, []column.ColumnCore{floatCol("v", 1, 2, 3)}, WithZSTD()); err != nil {
		t.Fatal(err)
	}
	// OpenDir reads each file's header; without the codec it must fail, not silently succeed.
	if _, err := OpenDir(dir); err == nil {
		t.Fatal("expected OpenDir without codec to fail on compressed dir")
	}
}
