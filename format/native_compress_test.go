package format

import (
	"path/filepath"
	"testing"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestFileCompressedRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.native.lz4")
	cols := []column.ColumnCore{
		floatCol("cpm", 1.5, 2.5, 3.5),
		strCol("bidder", "a", "bb", "ccc"),
	}
	if err := WriteFile(path, cols, WithLZ4()); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	fr, err := OpenFile(path, WithLZ4())
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
	if got[1].(*column.String).Row(0) != "a" {
		t.Fatalf("bidder[0] = %q, want %q", got[1].(*column.String).Row(0), "a")
	}

	// Prove the file is actually lz4-compressed: reading it without WithLZ4 must fail.
	plain, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile (no lz4): %v", err)
	}
	defer plain.Close()
	if _, _, err := plain.ReadBlock(); err == nil {
		t.Fatal("expected error reading lz4 file without WithLZ4; compression may be a no-op")
	}
}

func TestDirCompressedRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if err := WriteDir(dir, []column.ColumnCore{
		floatCol("cpm", 1.5, 2.5, 3.5),
		strCol("bidder", "a", "bb", "ccc"),
	}, WithLZ4()); err != nil {
		t.Fatalf("WriteDir: %v", err)
	}
	dr, err := OpenDir(dir, WithLZ4())
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
}
