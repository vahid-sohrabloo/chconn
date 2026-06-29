package format

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestWriteDirAndSelectiveRead(t *testing.T) {
	dir := t.TempDir()
	if err := WriteDir(dir, []column.ColumnCore{
		floatCol("cpm", 1.5, 2.5, 3.5),
		strCol("bidder", "a", "bb", "ccc"),
		floatCol("weight", 10, 20, 30),
	}); err != nil {
		t.Fatalf("WriteDir: %v", err)
	}

	dr, err := OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	defer dr.Close()

	if dr.RowCount() != 3 {
		t.Fatalf("RowCount = %d, want 3", dr.RowCount())
	}
	if len(dr.Columns()) != 3 {
		t.Fatalf("Columns = %d, want 3", len(dr.Columns()))
	}

	// Open ONLY the "weight" column — selective read.
	col, err := dr.OpenColumn("weight")
	if err != nil {
		t.Fatalf("OpenColumn: %v", err)
	}
	if col.(*column.Base[float64]).Row(2) != 30 {
		t.Fatal("wrong weight value")
	}

	if _, err := dr.OpenColumn("nope"); err == nil {
		t.Fatal("expected error for missing column")
	}
}

// TestOpenDirMissingMetadata verifies that OpenDir fails when no metadata.bin exists.
func TestOpenDirMissingMetadata(t *testing.T) {
	dir := t.TempDir()
	if _, err := OpenDir(dir); err == nil {
		t.Fatal("expected error opening dir without metadata.bin")
	}
}

func TestOpenDirCorruptMetadata(t *testing.T) {
	dir := t.TempDir()
	// Valid magic, then truncated (EOF before rowCount) — must error, not panic.
	if err := os.WriteFile(filepath.Join(dir, "metadata.bin"), []byte("CNDM1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDir(dir); err == nil {
		t.Fatal("expected error opening dir with truncated metadata")
	}
}
