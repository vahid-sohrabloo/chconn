package format

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestWriteAndOpenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.native")
	if err := WriteFile(path, []column.ColumnCore{
		floatCol("cpm", 1.5, 2.5),
		strCol("bidder", "x", "yy"),
	}); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	fr, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer fr.Close()

	numRows, cols, err := fr.ReadBlock()
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if numRows != 2 || len(cols) != 2 {
		t.Fatalf("got numRows=%d cols=%d", numRows, len(cols))
	}
	if cols[0].(*column.Base[float64]).Row(1) != 2.5 {
		t.Fatal("wrong float value")
	}
	if cols[1].(*column.String).Row(0) != "x" || cols[1].(*column.String).Row(1) != "yy" {
		t.Fatal("wrong string value")
	}
}

func TestFileReaderReadBlockIntoEOF(t *testing.T) {
	var buf bytes.Buffer
	fw := NewFileWriter(&buf)
	if err := fw.WriteBlock(floatCol("v", 1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := fw.WriteBlock(floatCol("v", 3)); err != nil {
		t.Fatal(err)
	}
	fr := newReader(&buf)
	total := 0
	for {
		n, err := fr.ReadBlockInto(floatCol("v"))
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadBlockInto: %v", err)
		}
		total += n
	}
	if total != 3 {
		t.Fatalf("total rows = %d, want 3", total)
	}
}

func TestFileWriterMultiBlock(t *testing.T) {
	var buf bytes.Buffer
	fw := NewFileWriter(&buf)
	if err := fw.WriteBlock(floatCol("v", 1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := fw.WriteBlock(floatCol("v", 3, 4, 5)); err != nil {
		t.Fatal(err)
	}

	r := newReader(&buf)
	total := 0
	for {
		n, cols, err := r.ReadBlock()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadBlock: %v", err)
		}
		total += n
		_ = cols
	}
	if total != 5 {
		t.Fatalf("total rows = %d, want 5", total)
	}
}
