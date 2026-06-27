package format

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func floatCol(name string, vals ...float64) *column.Base[float64] {
	c := column.New[float64]()
	c.SetName([]byte(name))
	c.SetType([]byte("Float64"))
	c.AppendMulti(vals...)
	return c
}

func strCol(name string, vals ...string) *column.String {
	c := column.NewString()
	c.SetName([]byte(name))
	c.SetType([]byte("String"))
	for _, v := range vals {
		c.Append(v)
	}
	return c
}

func TestBlockRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf)
	if err := nw.WriteBlock(
		floatCol("cpm", 1.5, 2.5, 3.5),
		strCol("bidder", "a", "bb", "ccc"),
	); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	nr := NewNativeReader()
	cols, err := nr.ReadBlock(&buf, nil)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if len(cols) != 2 {
		t.Fatalf("len(cols) = %d, want 2", len(cols))
	}
	if string(cols[0].Name()) != "cpm" || string(cols[1].Name()) != "bidder" {
		t.Fatalf("names = %q,%q", cols[0].Name(), cols[1].Name())
	}
	gotF := cols[0].(*column.Base[float64])
	if gotF.Row(0) != 1.5 || gotF.Row(2) != 3.5 {
		t.Fatalf("float values wrong: %v %v", gotF.Row(0), gotF.Row(2))
	}
	gotS := cols[1].(*column.String)
	if gotS.Row(1) != "bb" {
		t.Fatalf("string value wrong: %q", gotS.Row(1))
	}
}

func TestWriteBlockRequiresType(t *testing.T) {
	c := column.New[float64]()
	c.SetName([]byte("x"))
	c.Append(1) // no SetType — WriteFile must reject this
	if err := WriteFile(t.TempDir()+"/noType.native", []column.ColumnCore{c}); err == nil {
		t.Fatal("expected error for missing type, got nil")
	}
}

func TestWriteBlockRowMismatch(t *testing.T) {
	nw := NewNativeWriter(io.Discard)
	err := nw.WriteBlock(
		floatCol("a", 1, 2),
		floatCol("b", 1),
	)
	if err == nil {
		t.Fatal("expected row-count mismatch error, got nil")
	}
}

func TestReadBlockIntoCountMismatch(t *testing.T) {
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf)
	if err := nw.WriteBlock(floatCol("a", 1.0), floatCol("b", 2.0)); err != nil {
		t.Fatal(err)
	}
	nr := NewNativeReader()
	if err := nr.ReadBlockInto(&buf, nil, floatCol("a", 0)); err == nil {
		t.Fatal("expected count mismatch error")
	}
}

func TestReadBlockIntoIncompatibleColumn(t *testing.T) {
	// Write one Float64 row (8 bytes data). A Nullable(UInt64) column needs
	// null-bitmap (1 byte) + uint64 data (8 bytes) = 9 bytes — more than available,
	// so ReadRaw must fail.
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf)
	if err := nw.WriteBlock(floatCol("x", 1.0)); err != nil {
		t.Fatal(err)
	}
	nr := NewNativeReader()
	dst := column.New[uint64]().Nullable()
	dst.SetName([]byte("x"))
	dst.SetType([]byte("Nullable(UInt64)"))
	if err := nr.ReadBlockInto(&buf, nil, dst); err == nil {
		t.Fatal("expected error reading structurally incompatible column data")
	}
}

func TestReadBlockEOFAtBoundary(t *testing.T) {
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf)
	if err := nw.WriteBlock(floatCol("v", 1.0)); err != nil {
		t.Fatal(err)
	}
	nr := NewNativeReader()
	if _, err := nr.ReadBlock(&buf, nil); err != nil {
		t.Fatalf("first read: %v", err)
	}
	if _, err := nr.ReadBlock(&buf, nil); !errors.Is(err, io.EOF) {
		t.Fatalf("second read: want io.EOF, got %v", err)
	}
}

func TestNativeReaderHugeColumnCount(t *testing.T) {
	var b []byte
	b = binary.AppendUvarint(b, uint64(1)<<40) // absurd num_columns
	b = binary.AppendUvarint(b, 0)             // num_rows
	nr := NewNativeReader()
	if _, err := nr.ReadBlock(bytes.NewReader(b), nil); err == nil {
		t.Fatal("expected error for implausible column count")
	}
}
