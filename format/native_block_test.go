package format

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
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

func mixedBlock(t *testing.T, w *NativeWriter, first int64) {
	t.Helper()
	lc := column.NewString().LowCardinality()
	lc.SetName([]byte("lc"))
	lc.SetType([]byte("LowCardinality(String)"))
	n := column.New[uint64]().Nullable()
	n.SetName([]byte("n"))
	n.SetType([]byte("Nullable(UInt64)"))
	arr := column.NewArray[string](column.NewString())
	arr.SetName([]byte("arr"))
	arr.SetType([]byte("Array(String)"))
	m := column.NewMap[string, uint64](column.NewString(), column.New[uint64]())
	m.SetName([]byte("m"))
	m.SetType([]byte("Map(String, UInt64)"))
	last := column.New[int64]()
	last.SetName([]byte("last"))
	last.SetType([]byte("Int64"))
	for i := range int64(3) {
		lc.Append([]string{"x", "y"}[i%2])
		n.AppendP(nil)
		arr.Append([]string{"a", "bb"}[:i%3])
		m.Append(map[string]uint64{"k": uint64(i)})
		last.Append(first + i)
	}
	if err := w.WriteBlock(floatCol("f", 1, 2, 3), strCol("s", "p", "q", "r"), lc, n, arr, m, last); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
}

func TestReadBlockColumnsSkipsTheOthers(t *testing.T) {
	var buf bytes.Buffer
	w := NewNativeWriter(&buf)
	mixedBlock(t, w, 10)
	mixedBlock(t, w, 20)

	nr := NewNativeReader()
	keepNames := func(names ...string) func(string, string) bool {
		return func(name, _ string) bool { return slices.Contains(names, name) }
	}
	rows, cols, err := nr.ReadBlockColumns(&buf, nil, keepNames("s", "last"))
	if err != nil {
		t.Fatalf("first block: %v", err)
	}
	if rows != 3 || len(cols) != 2 || string(cols[0].Name()) != "s" || string(cols[1].Name()) != "last" {
		t.Fatalf("first block: rows=%d cols=%d", rows, len(cols))
	}
	if got := cols[1].(*column.Base[int64]).Row(2); got != 12 {
		t.Fatalf("last after skipped columns = %d, want 12", got)
	}

	rows, cols, err = nr.ReadBlockColumns(&buf, nil, func(string, string) bool { return false })
	if err != nil {
		t.Fatalf("second block: %v", err)
	}
	if rows != 3 || len(cols) != 0 {
		t.Fatalf("second block: rows=%d cols=%d, want 3 rows and no columns", rows, len(cols))
	}
	if _, _, err := nr.ReadBlockColumns(&buf, nil, nil); !errors.Is(err, io.EOF) {
		t.Fatalf("third read: want io.EOF, got %v", err)
	}
}

func TestReadBlockColumnsPassesTheType(t *testing.T) {
	var buf bytes.Buffer
	mixedBlock(t, NewNativeWriter(&buf), 0)
	var types []string
	_, cols, err := NewNativeReader().ReadBlockColumns(&buf, nil, func(_, chType string) bool {
		types = append(types, chType)
		return chType == "Int64"
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Float64", "String", "LowCardinality(String)", "Nullable(UInt64)", "Array(String)", "Map(String, UInt64)", "Int64"}
	if strings.Join(types, ";") != strings.Join(want, ";") || len(cols) != 1 || string(cols[0].Name()) != "last" {
		t.Fatalf("types = %q, kept %d", types, len(cols))
	}
}

func TestReadBlockColumnsTruncatedBlock(t *testing.T) {
	var buf bytes.Buffer
	mixedBlock(t, NewNativeWriter(&buf), 0)
	full := buf.Bytes()
	for _, cut := range []int{1, 3, len(full) / 2, len(full) - 1} {
		_, _, err := NewNativeReader().ReadBlockColumns(bytes.NewReader(full[:cut]), nil, nil)
		if !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			t.Fatalf("cut at %d of %d: want io.ErrUnexpectedEOF, got %v", cut, len(full), err)
		}
	}
}
