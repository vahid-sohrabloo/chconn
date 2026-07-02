package format

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestBytesReaderZeroCopy(t *testing.T) {
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf)
	if err := nw.WriteBlock(
		floatCol("cpm", 1.5, 2.5, 3.5),
		strCol("bidder", "a", "bb", "ccc"),
	); err != nil {
		t.Fatal(err)
	}

	br := OpenBytes(buf.Bytes())
	numRows, cols, err := br.ReadBlock()
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if numRows != 3 || len(cols) != 2 {
		t.Fatalf("got numRows=%d cols=%d", numRows, len(cols))
	}
	if cols[0].(*column.Base[float64]).Row(2) != 3.5 {
		t.Fatal("wrong float value")
	}
	if cols[1].(*column.String).Row(2) != "ccc" {
		t.Fatal("wrong string value")
	}
}

// Columns without ZeroCopyColumn support (Nullable, LowCardinality, etc.) must
// now fall back to a streaming read and succeed rather than returning an error.
func TestBytesReaderNonZeroCopyFallback(t *testing.T) {
	c := column.New[uint64]().Nullable()
	c.SetName([]byte("n"))
	c.SetType([]byte("Nullable(UInt64)"))
	v := uint64(42)
	c.AppendP(&v)
	c.AppendP(nil)
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf)
	if err := nw.WriteBlock(c); err != nil {
		t.Fatal(err)
	}
	br := OpenBytes(buf.Bytes())
	numRows, cols, err := br.ReadBlock()
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if numRows != 2 || len(cols) != 1 {
		t.Fatalf("got numRows=%d cols=%d, want 2,1", numRows, len(cols))
	}
	got, ok := cols[0].(column.NullableColumn[uint64])
	if !ok {
		t.Fatalf("expected NullableColumn[uint64], got %T", cols[0])
	}
	if p := got.RowP(0); p == nil || *p != 42 {
		t.Fatalf("row 0: got %v, want 42", p)
	}
	if got.RowP(1) != nil {
		t.Fatal("row 1: expected nil, got non-nil")
	}
}

func TestBytesReaderFloat64Aliases(t *testing.T) {
	var one uint16 = 1
	if *(*byte)(unsafe.Pointer(&one)) != 1 {
		t.Skip("alias is little-endian only")
	}
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf)
	if err := nw.WriteBlock(floatCol("v", 1.0)); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	_, cols, err := OpenBytes(data).ReadBlock()
	if err != nil {
		t.Fatal(err)
	}
	fc := cols[0].(*column.Base[float64])
	if fc.Row(0) != 1.0 {
		t.Fatalf("before mutate: got %v", fc.Row(0))
	}
	// Single float64 column: its 8-byte payload is the final 8 bytes of the file.
	binary.LittleEndian.PutUint64(data[len(data)-8:], math.Float64bits(2.0))
	if fc.Row(0) != 2.0 {
		t.Fatalf("alias not reflected: got %v, want 2.0", fc.Row(0))
	}
}

func TestBytesReaderHugeRowCountErrors(t *testing.T) {
	var b []byte
	b = binary.AppendUvarint(b, 1)             // num_columns
	b = binary.AppendUvarint(b, uint64(1)<<62) // num_rows (absurd)
	b = binary.AppendUvarint(b, uint64(len("v")))
	b = append(b, "v"...)
	b = binary.AppendUvarint(b, uint64(len("Float64")))
	b = append(b, "Float64"...)
	// no column data
	_, _, err := OpenBytes(b).ReadBlock()
	if err == nil {
		t.Fatal("expected error for absurd row count, got nil (possible OOB)")
	}
}

func TestBytesReaderRowCountExceedsIntRange(t *testing.T) {
	// uint64(math.MaxInt)+1 cannot fit in int on any platform; the guard must fire.
	numRows := uint64(math.MaxInt) + 1
	var b []byte
	b = binary.AppendUvarint(b, 1)       // num_columns
	b = binary.AppendUvarint(b, numRows) // num_rows exceeds int range
	b = binary.AppendUvarint(b, uint64(len("v")))
	b = append(b, "v"...)
	b = binary.AppendUvarint(b, uint64(len("Float64")))
	b = append(b, "Float64"...)
	_, _, err := OpenBytes(b).ReadBlock()
	if err == nil {
		t.Fatal("expected error for row count exceeding int range, got nil")
	}
}

func TestBytesReaderHugeColCountErrors(t *testing.T) {
	var b []byte
	b = binary.AppendUvarint(b, uint64(1)<<40) // num_columns absurd
	b = binary.AppendUvarint(b, 0)
	if _, _, err := OpenBytes(b).ReadBlock(); err == nil {
		t.Fatal("expected error for absurd column count")
	}
}

func BenchmarkBytesReaderFloatScan(b *testing.B) {
	var buf bytes.Buffer
	vals := make([]float64, 100000)
	for i := range vals {
		vals[i] = float64(i)
	}
	c := column.New[float64]()
	c.SetName([]byte("v"))
	c.SetType([]byte("Float64"))
	c.AppendMulti(vals...)
	nw := NewNativeWriter(&buf)
	if err := nw.WriteBlock(c); err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, cols, err := OpenBytes(data).ReadBlock()
		if err != nil {
			b.Fatal(err)
		}
		var sum float64
		for _, v := range cols[0].(*column.Base[float64]).Data() {
			sum += v
		}
		_ = sum
	}
}
