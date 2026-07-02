package column

import (
	"encoding/binary"
	"math"
	"testing"
	"unsafe"
)

var _ ZeroCopyColumn = (*Base[float64])(nil)

func TestBaseReadFromBytes_Float64(t *testing.T) {
	src := []float64{1.5, -2.25, 0, 3.5e10}
	buf := make([]byte, len(src)*8)
	for i, v := range src {
		binary.LittleEndian.PutUint64(buf[i*8:], math.Float64bits(v))
	}

	c := New[float64]()
	consumed, err := c.ReadFromBytes(len(src), buf)
	if err != nil {
		t.Fatalf("ReadFromBytes: %v", err)
	}
	if consumed != len(buf) {
		t.Fatalf("consumed = %d, want %d", consumed, len(buf))
	}
	if c.NumRow() != len(src) {
		t.Fatalf("NumRow = %d, want %d", c.NumRow(), len(src))
	}
	for i, want := range src {
		if got := c.Row(i); got != want {
			t.Fatalf("Row(%d) = %v, want %v", i, got, want)
		}
	}
}

func TestBaseReadFromBytes_ShortBuffer(t *testing.T) {
	c := New[int32]()
	if _, err := c.ReadFromBytes(3, make([]byte, 8)); err == nil {
		t.Fatal("expected error for short buffer, got nil")
	}
}

// Asserts the little-endian path aliases (zero-copy) rather than copies.
func TestBaseReadFromBytes_AliasesLE(t *testing.T) {
	if !isLittleEndianForTest() {
		t.Skip("alias check is little-endian only")
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(1.0))
	c := New[float64]()
	if _, err := c.ReadFromBytes(1, buf); err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint64(buf, math.Float64bits(2.0)) // mutate backing buffer
	if got := c.Row(0); got != 2.0 {
		t.Fatalf("expected aliased view to reflect 2.0, got %v", got)
	}
}

func TestBaseReadFromBytes_ZeroRows(t *testing.T) {
	c := New[float64]()
	consumed, err := c.ReadFromBytes(0, nil)
	if err != nil || consumed != 0 || c.NumRow() != 0 {
		t.Fatalf("zero-rows: consumed=%d err=%v numRow=%d", consumed, err, c.NumRow())
	}
}

func isLittleEndianForTest() bool {
	var x uint16 = 1
	return *(*byte)(unsafePointerOf(&x)) == 1
}

func unsafePointerOf[T any](p *T) unsafe.Pointer { return unsafe.Pointer(p) }

var _ ZeroCopyColumn = (*String)(nil)

func TestStringReadFromBytes(t *testing.T) {
	src := []string{"", "ab", "hello world", "x"}
	var buf []byte
	for _, s := range src {
		buf = binary.AppendUvarint(buf, uint64(len(s)))
		buf = append(buf, s...)
	}

	c := NewString()
	consumed, err := c.ReadFromBytes(len(src), buf)
	if err != nil {
		t.Fatalf("ReadFromBytes: %v", err)
	}
	if consumed != len(buf) {
		t.Fatalf("consumed = %d, want %d", consumed, len(buf))
	}
	if c.NumRow() != len(src) {
		t.Fatalf("NumRow = %d, want %d", c.NumRow(), len(src))
	}
	for i, want := range src {
		if got := c.Row(i); got != want {
			t.Fatalf("Row(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestStringReadFromBytes_Truncated(t *testing.T) {
	buf := binary.AppendUvarint(nil, 10) // claims 10 bytes, provides 2
	buf = append(buf, "ab"...)
	c := NewString()
	if _, err := c.ReadFromBytes(1, buf); err == nil {
		t.Fatal("expected truncation error, got nil")
	}
}

func TestStringReadFromBytes_BadVarint(t *testing.T) {
	c := NewString()
	if _, err := c.ReadFromBytes(1, []byte{}); err == nil {
		t.Fatal("expected bad-varint error on empty buffer, got nil")
	}
}

func TestStringReadFromBytes_NegativeCount(t *testing.T) {
	c := NewString()
	if _, err := c.ReadFromBytes(-1, []byte{}); err == nil {
		t.Fatal("expected error for negative num, got nil")
	}
}
