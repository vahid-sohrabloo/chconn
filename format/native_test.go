package format_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/format"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

func TestNativeRoundtrip(t *testing.T) {
	// Create and populate columns
	colInt := column.New[int64]()
	colInt.SetName([]byte("id"))
	colInt.SetType([]byte("Int64"))
	colInt.Append(1)
	colInt.Append(2)
	colInt.Append(3)

	colStr := column.NewString()
	colStr.SetName([]byte("name"))
	colStr.SetType([]byte("String"))
	colStr.Append("alice")
	colStr.Append("bob")
	colStr.Append("charlie")

	// Encode
	var buf bytes.Buffer
	writer := format.NewNativeWriter(&buf)
	if err := writer.WriteBlock(colInt, colStr); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	// Decode
	reader := format.NewNativeReader()
	cols, err := reader.ReadBlock(&buf, nil)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}

	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}

	// Verify Int64 column
	intCol, ok := cols[0].(column.Column[int64])
	if !ok {
		t.Fatalf("expected Column[int64], got %T", cols[0])
	}
	if intCol.NumRow() != 3 {
		t.Fatalf("expected 3 rows, got %d", intCol.NumRow())
	}
	for i, want := range []int64{1, 2, 3} {
		if got := intCol.Row(i); got != want {
			t.Errorf("row %d: got %d, want %d", i, got, want)
		}
	}

	// Verify String column
	strCol, ok := cols[1].(column.Column[string])
	if !ok {
		t.Fatalf("expected Column[string], got %T", cols[1])
	}
	for i, want := range []string{"alice", "bob", "charlie"} {
		if got := strCol.Row(i); got != want {
			t.Errorf("row %d: got %q, want %q", i, got, want)
		}
	}

	// Verify column metadata
	if got := string(cols[0].Name()); got != "id" {
		t.Errorf("column 0 name: got %q, want %q", got, "id")
	}
	if got := string(cols[1].Name()); got != "name" {
		t.Errorf("column 1 name: got %q, want %q", got, "name")
	}
}

func TestNativeAppendModeRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	writer := format.NewNativeWriter(&buf)

	err := writer.SetColumns([]column.ColumnHeader{
		{Name: []byte("id"), ChType: []byte("Int64")},
		{Name: []byte("value"), ChType: []byte("Float64")},
		{Name: []byte("label"), ChType: []byte("String")},
	})
	if err != nil {
		t.Fatalf("SetColumns: %v", err)
	}

	// Append rows
	rows := []struct {
		id    int64
		value float64
		label string
	}{
		{1, 1.5, "a"},
		{2, 2.5, "b"},
		{3, 3.5, "c"},
	}
	for _, r := range rows {
		if err := writer.Append(r.id, r.value, r.label); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Decode
	reader := format.NewNativeReader()
	cols, err := reader.ReadBlock(&buf, nil)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}

	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}

	intCol := cols[0].(column.Column[int64])
	floatCol := cols[1].(column.Column[float64])
	strCol := cols[2].(column.Column[string])

	for i, r := range rows {
		if got := intCol.Row(i); got != r.id {
			t.Errorf("row %d id: got %d, want %d", i, got, r.id)
		}
		if got := floatCol.Row(i); got != r.value {
			t.Errorf("row %d value: got %f, want %f", i, got, r.value)
		}
		if got := strCol.Row(i); got != r.label {
			t.Errorf("row %d label: got %q, want %q", i, got, r.label)
		}
	}
}

func TestNativeEmptyBlock(t *testing.T) {
	colInt := column.New[int64]()
	colInt.SetName([]byte("id"))
	colInt.SetType([]byte("Int64"))

	var buf bytes.Buffer
	writer := format.NewNativeWriter(&buf)
	if err := writer.WriteBlock(colInt); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	reader := format.NewNativeReader()
	cols, err := reader.ReadBlock(&buf, nil)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}

	if len(cols) != 1 {
		t.Fatalf("expected 1 column, got %d", len(cols))
	}
	if cols[0].NumRow() != 0 {
		t.Fatalf("expected 0 rows, got %d", cols[0].NumRow())
	}
}

func TestNativeMultipleBlocks(t *testing.T) {
	var buf bytes.Buffer
	writer := format.NewNativeWriter(&buf)

	// Write 3 blocks
	for blockIdx := range 3 {
		col := column.New[int64]()
		col.SetName([]byte("val"))
		col.SetType([]byte("Int64"))
		for j := range 5 {
			col.Append(int64(blockIdx*10 + j))
		}
		if err := writer.WriteBlock(col); err != nil {
			t.Fatalf("WriteBlock %d: %v", blockIdx, err)
		}
	}

	// Read 3 blocks
	reader := format.NewNativeReader()
	r := bytes.NewReader(buf.Bytes())
	for blockIdx := range 3 {
		cols, err := reader.ReadBlock(r, nil)
		if err != nil {
			t.Fatalf("ReadBlock %d: %v", blockIdx, err)
		}
		intCol := cols[0].(column.Column[int64])
		if intCol.NumRow() != 5 {
			t.Fatalf("block %d: expected 5 rows, got %d", blockIdx, intCol.NumRow())
		}
		for j := range 5 {
			want := int64(blockIdx*10 + j)
			if got := intCol.Row(j); got != want {
				t.Errorf("block %d row %d: got %d, want %d", blockIdx, j, got, want)
			}
		}
	}

	// Next read should fail (EOF)
	_, err := reader.ReadBlock(r, nil)
	if err == nil {
		t.Fatal("expected error at EOF, got nil")
	}
}

func TestNativeReadBlockInto(t *testing.T) {
	// Encode
	colInt := column.New[int64]()
	colInt.SetName([]byte("x"))
	colInt.SetType([]byte("Int64"))
	colInt.Append(100)
	colInt.Append(200)

	colStr := column.NewString()
	colStr.SetName([]byte("y"))
	colStr.SetType([]byte("String"))
	colStr.Append("hello")
	colStr.Append("world")

	var buf bytes.Buffer
	writer := format.NewNativeWriter(&buf)
	if err := writer.WriteBlock(colInt, colStr); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	// Decode into pre-created columns
	destInt := column.New[int64]()
	destStr := column.NewString()

	reader := format.NewNativeReader()
	if err := reader.ReadBlockInto(&buf, nil, destInt, destStr); err != nil {
		t.Fatalf("ReadBlockInto: %v", err)
	}

	if destInt.NumRow() != 2 {
		t.Fatalf("expected 2 rows, got %d", destInt.NumRow())
	}
	if got := destInt.Row(0); got != 100 {
		t.Errorf("row 0: got %d, want 100", got)
	}
	if got := destInt.Row(1); got != 200 {
		t.Errorf("row 1: got %d, want 200", got)
	}
	if got := destStr.Row(0); got != "hello" {
		t.Errorf("row 0: got %q, want %q", got, "hello")
	}
	if got := destStr.Row(1); got != "world" {
		t.Errorf("row 1: got %q, want %q", got, "world")
	}

	// Verify metadata was set
	if got := string(destInt.Name()); got != "x" {
		t.Errorf("name: got %q, want %q", got, "x")
	}
	if got := string(destStr.Name()); got != "y" {
		t.Errorf("name: got %q, want %q", got, "y")
	}
}

func TestNativeReadBlockIntoColumnMismatch(t *testing.T) {
	colInt := column.New[int64]()
	colInt.SetName([]byte("x"))
	colInt.SetType([]byte("Int64"))
	colInt.Append(1)

	var buf bytes.Buffer
	writer := format.NewNativeWriter(&buf)
	if err := writer.WriteBlock(colInt); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	// Try to read into wrong number of columns
	destInt := column.New[int64]()
	destStr := column.NewString()

	reader := format.NewNativeReader()
	err := reader.ReadBlockInto(&buf, nil, destInt, destStr)
	if err == nil {
		t.Fatal("expected error for column count mismatch")
	}
}

func TestNativeWriterMismatchedRows(t *testing.T) {
	col1 := column.New[int64]()
	col1.SetName([]byte("a"))
	col1.SetType([]byte("Int64"))
	col1.Append(1)

	col2 := column.New[int64]()
	col2.SetName([]byte("b"))
	col2.SetType([]byte("Int64"))
	col2.Append(1)
	col2.Append(2)

	writer := format.NewNativeWriter(io.Discard)
	err := writer.WriteBlock(col1, col2)
	if err == nil {
		t.Fatal("expected error for mismatched row counts")
	}
}

func TestNativeAppendBeforeSetColumns(t *testing.T) {
	writer := format.NewNativeWriter(io.Discard)
	err := writer.Append(1)
	if err == nil {
		t.Fatal("expected error when Append called before SetColumns")
	}
}

func TestNativeFlushBeforeSetColumns(t *testing.T) {
	writer := format.NewNativeWriter(io.Discard)
	err := writer.Flush()
	if err == nil {
		t.Fatal("expected error when Flush called before SetColumns")
	}
}

func TestNativeAppendWrongValueCount(t *testing.T) {
	writer := format.NewNativeWriter(io.Discard)
	if err := writer.SetColumns([]column.ColumnHeader{
		{Name: []byte("id"), ChType: []byte("Int64")},
	}); err != nil {
		t.Fatalf("SetColumns: %v", err)
	}

	err := writer.Append(int64(1), "extra")
	if err == nil {
		t.Fatal("expected error for wrong value count")
	}
}

func TestNativeWriteBlockNoColumns(t *testing.T) {
	writer := format.NewNativeWriter(io.Discard)
	// Writing with no columns should be a no-op
	if err := writer.WriteBlock(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNativeColumnsAccessor(t *testing.T) {
	writer := format.NewNativeWriter(io.Discard)
	if cols := writer.Columns(); cols != nil {
		t.Fatal("expected nil columns before SetColumns")
	}

	if err := writer.SetColumns([]column.ColumnHeader{
		{Name: []byte("id"), ChType: []byte("Int64")},
	}); err != nil {
		t.Fatalf("SetColumns: %v", err)
	}

	cols := writer.Columns()
	if len(cols) != 1 {
		t.Fatalf("expected 1 column, got %d", len(cols))
	}
}

func TestNativeWithServerInfo(t *testing.T) {
	colInt := column.New[int64]()
	colInt.SetName([]byte("id"))
	colInt.SetType([]byte("Int64"))
	colInt.Append(42)

	var buf bytes.Buffer
	writer := format.NewNativeWriter(&buf)
	if err := writer.WriteBlock(colInt); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	// Use explicit server info
	serverInfo := &shared.ServerInfo{
		Timezone: "UTC",
	}
	reader := format.NewNativeReader()
	cols, err := reader.ReadBlock(&buf, serverInfo)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}

	intCol := cols[0].(column.Column[int64])
	if got := intCol.Row(0); got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestNativeFlushResetsColumns(t *testing.T) {
	var buf bytes.Buffer
	writer := format.NewNativeWriter(&buf)

	if err := writer.SetColumns([]column.ColumnHeader{
		{Name: []byte("id"), ChType: []byte("Int64")},
	}); err != nil {
		t.Fatalf("SetColumns: %v", err)
	}

	// Append and flush first block
	if err := writer.Append(int64(1)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Verify column was reset
	cols := writer.Columns()
	if cols[0].NumRow() != 0 {
		t.Fatalf("expected 0 rows after flush, got %d", cols[0].NumRow())
	}

	// Append and flush second block
	if err := writer.Append(int64(2)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := writer.Append(int64(3)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Read both blocks back
	r := bytes.NewReader(buf.Bytes())
	reader := format.NewNativeReader()

	// Block 1
	block1, err := reader.ReadBlock(r, nil)
	if err != nil {
		t.Fatalf("ReadBlock 1: %v", err)
	}
	intCol1 := block1[0].(column.Column[int64])
	if intCol1.NumRow() != 1 {
		t.Fatalf("block 1: expected 1 row, got %d", intCol1.NumRow())
	}
	if got := intCol1.Row(0); got != 1 {
		t.Errorf("block 1 row 0: got %d, want 1", got)
	}

	// Block 2
	block2, err := reader.ReadBlock(r, nil)
	if err != nil {
		t.Fatalf("ReadBlock 2: %v", err)
	}
	intCol2 := block2[0].(column.Column[int64])
	if intCol2.NumRow() != 2 {
		t.Fatalf("block 2: expected 2 rows, got %d", intCol2.NumRow())
	}
	if got := intCol2.Row(0); got != 2 {
		t.Errorf("block 2 row 0: got %d, want 2", got)
	}
	if got := intCol2.Row(1); got != 3 {
		t.Errorf("block 2 row 1: got %d, want 3", got)
	}
}

func BenchmarkNativeEncode(b *testing.B) {
	const numRows = 100_000

	colInt := column.New[int64]()
	colInt.SetName([]byte("id"))
	colInt.SetType([]byte("Int64"))

	colStr := column.NewString()
	colStr.SetName([]byte("name"))
	colStr.SetType([]byte("String"))

	for i := range numRows {
		colInt.Append(int64(i))
		colStr.Append("benchmark-value")
	}

	writer := format.NewNativeWriter(io.Discard)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if err := writer.WriteBlock(colInt, colStr); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNativeDecode(b *testing.B) {
	const numRows = 100_000

	colInt := column.New[int64]()
	colInt.SetName([]byte("id"))
	colInt.SetType([]byte("Int64"))

	colStr := column.NewString()
	colStr.SetName([]byte("name"))
	colStr.SetType([]byte("String"))

	for i := range numRows {
		colInt.Append(int64(i))
		colStr.Append("benchmark-value")
	}

	var buf bytes.Buffer
	writer := format.NewNativeWriter(&buf)
	if err := writer.WriteBlock(colInt, colStr); err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()

	reader := format.NewNativeReader()
	serverInfo := shared.EmptyServerInfo()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		r := bytes.NewReader(data)
		if _, err := reader.ReadBlock(r, serverInfo); err != nil {
			b.Fatal(err)
		}
	}
}
