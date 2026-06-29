package format

import (
	"testing"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// TestWriteDirParallelExplicitConcurrency writes several columns with
// WithConcurrency(4), then reads each back via OpenColumn and asserts values.
func TestWriteDirParallelExplicitConcurrency(t *testing.T) {
	dir := t.TempDir()
	cols := []column.ColumnCore{
		floatCol("a", 1.1, 2.2, 3.3),
		strCol("b", "x", "y", "z"),
		floatCol("c", 10, 20, 30),
		floatCol("d", 0.5, 1.5, 2.5),
		strCol("e", "alpha", "beta", "gamma"),
	}
	if err := WriteDir(dir, cols, WithConcurrency(4)); err != nil {
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
	if len(dr.Columns()) != 5 {
		t.Fatalf("Columns = %d, want 5", len(dr.Columns()))
	}

	colA, err := dr.OpenColumn("a")
	if err != nil {
		t.Fatalf("OpenColumn a: %v", err)
	}
	if got := colA.(*column.Base[float64]).Row(1); got != 2.2 {
		t.Fatalf("a[1] = %v, want 2.2", got)
	}

	colB, err := dr.OpenColumn("b")
	if err != nil {
		t.Fatalf("OpenColumn b: %v", err)
	}
	if got := colB.(*column.String).Row(2); got != "z" {
		t.Fatalf("b[2] = %q, want %q", got, "z")
	}

	colE, err := dr.OpenColumn("e")
	if err != nil {
		t.Fatalf("OpenColumn e: %v", err)
	}
	if got := colE.(*column.String).Row(0); got != "alpha" {
		t.Fatalf("e[0] = %q, want %q", got, "alpha")
	}
}

// TestWriteDirParallelDefaultConcurrency uses the default concurrency (NumCPU).
func TestWriteDirParallelDefaultConcurrency(t *testing.T) {
	dir := t.TempDir()
	if err := WriteDir(dir, []column.ColumnCore{
		floatCol("p", 7, 8, 9),
		strCol("q", "one", "two", "three"),
	}); err != nil {
		t.Fatalf("WriteDir default concurrency: %v", err)
	}
	dr, err := OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	defer dr.Close()

	colP, err := dr.OpenColumn("p")
	if err != nil {
		t.Fatalf("OpenColumn p: %v", err)
	}
	if got := colP.(*column.Base[float64]).Row(0); got != 7 {
		t.Fatalf("p[0] = %v, want 7", got)
	}
}

// TestWriteDirParallelSingleColumn ensures single-column input still works.
func TestWriteDirParallelSingleColumn(t *testing.T) {
	dir := t.TempDir()
	if err := WriteDir(dir, []column.ColumnCore{
		floatCol("solo", 42),
	}, WithConcurrency(4)); err != nil {
		t.Fatalf("WriteDir single column: %v", err)
	}
	dr, err := OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	defer dr.Close()

	col, err := dr.OpenColumn("solo")
	if err != nil {
		t.Fatalf("OpenColumn: %v", err)
	}
	if got := col.(*column.Base[float64]).Row(0); got != 42 {
		t.Fatalf("solo[0] = %v, want 42", got)
	}
}

// TestOpenColumns verifies parallel multi-column read: order matches names,
// values are correct, and unknown names produce an error.
func TestOpenColumns(t *testing.T) {
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

	// Read a subset in a specific order.
	cols, err := dr.OpenColumns([]string{"weight", "cpm"})
	if err != nil {
		t.Fatalf("OpenColumns: %v", err)
	}
	if len(cols) != 2 {
		t.Fatalf("OpenColumns returned %d cols, want 2", len(cols))
	}
	// First result must be "weight".
	if got := cols[0].(*column.Base[float64]).Row(2); got != 30 {
		t.Fatalf("weight[2] = %v, want 30", got)
	}
	// Second result must be "cpm".
	if got := cols[1].(*column.Base[float64]).Row(0); got != 1.5 {
		t.Fatalf("cpm[0] = %v, want 1.5", got)
	}

	// All three columns.
	all, err := dr.OpenColumns([]string{"cpm", "bidder", "weight"})
	if err != nil {
		t.Fatalf("OpenColumns all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("OpenColumns all returned %d cols, want 3", len(all))
	}
	if got := all[1].(*column.String).Row(1); got != "bb" {
		t.Fatalf("bidder[1] = %q, want %q", got, "bb")
	}

	// Unknown name must error.
	if _, err := dr.OpenColumns([]string{"cpm", "nonexistent"}); err == nil {
		t.Fatal("expected error for unknown column name")
	}
}

// TestOpenColumnsEmpty ensures OpenColumns with no names returns nil, nil.
func TestOpenColumnsEmpty(t *testing.T) {
	dir := t.TempDir()
	if err := WriteDir(dir, []column.ColumnCore{floatCol("v", 1)}); err != nil {
		t.Fatal(err)
	}
	dr, err := OpenDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer dr.Close()

	cols, err := dr.OpenColumns(nil)
	if err != nil {
		t.Fatalf("OpenColumns empty: %v", err)
	}
	if cols != nil {
		t.Fatalf("expected nil slice for empty names, got %v", cols)
	}
}

// TestWriteDirParallelCompressed exercises concurrent codec use:
// WithZSTD + WithConcurrency(4) then reads back all columns.
func TestWriteDirParallelCompressed(t *testing.T) {
	dir := t.TempDir()
	if err := WriteDir(dir, []column.ColumnCore{
		floatCol("cpm", 1.5, 2.5, 3.5),
		strCol("bidder", "a", "bb", "ccc"),
		floatCol("weight", 10, 20, 30),
		strCol("zone", "us", "eu", "ap"),
	}, WithZSTD(), WithConcurrency(4)); err != nil {
		t.Fatalf("WriteDir compressed parallel: %v", err)
	}

	dr, err := OpenDir(dir, WithZSTD())
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	defer dr.Close()

	if dr.RowCount() != 3 {
		t.Fatalf("RowCount = %d, want 3", dr.RowCount())
	}

	cols, err := dr.OpenColumns([]string{"zone", "cpm", "bidder", "weight"})
	if err != nil {
		t.Fatalf("OpenColumns compressed: %v", err)
	}
	if got := cols[0].(*column.String).Row(1); got != "eu" {
		t.Fatalf("zone[1] = %q, want eu", got)
	}
	if got := cols[1].(*column.Base[float64]).Row(2); got != 3.5 {
		t.Fatalf("cpm[2] = %v, want 3.5", got)
	}
	if got := cols[2].(*column.String).Row(0); got != "a" {
		t.Fatalf("bidder[0] = %q, want a", got)
	}
	if got := cols[3].(*column.Base[float64]).Row(1); got != 20 {
		t.Fatalf("weight[1] = %v, want 20", got)
	}
}

func TestWriteDirEmpty(t *testing.T) {
	if err := WriteDir(t.TempDir(), nil); err != nil {
		t.Fatalf("WriteDir empty: %v", err)
	}
}

func TestWriteDirDuplicateName(t *testing.T) {
	a := floatCol("dup", 1, 2)
	b := floatCol("dup", 3, 4)
	if err := WriteDir(t.TempDir(), []column.ColumnCore{a, b}); err == nil {
		t.Fatal("expected duplicate column name error from WriteDir")
	}
}
