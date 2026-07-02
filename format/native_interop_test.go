package format

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func clickhouseBin(t *testing.T) string {
	for _, name := range []string{"clickhouse-local", "clickhouse"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	t.Skip("clickhouse-local not found; skipping interop test")
	return ""
}

// Write a Native file here, then have ClickHouse read it and emit TSV.
func TestInteropClickHouseReadsOurNative(t *testing.T) {
	bin := clickhouseBin(t)
	path := filepath.Join(t.TempDir(), "out.native")
	if err := WriteFile(path, []column.ColumnCore{
		floatCol("cpm", 1.5, 2.5),
		strCol("bidder", "alpha", "beta"),
	}); err != nil {
		t.Fatal(err)
	}

	args := []string{}
	if strings.HasSuffix(bin, "clickhouse") {
		args = append(args, "local")
	}
	args = append(args,
		"--query",
		"SELECT cpm, bidder FROM file('"+path+"', Native, 'cpm Float64, bidder String') ORDER BY cpm FORMAT TSV",
	)
	var out bytes.Buffer
	cmd := exec.Command(bin, args...)
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("clickhouse read failed: %v", err)
	}
	got := strings.TrimSpace(out.String())
	want := "1.5\talpha\n2.5\tbeta"
	if got != want {
		t.Fatalf("interop mismatch:\n got: %q\nwant: %q", got, want)
	}
}
