package chpool

import (
	"bytes"
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
)

// TestMain runs the goroutineleak profile after the package's tests finish.
//
// The check has to live here rather than in a Test function: most pool tests
// call t.Parallel(), and Go holds parallel tests until every sequential test
// has returned, so a plain test would be evaluated before they run and would
// audit nothing.
//
// The Go 1.27 goroutineleak profile finds goroutines blocked on a concurrency
// primitive unreachable from any runnable goroutine, which is the shape a
// leaked pooled connection takes. The baseline is zero, so any leak fails the
// run; relax this to a warning if it proves noisy on some platform.
func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 {
		if report, n := goroutineLeakReport(); n > 0 {
			fmt.Fprintf(os.Stderr, "\n%d leaked goroutine(s) after chpool tests:\n%s\n", n, report)
			code = 1
		}
	}
	os.Exit(code)
}

// goroutineLeakReport returns the human-readable leak profile and the number of
// leaked goroutines. The analysis runs inside WriteTo; Profile.Count reports 0
// until it has.
func goroutineLeakReport() (report string, leaked int) {
	p := pprof.Lookup("goroutineleak")
	if p == nil {
		return "", 0
	}
	var buf bytes.Buffer
	if err := p.WriteTo(&buf, 1); err != nil {
		return fmt.Sprintf("write leak profile: %v", err), 0
	}
	return buf.String(), p.Count()
}
