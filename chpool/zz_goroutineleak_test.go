package chpool

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime/pprof"
	"testing"
)

// TestGoroutineLeakProfile reports goroutines left permanently blocked once the
// rest of the pool tests have run. It is named with a zz_ prefix so it runs last.
//
// The Go 1.27 goroutineleak profile finds goroutines blocked on a concurrency
// primitive that is unreachable from any runnable goroutine, which is the shape
// a leaked pooled connection takes. The baseline is zero, so any leak fails
// the test; relax this to t.Logf if it proves noisy on some platform.
func TestGoroutineLeakProfile(t *testing.T) {
	p := pprof.Lookup("goroutineleak")
	if p == nil {
		t.Skip("goroutineleak profile not available")
	}

	// The leak analysis runs inside WriteTo. Profile.Count reports 0 until it has.
	var buf bytes.Buffer
	if err := p.WriteTo(&buf, 1); err != nil {
		t.Fatalf("write leak profile: %v", err)
	}

	n := p.Count()
	if n == 0 {
		return
	}

	path := filepath.Join(t.ArtifactDir(), "goroutineleak.pb.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create leak profile: %v", err)
	}
	defer f.Close()
	if err := p.WriteTo(f, 0); err != nil {
		t.Fatalf("write leak profile: %v", err)
	}
	t.Errorf("%d leaked goroutine(s):\n%s\nprofile written to %s (run with -artifacts to retain it)",
		n, buf.String(), path)
}
