package format

import "runtime"

// config holds resolved writer/reader options.
type config struct {
	codec       *Codec
	concurrency int
}

// Option configures native readers/writers.
type Option func(*config)

// WithZSTD stores/reads the file as zstd-compressed per-block units. Incompatible
// with zero-copy (BytesReader); use the streaming OpenFile path with this option.
func WithZSTD() Option { return func(c *config) { c.codec = zstdCodec() } }

// WithLZ4 stores/reads the file as lz4-compressed per-block units. Incompatible
// with zero-copy (BytesReader); use the streaming OpenFile path with this option.
func WithLZ4() Option { return func(c *config) { c.codec = lz4Codec() } }

// WithCodec stores/reads the file using a custom pluggable codec. Incompatible
// with zero-copy (BytesReader); use the streaming OpenFile path with this option.
func WithCodec(codec Codec) Option { return func(c *config) { cc := codec; c.codec = &cc } }

// WithConcurrency sets the number of worker goroutines for parallel dir-mode
// writes and reads. n <= 0 uses runtime.NumCPU().
func WithConcurrency(n int) Option { return func(c *config) { c.concurrency = n } }

func resolve(opts []Option) config {
	var c config
	for _, o := range opts {
		o(&c)
	}
	return c
}

// workers returns the effective worker count: the configured value if positive,
// otherwise runtime.NumCPU().
func (c config) workers() int {
	if c.concurrency > 0 {
		return c.concurrency
	}
	return runtime.NumCPU()
}
