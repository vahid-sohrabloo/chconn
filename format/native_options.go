package format

// config holds resolved writer/reader options.
type config struct {
	lz4 bool
}

// Option configures native readers/writers.
type Option func(*config)

// WithLZ4 stores/reads the file as an lz4-compressed stream. Incompatible with
// zero-copy (BytesReader); use the streaming OpenFile path with this option.
func WithLZ4() Option { return func(c *config) { c.lz4 = true } }

func resolve(opts []Option) config {
	var c config
	for _, o := range opts {
		o(&c)
	}
	return c
}
