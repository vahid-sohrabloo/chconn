package column_test

import (
	"io"
)

type readErrorHelper struct {
	numberValid int
	err         error
	r           io.Reader
	count       int
}

func (r *readErrorHelper) Read(p []byte) (int, error) {
	r.count++
	if r.count > r.numberValid {
		return 0, r.err
	}
	return r.r.Read(p)
}
