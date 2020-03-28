package chconn

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

type writerErrorHelper struct {
	numberValid int
	err         error
	w           io.Writer
	count       int
}

func (w *writerErrorHelper) Write(p []byte) (int, error) {
	w.count++
	if w.count > w.numberValid {
		return 0, w.err
	}
	return w.w.Write(p)
}
