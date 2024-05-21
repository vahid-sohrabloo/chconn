package chconn

import (
	"io"
	"time"
)

type readErrorHelper struct {
	numberValid     int
	numberValidFunc func(Conn) int
	err             error
	r               io.Reader
	c               Conn
	count           int
}

func (r *readErrorHelper) Read(p []byte) (int, error) {
	r.count++
	if r.numberValidFunc != nil {
		r.numberValid = r.numberValidFunc(r.c)
	}
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

type writerSlowHelper struct {
	w     io.Writer
	sleep time.Duration
}

func (w *writerSlowHelper) Write(p []byte) (int, error) {
	time.Sleep(w.sleep)
	return w.w.Write(p)
}
