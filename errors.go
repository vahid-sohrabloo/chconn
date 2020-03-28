package chconn

import (
	"fmt"
	"strings"
)

// ChError represents an error reported by the Clickhouse server
type ChError struct {
	Code       int32
	Name       string
	Message    string
	StackTrace string
	err        error
}

func (e *ChError) read(r *Reader) error {
	var (
		err       error
		hasNested bool
	)
	if e.Code, err = r.Int32(); err != nil {
		return &readError{"ChError: read code", err}
	}
	if e.Name, err = r.String(); err != nil {
		return &readError{"ChError: read name", err}
	}
	if e.Message, err = r.String(); err != nil {
		return &readError{"ChError: read message", err}
	}
	e.Message = strings.TrimSpace(strings.TrimPrefix(e.Message, e.Name+":"))
	if e.StackTrace, err = r.String(); err != nil {
		return &readError{"ChError: read StackTrace", err}
	}
	if hasNested, err = r.Bool(); err != nil {
		return &readError{"ChError: read hasNested", err}
	}
	if hasNested {
		nestedErr := &ChError{}
		if err := nestedErr.read(r); err != nil {
			return err
		}

		e.err = nestedErr
	}
	return nil
}

func (e *ChError) Unwrap() error {
	return e.err
}

func (e *ChError) Error() string {
	if e.err == nil {
		return fmt.Sprintf(" %s (%d): %s", e.Name, e.Code, e.Message)
	}
	return fmt.Sprintf(" %s (%d): %s (%s)", e.Name, e.Code, e.Message, e.err)
}

type unexpectedPacket struct {
	expected string
	actual   interface{}
}

func (e *unexpectedPacket) Error() string {
	return fmt.Sprintf("Unexpected packet from server (expected %s got %#v)", e.expected, e.actual)
}

type connectError struct {
	config *Config
	msg    string
	err    error
}

func (e *connectError) Error() string {
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "failed to connect to `host=%s user=%s database=%s`: %s", e.config.Host, e.config.User, e.config.Database, e.msg)
	if e.err != nil {
		fmt.Fprintf(sb, " (%s)", e.err.Error())
	}
	return sb.String()
}

func (e *connectError) Unwrap() error {
	return e.err
}

type connLockError struct {
	status string
}

func (e *connLockError) Error() string {
	return e.status
}

type parseConfigError struct {
	connString string
	msg        string
	err        error
}

func (e *parseConfigError) Error() string {
	return fmt.Sprintf("cannot parse `%s`: %s (%s)", e.connString, e.msg, e.err.Error())
}

func (e *parseConfigError) Unwrap() error {
	return e.err
}

type readError struct {
	msg string
	err error
}

func (e *readError) Error() string {
	return fmt.Sprintf("%s (%s)", e.msg, e.err.Error())
}

func (e *readError) Unwrap() error {
	return e.err
}

type writeError struct {
	msg string
	err error
}

func (e *writeError) Error() string {
	return fmt.Sprintf("%s (%s)", e.msg, e.err.Error())
}

func (e *writeError) Unwrap() error {
	return e.err
}
