package chconn

import (
	"context"
	"fmt"
	"net"
	"strings"

	errors "golang.org/x/xerrors"
)

var ErrNotInsertQuery = errors.New("only insert query allowed")

// SafeToRetry checks if the err is guaranteed to have occurred before sending any data to the server.
func SafeToRetry(err error) bool {
	if e, ok := err.(interface{ SafeToRetry() bool }); ok {
		return e.SafeToRetry()
	}
	return false
}

// Timeout checks if err was was caused by a timeout. To be specific, it is true if err is or was caused by a
// context.Canceled, context.Canceled or an implementer of net.Error where Timeout() is true.
func Timeout(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

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
		return err
	}
	if e.Name, err = r.String(); err != nil {
		return err
	}
	if e.Message, err = r.String(); err != nil {
		return err
	}
	e.Message = strings.TrimSpace(strings.TrimPrefix(e.Message, e.Name+":"))
	if e.StackTrace, err = r.String(); err != nil {
		return err
	}
	if hasNested, err = r.Bool(); err != nil {
		return err
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
	expected uint64
	actual   uint64
}

func (e *unexpectedPacket) Error() string {
	return fmt.Sprintf("Unexpected packet from server  (expected %d got %d )", e.expected, e.actual)
}

type unexpectedMessage struct {
	expected string
	actual   interface{}
}

func (e *unexpectedMessage) Error() string {
	return fmt.Sprintf("Unexpected packet from server  (expected %s got %#v )", e.expected, e.actual)
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

func (e *connLockError) SafeToRetry() bool {
	return true // a lock failure by definition happens before the connection is used.
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
	if e.err == nil {
		return fmt.Sprintf("cannot parse `%s`: %s", e.connString, e.msg)
	}
	return fmt.Sprintf("cannot parse `%s`: %s (%s)", e.connString, e.msg, e.err.Error())
}

func (e *parseConfigError) Unwrap() error {
	return e.err
}
