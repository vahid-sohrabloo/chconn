package chconn

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

// ErrNegativeTimeout when negative timeout provided
var ErrNegativeTimeout = errors.New("negative timeout")

// ErrPortInvalid when privide out of range port
var ErrPortInvalid = errors.New("outside range")

// ErrSSLModeInvalid when privide invalid ssl mode
var ErrSSLModeInvalid = errors.New("sslmode is invalid")

// ErrAddCA when can't add ca
var ErrAddCA = errors.New("unable to add CA to cert pool")

// ErrMissCertReqirement when sslcert or sslkey not provided
var ErrMissCertReqirement = errors.New(`both "sslcert" and "sslkey" are required`)

// ErrInvalidDSN for invalid dsn
var ErrInvalidDSN = errors.New("invalid dsn")

// ErrInvalidBackSlash invalid backslash in dsn
var ErrInvalidBackSlash = errors.New("invalid backslash")

// ErrInvalidquoted invalid quoted in dsn
var ErrInvalidquoted = errors.New("unterminated quoted string in connection info string")

// ErrIPNotFound when can't found ip in connecting
var ErrIPNotFound = errors.New("ip addr wasn't found")

var ErrInsertMinColumn = errors.New("you should pass at least one column")

// ChError represents an error reported by the Clickhouse server
type ChError struct {
	Code       int32
	Name       string
	Message    string
	StackTrace string
	err        error
}

func (e *ChError) read(r *readerwriter.Reader) error {
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

type notImplementedPacket struct {
	packet uint64
}

func (e *notImplementedPacket) Error() string {
	return fmt.Sprintf("packet not implemented: %d", e.packet)
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
	connString := redactPW(e.connString)
	if e.err == nil {
		return fmt.Sprintf("cannot parse `%s`: %s", connString, e.msg)
	}
	return fmt.Sprintf("cannot parse `%s`: %s (%s)", connString, e.msg, e.err.Error())
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

func redactPW(connString string) string {
	if strings.HasPrefix(connString, "clickhouse://") {
		if u, err := url.Parse(connString); err == nil {
			return redactURL(u)
		}
	}
	quotedDSN := regexp.MustCompile(`password='[^']*'`)
	connString = quotedDSN.ReplaceAllLiteralString(connString, "password=xxxxx")
	plainDSN := regexp.MustCompile(`password=[^ ]*`)
	connString = plainDSN.ReplaceAllLiteralString(connString, "password=xxxxx")
	return connString
}

func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	if _, pwSet := u.User.Password(); pwSet {
		u.User = url.UserPassword(u.User.Username(), "xxxxx")
	}
	return u.String()
}

// InsertError represents an error when insert error
type InsertError struct {
	Block *Block
	err   error
}

func (e *InsertError) Error() string {
	return fmt.Sprintf("failed to insert data : %s", e.err.Error())
}

func (e *InsertError) Unwrap() error {
	return e.err
}

// ColumnNumberReadError represents an error when read more or less column
type ColumnNumberReadError struct {
	Read      int
	Available uint64
}

func (e *ColumnNumberReadError) Error() string {
	return fmt.Sprintf("read %d column(s), but available %d column(s)", e.Read, e.Available)
}

// ColumnNumberReadError represents an error when number of write column is not equal to number of query column
type ColumnNumberWriteError struct {
	WriteColumn int
	NeedColumn  uint64
}

func (e *ColumnNumberWriteError) Error() string {
	return fmt.Sprintf("write %d column(s) but insert query needs %d column(s)", e.WriteColumn, e.NeedColumn)
}

type NumberWriteError struct {
	FirstNumRow int
	NumRow      int
	Column      string
}

func (e *NumberWriteError) Error() string {
	return fmt.Sprintf("first column has %d rows but \"%s\"  column has %d rows", e.FirstNumRow, e.Column, e.NumRow)
}
