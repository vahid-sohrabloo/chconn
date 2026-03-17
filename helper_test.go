package chconn

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// helloReadsCount returns the number of reads needed to complete a successful
// hello handshake. This is used by error injection tests to determine the
// correct startValidReader value, avoiding hardcoded read counts that break
// when the protocol version changes.
func helloReadsCount(t *testing.T) int {
	t.Helper()
	var count int
	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	config.ReaderFunc = func(r io.Reader, _ Conn) io.Reader {
		return &readCounterHelper{r: r, count: &count}
	}
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	c.Close()
	return count
}

// progressReadsCount returns the exact number of valid reads after which the
// next read would fail on "read ReadRows" in a progress packet. Used by
// progress error tests. Dynamically discovered to be version-agnostic.
func progressReadsCount(t *testing.T) int {
	t.Helper()
	var count int
	var progressAt int
	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	config.ReaderFunc = func(r io.Reader, _ Conn) io.Reader {
		return &readCounterHelper{r: r, count: &count}
	}
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	colSleep := column.New[uint8]()
	colNumber := column.New[uint64]()
	res, err := c.SelectWithOption(context.Background(),
		"SELECT sleep(1), * FROM system.numbers LIMIT 1",
		&QueryOptions{
			OnProgress: func(_ *Progress) {
				if progressAt == 0 {
					progressAt = count
				}
			},
		},
		colSleep, colNumber,
	)
	require.NoError(t, err)
	for res.Next() {
	}
	require.NoError(t, res.Err())
	c.Close()
	require.Greater(t, progressAt, 0, "no progress packet received")

	// Dynamically find the exact position where injecting an error produces
	// "progress: read ReadRows". Search backward from progressAt.
	for n := progressAt - 1; n >= progressAt-15; n-- {
		if progressErrorAt(t, n) == "progress: read ReadRows (timeout)" {
			return n
		}
	}
	t.Fatal("could not find progressReadsCount: ReadRows error position not found")
	return 0
}

// progressErrorAt injects a timeout at read n+1 and returns the error message.
func progressErrorAt(t *testing.T, n int) string {
	t.Helper()
	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	if err != nil {
		return ""
	}
	config.ReaderFunc = func(r io.Reader, _ Conn) io.Reader {
		return &readErrorHelper{err: errTestTimeout, r: r, numberValid: n}
	}
	c, err := ConnectConfig(context.Background(), config)
	if err != nil {
		return ""
	}
	colSleep := column.New[uint8]()
	colNumber := column.New[uint64]()
	res, err := c.SelectWithOption(context.Background(),
		"SELECT sleep(1), * FROM system.numbers LIMIT 1",
		&QueryOptions{OnProgress: func(_ *Progress) {}},
		colSleep, colNumber,
	)
	if err != nil {
		c.Close()
		return err.Error()
	}
	for res.Next() {
	}
	c.Close()
	if res.Err() != nil {
		return res.Err().Error()
	}
	return ""
}

// insertColumnNameReadsCount returns the number of reads needed to reach the
// column name read during an INSERT. Used by insert error injection tests.
func insertColumnNameReadsCount(t *testing.T) int {
	t.Helper()
	// Do a successful insert and count reads up to a known point.
	// We create a table, do the insert, and count total reads for the whole
	// insert flow. Then we binary search for the exact read that triggers
	// "read column name" error.
	var totalCount int
	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	config.ReaderFunc = func(r io.Reader, _ Conn) io.Reader {
		return &readCounterHelper{r: r, count: &totalCount}
	}
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	helloReads := totalCount

	col := column.New[int8]()
	col.Append(1)
	err = c.Insert(context.Background(),
		`INSERT INTO clickhouse_test_insert_error (int8) VALUES`, col)
	require.NoError(t, err)
	c.Close()

	// Now find the exact read that produces "read column name" error.
	// Try each position from helloReads to totalCount.
	for n := helloReads; n < totalCount; n++ {
		cfg, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
		require.NoError(t, err)
		cfg.ReaderFunc = func(r io.Reader, _ Conn) io.Reader {
			return &readErrorHelper{
				err:         errTestTimeout,
				r:           r,
				numberValid: n,
			}
		}
		c, err := ConnectConfig(context.Background(), cfg)
		if err != nil {
			continue
		}
		err = c.Insert(context.Background(),
			`INSERT INTO clickhouse_test_insert_error (int8) VALUES`)
		if err != nil && err.Error() == "read column name: read string length: timeout" {
			return n
		}
	}
	t.Fatal("could not find insertColumnNameReadsCount")
	return 0
}

var errTestTimeout = errors.New("timeout")

// readCounterHelper counts reads without injecting errors.
type readCounterHelper struct {
	r     io.Reader
	count *int
}

func (r *readCounterHelper) Read(p []byte) (int, error) {
	*r.count++
	return r.r.Read(p)
}

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
