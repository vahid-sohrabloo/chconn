package chconn

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING") + " connect_timeout=10"

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))

	require.NotEmpty(t, conn.ServerInfo().String())

	require.Nil(t, conn.Close())
	require.True(t, conn.IsClosed())
	// test protected two close

	require.Nil(t, conn.Close())
}

func TestConnectError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)
	config.Password = "invalid password"
	config.User = "invalid username"
	conn, err := ConnectConfig(context.Background(), config)
	assert.Contains(t,
		errors.Unwrap(err).Error(),
		"DB::Exception (516): invalid username: Authentication failed")
	assert.Nil(t, conn)

	conn, err = Connect(context.Background(), "host>0")
	assert.EqualError(t,
		err,
		"cannot parse `host>0`: failed to parse as DSN (invalid dsn)")
	assert.Nil(t, conn)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn, err = Connect(ctx, connString)
	assert.Error(t,
		errors.Unwrap(err),
		context.Canceled)
	assert.Nil(t, conn)

	conn, err = Connect(context.Background(), "host=invalid_host")
	assert.Contains(t,
		err.Error(),
		"hostname resolving error")
	assert.Nil(t, conn)

	config, err = ParseConfig(connString)
	require.NoError(t, err)
	config.Port = 63666

	conn, err = ConnectConfig(context.Background(), config)
	assert.Contains(t,
		err.Error(),
		"connect: connection refused")
	assert.Nil(t, conn)

	config, err = ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	config.AfterConnect = func(ctx context.Context, c Conn) error {
		return errors.New("afterConnect err")
	}

	_, err = ConnectConfig(context.Background(), config)
	assert.EqualError(t,
		errors.Unwrap(err),
		"afterConnect err")

	config, err = ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	config.WriterFunc = func(w io.Writer) io.Writer {
		return &writerErrorHelper{
			err:         errors.New("timeout"),
			w:           w,
			numberValid: 0,
		}
	}

	_, err = ConnectConfig(context.Background(), config)
	assert.EqualError(t, err, "write hello: timeout")
}

func TestEndOfStream(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	err = conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS example (
				country_code FixedString(2),
				os_id        UInt8,
				browser_id   UInt8,
				categories   Array(Int16),
				action_day   Date,
				action_time  DateTime
			) engine=Memory`)

	require.NoError(t, err)
}

func TestException(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))
	err = conn.Exec(context.Background(), `invalid query`)

	var chError *ChError
	require.True(t, errors.As(err, &chError))
	require.Equal(t, chError.Code, ChErrorSyntaxError)
	require.Equal(t, chError.Name, "DB::Exception")
}

func TestTlsPreferConnect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_TLS_CONN_STRING")

	if connString == "" {
		t.Skip("please set CHX_TEST_TCP_TLS_CONN_STRING env")
		return
	}

	conn, err := Connect(context.Background(), connString)

	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))

	if _, ok := conn.RawConn().(*tls.Conn); !ok {
		t.Error("not a TLS connection")
	}

	conn.RawConn().Close()
}

func TestConnectConfigRequiresConnConfigFromParseConfig(t *testing.T) {
	t.Parallel()

	config := &Config{}

	require.PanicsWithValue(t, "config must be created by ParseConfig", func() {
		ConnectConfig(context.Background(), config)
	})
}

func TestLockError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	c, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	c.(*conn).status = connStatusBusy
	require.EqualError(t, c.(*conn).lock(), "conn busy")

	c.(*conn).status = connStatusClosed
	require.EqualError(t, c.(*conn).lock(), "conn closed")

	c.(*conn).status = connStatusUninitialized
	require.EqualError(t, c.(*conn).lock(), "conn uninitialized")

	resSelect, err := c.Select(context.Background(), "SET enable_http_compression=1")
	require.EqualError(t, err, "conn uninitialized")
	require.NotNil(t, resSelect)
	require.EqualError(t, c.(*conn).lock(), "conn uninitialized")
}

func TestUnlockError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	c, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	c.(*conn).status = connStatusUninitialized
	require.PanicsWithValue(t, "BUG: cannot unlock unlocked connection", func() {
		c.(*conn).unlock()
	})
}

func TestExecError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	c.(*conn).status = connStatusUninitialized
	err = c.Exec(context.Background(), "SET enable_http_compression=1")
	require.EqualError(t, err, "conn uninitialized")
	require.EqualError(t, c.(*conn).lock(), "conn uninitialized")
	c.Close()

	config.WriterFunc = func(w io.Writer) io.Writer {
		return &writerErrorHelper{
			err:         errors.New("timeout"),
			w:           w,
			numberValid: 1,
		}
	}
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	err = c.Exec(context.Background(), "SET enable_http_compression=1")
	require.EqualError(t, err, "write block info (timeout)")
	assert.True(t, c.IsClosed())
}

func TestExecCtxError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = c.Exec(ctx, "select * from system.numbers limit 1")
	require.EqualError(t, err, "timeout: context already done: context canceled")
	assert.False(t, c.IsClosed())

	config.WriterFunc = func(w io.Writer) io.Writer {
		return &writerSlowHelper{
			w:     w,
			sleep: time.Second,
		}
	}
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	err = c.Exec(ctx, "select * from system.numbers")
	require.EqualError(t, errors.Unwrap(err), "context deadline exceeded")
	assert.True(t, c.IsClosed())
}

func TestReceivePackError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	config.ReaderFunc = func(r io.Reader, c Conn) io.Reader {
		return &readErrorHelper{
			err:         errors.New("timeout"),
			r:           r,
			numberValid: 15,
		}
	}
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	err = c.Exec(context.Background(), `SELECT * FROM system.numbers limit 1`)
	require.EqualError(t, err, "packet: read packet type (timeout)")
	assert.True(t, c.IsClosed())
}
