package chconn

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))

	require.NotEmpty(t, conn.ServerInfo().String())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.Nil(t, conn.Close(ctx))
	require.True(t, conn.IsClosed())
	// test protected two close

	require.Nil(t, conn.Close(context.Background()))
}

func TestConnectError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)
	config.Password = "invalid password"
	config.User = "invalid username"
	conn, err := ConnectConfig(context.Background(), config)
	assert.EqualError(t,
		err,
		strings.Join([]string{
			"failed to connect to `host=127.0.0.1 user=invalid username database=default`: ",
			"server error ( DB::Exception (516): invalid username: Authentication failed: password is incorrect or there is no user with such name)",
		}, ""))
	assert.EqualError(t,
		errors.Unwrap(err),
		" DB::Exception (516): invalid username: Authentication failed: password is incorrect or there is no user with such name")
	assert.Nil(t, conn)

	conn, err = Connect(context.Background(), "host>0")
	assert.EqualError(t,
		err,
		"cannot parse `host>0`: failed to parse as DSN (invalid dsn)")
	assert.Nil(t, conn)

	conn, err = Connect(context.Background(), "host=invalid_host")
	assert.EqualError(t,
		err,
		"failed to connect to `host=invalid_host user=default database=default`: hostname resolving error (lookup invalid_host: no such host)")
	assert.Nil(t, conn)

	conn, err = Connect(context.Background(), "host=invalid_host")
	assert.EqualError(t,
		err,
		"failed to connect to `host=invalid_host user=default database=default`: hostname resolving error (lookup invalid_host: no such host)")
	assert.Nil(t, conn)

	config, err = ParseConfig(connString)
	require.NoError(t, err)
	config.Port = 63666

	conn, err = ConnectConfig(context.Background(), config)
	assert.EqualError(t,
		err,
		"failed to connect to `host=127.0.0.1 user=default database=default`: dial error (dial tcp 127.0.0.1:63666: connect: connection refused)")
	assert.Nil(t, conn)

	config, err = ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	config.AfterConnect = func(ctx context.Context, c Conn) error {
		return errors.New("afterConnect err")
	}

	_, err = ConnectConfig(context.Background(), config)
	assert.EqualError(t,
		err,
		"failed to connect to `host=127.0.0.1 user=default database=default`: AfterConnect error (afterConnect err)")

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

	res, err := conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS example (
				country_code FixedString(2),
				os_id        UInt8,
				browser_id   UInt8,
				categories   Array(Int16),
				action_day   Date,
				action_time  DateTime
			) engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)
}

func TestException(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))
	res, err := conn.Exec(context.Background(), `invalid query`)

	require.Nil(t, res)
	var chError *ChError
	require.True(t, errors.As(err, &chError))
	require.Equal(t, chError.Code, int32(62))
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

func TestTlsVerifyCAConnect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_TLS_CONN_CA_STRING")

	_, err := Connect(context.Background(), connString)

	// there is certificate signed by unknown authority error
	// later maybe complete by valid ca
	require.Error(t, err)
}

func TestConnectConfigRequiresConnConfigFromParseConfig(t *testing.T) {
	t.Parallel()

	config := &Config{}

	require.PanicsWithValue(t, "config must be created by ParseConfig", func() {
		ConnectConfig(context.Background(), config) //nolint:errcheck not needed
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
	require.Nil(t, resSelect)
	require.EqualError(t, c.(*conn).lock(), "conn uninitialized")
}

func TestUlockError(t *testing.T) {
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
	res, err := c.Exec(context.Background(), "SET enable_http_compression=1")
	require.EqualError(t, err, "conn uninitialized")
	require.Nil(t, res)
	require.EqualError(t, c.(*conn).lock(), "conn uninitialized")
	c.Close(context.Background())

	config.WriterFunc = func(w io.Writer) io.Writer {
		return &writerErrorHelper{
			err:         errors.New("timeout"),
			w:           w,
			numberValid: 1,
		}
	}
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	res, err = c.Exec(context.Background(), "SET enable_http_compression=1")
	require.EqualError(t, err, "block: write block info (timeout)")
	require.Nil(t, res)
}

func TestRecivePackError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	config.ReaderFunc = func(r io.Reader) io.Reader {
		return &readErrorHelper{
			err:         errors.New("timeout"),
			r:           r,
			numberValid: 13,
		}
	}
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	res, err := c.Exec(context.Background(), `SELECT * FROM system.numbers limit 1`)
	require.EqualError(t, err, "packet: read packet type (timeout)")
	require.Nil(t, res)
}
