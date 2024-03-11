package chconn

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPing(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)
	require.NoError(t, conn.Ping(context.Background()))
	conn.Close()
}

func TestPingWriteError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	config.WriterFunc = func(w io.Writer) io.Writer {
		return &writerErrorHelper{
			err:         errors.New("timeout"),
			w:           w,
			numberValid: 1,
		}
	}
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	err = c.Ping(context.Background())
	require.EqualError(t, err, "ping: write packet type (timeout)")
	require.EqualError(t, errors.Unwrap(err), "timeout")

	assert.True(t, c.IsClosed())

	config.WriterFunc = nil

	config.ReaderFunc = func(r io.Reader, c Conn) io.Reader {
		return &readErrorHelper{
			err:         errors.New("timeout"),
			r:           r,
			numberValid: 15,
		}
	}

	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	require.EqualError(t, c.Ping(context.Background()), "packet: read packet type (timeout)")
	assert.True(t, c.IsClosed())
}

func TestPingCtxError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = c.Ping(ctx)
	require.EqualError(t, err, "timeout: context already done: context canceled")
	require.EqualError(t, errors.Unwrap(err), "context already done: context canceled")

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
	err = c.Ping(ctx)
	require.EqualError(t, errors.Unwrap(errors.Unwrap(err)), "context deadline exceeded")
	assert.True(t, c.IsClosed())
}
