package chconn

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

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

	c.Close()

	config.WriterFunc = nil

	config.ReaderFunc = func(r io.Reader) io.Reader {
		return &readErrorHelper{
			err:         errors.New("timeout"),
			r:           r,
			numberValid: 13,
		}
	}

	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	require.EqualError(t, c.Ping(context.Background()), "packet: read packet type (timeout)")
}
