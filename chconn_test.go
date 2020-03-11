package chconn

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "CHX_TEST_TCP_CONN_STRING")
	}

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))

	// conn.conn.Close()
}

func TestEndOfStream(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "CHX_TEST_TCP_CONN_STRING")
	}

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))
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
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "CHX_TEST_TCP_CONN_STRING")
	}

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

func TestTlsConnect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_TLS_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "CHX_TEST_TCP_TLS_CONN_STRING")
	}

	conn, err := Connect(context.Background(), connString)

	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))

	if _, ok := conn.conn.(*tls.Conn); !ok {
		t.Error("not a TLS connection")
	}

	conn.conn.Close()
}
