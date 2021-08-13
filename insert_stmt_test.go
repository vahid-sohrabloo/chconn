package chconn

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/setting"
)

func TestInsertError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	// test lock error
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	c.(*conn).status = connStatusUninitialized
	res, err := c.Insert(context.Background(), "insert into system.numbers VALUES")
	require.Nil(t, res)
	require.EqualError(t, err, "conn uninitialized")
	require.EqualError(t, c.(*conn).lock(), "conn uninitialized")
	c.Close(context.Background())

	// test write block info error
	config.WriterFunc = func(w io.Writer) io.Writer {
		return &writerErrorHelper{
			err:         errors.New("timeout"),
			w:           w,
			numberValid: 1,
		}
	}
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	res, err = c.Insert(context.Background(), "insert into system.numbers VALUES")
	require.EqualError(t, err, "block: write block info (timeout)")
	require.Nil(t, res)

	// test insert server error
	config.WriterFunc = nil
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	res, err = c.Insert(context.Background(), "insert into system.numbers VALUES")
	require.EqualError(t, err, " DB::Exception (48): Method write is not supported by storage SystemNumbers")
	require.Nil(t, res)

	// test not block data error
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	res, err = c.Insert(context.Background(), "SET enable_http_compression=1")
	require.EqualError(t, err, "Unexpected packet from server (expected serverData got <nil>)")
	require.Nil(t, res)

	// test read column error
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	_, err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_error`)
	require.NoError(t, err)
	setting := setting.NewSettings()
	setting.JoinUseNulls(false)
	_, err = c.ExecWithSetting(context.Background(), `CREATE TABLE clickhouse_test_insert_error (
				int8  Int8
			) Engine=Memory`, setting)

	require.NoError(t, err)

	config.ReaderFunc = func(r io.Reader) io.Reader {
		return &readErrorHelper{
			err:         errors.New("timeout"),
			r:           r,
			numberValid: 27,
		}
	}
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	res, err = c.Insert(context.Background(), `INSERT INTO clickhouse_test_insert_error (
				int8
			) VALUES`)
	require.EqualError(t, err, "block: read column name (timeout)")
	require.Nil(t, res)
}

func TestInsertIPError(t *testing.T) {
	stmt := &InsertWriter{}
	invalidIP := net.IP([]byte{1})
	assert.EqualError(t, stmt.IPv4(0, invalidIP), "invalid ipv4")
	assert.EqualError(t, stmt.IPv6(0, invalidIP), "invalid ipv6")
	assert.EqualError(t, stmt.IPv4P(0, &invalidIP), "invalid ipv4")
	assert.EqualError(t, stmt.IPv6P(0, &invalidIP), "invalid ipv6")
}
