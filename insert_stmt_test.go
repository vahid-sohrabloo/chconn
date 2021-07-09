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
	_, err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_error (
				int8  Int8
			) Engine=Memory`)

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

func TestInsertWriteFlushError(t *testing.T) {
	startValid := 2

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "write block info",
			wantErr:     "block: write block info",
			numberValid: startValid,
		}, {
			name:        "write block info",
			wantErr:     "block: write block data for column int8",
			numberValid: startValid + 1,
		}, {
			name:        "write block info step 2",
			wantErr:     "block: write block info",
			numberValid: startValid + 2,
		}, {
			name:        "write block info step 3",
			wantErr:     "block: write block info",
			numberValid: startValid + 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)

			c, err := ConnectConfig(context.Background(), config)
			require.NoError(t, err)

			_, err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_write_error`)
			require.NoError(t, err)
			_, err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_write_error (
				int8  Int8
			) Engine=Memory`)
			require.NoError(t, err)

			c.Close(context.Background())

			config.WriterFunc = func(w io.Writer) io.Writer {
				return &writerErrorHelper{
					err:         errors.New("timeout"),
					w:           w,
					numberValid: tt.numberValid,
				}
			}

			c, err = ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			stmt, err := c.Insert(context.Background(), "insert into clickhouse_test_insert_write_error (int8) values ")
			require.NoError(t, err)
			stmt.AddRow(1)
			stmt.Int8(0, 1)
			err = stmt.Flush(context.Background())
			require.Error(t, err)
			insertErr, ok := err.(*InsertError)
			var writeErr *writeError
			if ok {
				writeErr, ok = insertErr.Unwrap().(*writeError)
				require.True(t, ok)
			} else {
				writeErr, ok = err.(*writeError)
				require.True(t, ok)
			}
			require.True(t, ok)
			require.Equal(t, writeErr.msg, tt.wantErr)
			require.EqualError(t, writeErr.Unwrap(), "timeout")
			c.Close(context.Background())
		})
	}
}

func TestInsertReadFlushError(t *testing.T) {
	startValid := 31

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "write block info",
			wantErr:     "packet: read packet type",
			numberValid: startValid,
		}, {
			name:        "write block info step 2",
			wantErr:     "packet: read packet type",
			numberValid: startValid + 1,
		}, {
			name:        "write block info",
			wantErr:     "block: read column name",
			numberValid: startValid + 14,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)

			c, err := ConnectConfig(context.Background(), config)
			require.NoError(t, err)

			_, err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_write_error`)
			require.NoError(t, err)
			_, err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_write_error (
				int8  Int8
			) Engine=Memory`)
			require.NoError(t, err)

			c.Close(context.Background())

			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err = ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			stmt, err := c.Insert(context.Background(), "insert into clickhouse_test_insert_write_error (int8) values ")
			require.NoError(t, err)
			stmt.AddRow(1)
			stmt.Int8(0, 1)
			err = stmt.Flush(context.Background())
			require.Error(t, err)
			readErr, ok := err.(*readError)
			require.True(t, ok)
			require.Equal(t, readErr.msg, tt.wantErr)
			require.EqualError(t, readErr.Unwrap(), "timeout")
			c.Close(context.Background())
		})
	}
}

func TestInsertIPError(t *testing.T) {
	stmt := &insertStmt{
		block:      nil,
		conn:       nil,
		query:      "",
		queryID:    "",
		stage:      QueryProcessingStageComplete,
		settings:   nil,
		clientInfo: nil,
	}
	invalidIP := net.IP([]byte{1})
	assert.EqualError(t, stmt.IPv4(0, invalidIP), "invalid ipv4")
	assert.EqualError(t, stmt.IPv6(0, invalidIP), "invalid ipv6")
	assert.EqualError(t, stmt.IPv4P(0, &invalidIP), "invalid ipv4")
	assert.EqualError(t, stmt.IPv6P(0, &invalidIP), "invalid ipv6")
}
