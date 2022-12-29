package chconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
)

func TestProfileReadError(t *testing.T) {
	startValidReader := 43
	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	if c.ServerInfo().Revision >= helper.DbmsMinProtocolWithServerQueryTimeInProgress {
		// todo we need to fix this for clickhouse 22.10 and above
		return
	}

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "profile: read Rows",
			wantErr:     "profile: read Rows",
			numberValid: startValidReader,
		}, {
			name:        "profile: read Blocks",
			wantErr:     "profile: read Blocks",
			numberValid: startValidReader + 1,
		}, {
			name:        "profile: read Bytes",
			wantErr:     "profile: read Bytes",
			numberValid: startValidReader + 2,
		}, {
			name:        "profile: read AppliedLimit",
			wantErr:     "profile: read AppliedLimit",
			numberValid: startValidReader + 3,
		}, {
			name:        "profile: read RowsBeforeLimit",
			wantErr:     "profile: read RowsBeforeLimit",
			numberValid: startValidReader + 4,
		}, {
			name:        "profile: read CalculatedRowsBeforeLimit",
			wantErr:     "profile: read CalculatedRowsBeforeLimit",
			numberValid: startValidReader + 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)

			if c.ServerInfo().Revision >= helper.DbmsMinProtocolWithServerQueryTimeInProgress {
				tt.numberValid++
			}

			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			col := column.New[uint64]()
			stmt, err := c.Select(context.Background(), "SELECT * FROM system.numbers LIMIT 1;", col)
			require.NoError(t, err)
			for stmt.Next() {
			}
			require.Error(t, stmt.Err())
			readErr, ok := stmt.Err().(*readError)
			require.True(t, ok)
			fmt.Println("readErr.msg:", readErr.msg)
			require.Equal(t, tt.wantErr, readErr.msg)
			require.EqualError(t, readErr.Unwrap(), "timeout")
			assert.True(t, c.IsClosed())
		})
	}
}
