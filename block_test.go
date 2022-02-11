package chconn

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockReadError(t *testing.T) {
	startValidReader := 15

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "blockInfo: read field1",
			wantErr:     "block: temporary table",
			numberValid: startValidReader - 1,
		}, {
			name:        "blockInfo: read field1",
			wantErr:     "blockInfo: read field1",
			numberValid: startValidReader,
		}, {
			name:        "blockInfo: read isOverflows",
			wantErr:     "blockInfo: read isOverflows",
			numberValid: startValidReader + 1,
		}, {
			name:        "blockInfo: read field2",
			wantErr:     "blockInfo: read field2",
			numberValid: startValidReader + 2,
		}, {
			name:        "blockInfo: read bucketNum",
			wantErr:     "blockInfo: read bucketNum",
			numberValid: startValidReader + 3,
		}, {
			name:        "blockInfo: read num3",
			wantErr:     "blockInfo: read num3",
			numberValid: startValidReader + 4,
		}, {
			name:        "block: read NumColumns",
			wantErr:     "block: read NumColumns",
			numberValid: startValidReader + 5,
		}, {
			name:        "block: read NumRows",
			wantErr:     "block: read NumRows",
			numberValid: startValidReader + 6,
		}, {
			name:        "block: read column name",
			wantErr:     "block: read column name",
			numberValid: startValidReader + 8,
		}, {
			name:        "block: read column type",
			wantErr:     "block: read column type",
			numberValid: startValidReader + 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			stmt, err := c.Select(context.Background(), "SELECT * FROM system.numbers LIMIT 5;")
			require.NoError(t, err)
			require.False(t, stmt.Next())

			require.Error(t, stmt.Err())
			readErr, ok := stmt.Err().(*readError)
			require.True(t, ok)
			require.Equal(t, readErr.msg, tt.wantErr)
			require.EqualError(t, readErr.Unwrap(), "timeout")
			assert.True(t, c.IsClosed())
		})
	}
}
