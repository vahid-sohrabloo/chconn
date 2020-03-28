package chconn

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	errors "golang.org/x/xerrors"
)

func TestBlockReadError(t *testing.T) {
	startValidReader := 14

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "block: read field1",
			wantErr:     "block: read field1",
			numberValid: startValidReader,
		}, {
			name:        "block: read isOverflows",
			wantErr:     "block: read isOverflows",
			numberValid: startValidReader + 1,
		}, {
			name:        "block: read field2",
			wantErr:     "block: read field2",
			numberValid: startValidReader + 2,
		}, {
			name:        "block: read bucketNum",
			wantErr:     "block: read bucketNum",
			numberValid: startValidReader + 3,
		}, {
			// beacuse varint need two read
			name:        "block: read num3",
			wantErr:     "block: read num3",
			numberValid: startValidReader + 4,
		}, {
			name:        "block: read NumColumns",
			wantErr:     "block: read NumColumns",
			numberValid: startValidReader + 6,
		}, {
			name:        "block: read NumColumns",
			wantErr:     "block: read NumColumns",
			numberValid: startValidReader + 6,
		}, {
			name:        "block: read NumRows",
			wantErr:     "block: read NumRows",
			numberValid: startValidReader + 7,
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

		})
	}
}
