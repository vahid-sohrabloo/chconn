package chconn

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChErrorReadError(t *testing.T) {
	startValidReader := 14

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "ChError: read code",
			wantErr:     "ChError: read code",
			numberValid: startValidReader,
		}, {
			name:        "ChError: read name",
			wantErr:     "ChError: read name",
			numberValid: startValidReader + 1,
		}, {
			name:        "ChError: read message",
			wantErr:     "ChError: read message",
			numberValid: startValidReader + 3,
		}, {
			name:        "ChError: read StackTrace",
			wantErr:     "ChError: read StackTrace",
			numberValid: startValidReader + 5,
		}, {
			name:        "ChError: read hasNested",
			wantErr:     "ChError: read hasNested",
			numberValid: startValidReader + 8,
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
			require.NoError(t, err)
			_, err = c.Exec(context.Background(), "SELECT * FROM invalid_table LIMIT 5;")
			require.Error(t, err)
			readErr, ok := err.(*readError)
			require.True(t, ok)
			require.Equal(t, readErr.msg, tt.wantErr)
			require.EqualError(t, readErr.Unwrap(), "timeout")
		})
	}
}
