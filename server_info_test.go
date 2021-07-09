package chconn

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerInfoError(t *testing.T) {
	startValidReader := 1

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "server name",
			wantErr:     "ServerInfo: could not read server name",
			numberValid: startValidReader,
		}, {
			name:        "server major version",
			wantErr:     "ServerInfo: could not read server major version",
			numberValid: startValidReader + 2,
		}, {
			name:        "server minor version",
			wantErr:     "ServerInfo: could not read server minor version",
			numberValid: startValidReader + 3,
		}, {
			name:        "server revision",
			wantErr:     "ServerInfo: could not read server revision",
			numberValid: startValidReader + 4,
		}, {
			name:        "server timezone",
			wantErr:     "ServerInfo: could not read server timezone",
			numberValid: startValidReader + 7,
		}, {
			name:        "server display name",
			wantErr:     "ServerInfo: could not read server display name",
			numberValid: startValidReader + 9,
		}, {
			name:        "server version patch",
			wantErr:     "ServerInfo: could not read server version patch",
			numberValid: startValidReader + 11,
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

			_, err = ConnectConfig(context.Background(), config)
			require.Error(t, err)
			readErr, ok := err.(*readError)
			require.True(t, ok)
			require.Equal(t, readErr.msg, tt.wantErr)
			require.EqualError(t, readErr.Unwrap(), "timeout")
		})
	}
}
