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

func TestChErrorReadError(t *testing.T) {
	startValidReader := 16

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
			config.ReaderFunc = func(r io.Reader, c Conn) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			err = c.Exec(context.Background(), "SELECT * FROM invalid_table LIMIT 5;")
			require.Error(t, err)
			readErr, ok := err.(*readError)
			require.True(t, ok)
			require.Equal(t, readErr.msg, tt.wantErr)
			require.EqualError(t, readErr.Unwrap(), "timeout")
			assert.True(t, c.IsClosed())
		})
	}
}

func NewParseConfigError(conn, msg string, err error) error {
	return &parseConfigError{
		connString: conn,
		msg:        msg,
		err:        err,
	}
}

func TestConfigError(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		expectedMsg string
	}{
		{
			name:        "url with password",
			err:         NewParseConfigError("clickhouse://foo:password@host", "msg", nil),
			expectedMsg: "cannot parse `clickhouse://foo:xxxxx@host`: msg",
		},
		{
			name:        "dsn with password unquoted",
			err:         NewParseConfigError("host=host password=password user=user", "msg", nil),
			expectedMsg: "cannot parse `host=host password=xxxxx user=user`: msg",
		},
		{
			name:        "dsn with password quoted",
			err:         NewParseConfigError("host=host password='pass word' user=user", "msg", nil),
			expectedMsg: "cannot parse `host=host password=xxxxx user=user`: msg",
		},
		{
			name:        "weird url",
			err:         NewParseConfigError("clickhouse://foo::pasword@host:1:", "msg", nil),
			expectedMsg: "cannot parse `clickhouse://foo:xxxxx@host:1:`: msg",
		},
		{
			name:        "weird url with slash in password",
			err:         NewParseConfigError("clickhouse://user:pass/word@host:5432/db_name", "msg", nil),
			expectedMsg: "cannot parse `clickhouse://user:xxxxxx@host:5432/db_name`: msg",
		},
		{
			name:        "url without password",
			err:         NewParseConfigError("clickhouse://other@host/db", "msg", nil),
			expectedMsg: "cannot parse `clickhouse://other@host/db`: msg",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.EqualError(t, tt.err, tt.expectedMsg)
		})
	}
}
