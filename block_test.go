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
	// First, determine how many reads are needed for a successful hello+select
	// by connecting normally and counting. This avoids hardcoding a value that
	// changes with protocol version.
	helloReads := helloReadsCount(t)

	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)

	var totalReads int
	config.ReaderFunc = func(r io.Reader, _ Conn) io.Reader {
		return &readCounterHelper{r: r, count: &totalReads}
	}
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	stmt, err := c.Select(context.Background(), "SELECT * FROM system.numbers LIMIT 5;")
	require.NoError(t, err)
	require.NotNil(t, stmt)
	stmt.Close()
	c.Close()

	// Test that errors at various points during block reading are properly reported.
	for n := helloReads; n < totalReads; n++ {
		t.Run("", func(t *testing.T) {
			cfg, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			cfg.ReaderFunc = func(r io.Reader, _ Conn) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: n,
				}
			}

			c, err := ConnectConfig(context.Background(), cfg)
			if err != nil {
				return
			}
			_, err = c.Select(context.Background(), "SELECT * FROM system.numbers LIMIT 5;")
			require.Error(t, err)
			require.Contains(t, err.Error(), "timeout")
			assert.True(t, c.IsClosed())
		})
	}
}
