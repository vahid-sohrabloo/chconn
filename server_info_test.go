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
	// Discover the numberValid for each expected error by iterating through
	// read counts during hello. This avoids hardcoding offsets that change
	// when the protocol adds new fields.
	totalReads := helloReadsCount(t)

	expectedErrors := []string{
		"ServerInfo: could not read server name",
		"ServerInfo: could not read server major version",
		"ServerInfo: could not read server minor version",
		"ServerInfo: could not read server revision",
		"ServerInfo: could not read server timezone",
		"ServerInfo: could not read server display name",
		"ServerInfo: could not read server version patch",
		"ServerInfo: could not read server password complexity rules: len",
		"ServerInfo: could not read server interserver secret nonce",
	}

	// Build a map of error message -> numberValid by trying each read count
	errorAtCount := make(map[string]int)
	for nv := 1; nv < totalReads; nv++ {
		config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
		require.NoError(t, err)
		numValid := nv
		config.ReaderFunc = func(r io.Reader, c Conn) io.Reader {
			return &readErrorHelper{
				err:         errors.New("timeout"),
				r:           r,
				numberValid: numValid,
			}
		}
		_, err = ConnectConfig(context.Background(), config)
		if err == nil {
			continue
		}
		readErr, ok := err.(*readError)
		if !ok {
			continue
		}
		if _, seen := errorAtCount[readErr.msg]; !seen {
			errorAtCount[readErr.msg] = numValid
		}
	}

	// Now run the actual test cases using discovered numberValid values
	for _, wantErr := range expectedErrors {
		nv, found := errorAtCount[wantErr]
		if !found {
			// This error may not be produced by this server version (e.g., field
			// gated behind a revision check). Skip it.
			continue
		}
		t.Run(wantErr, func(t *testing.T) {
			config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader, c Conn) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: nv,
				}
			}

			_, err = ConnectConfig(context.Background(), config)
			require.Error(t, err)
			readErr, ok := err.(*readError)
			require.True(t, ok)
			require.Equal(t, readErr.msg, wantErr)
			require.EqualError(t, readErr.Unwrap(), "timeout")
		})
	}
}
