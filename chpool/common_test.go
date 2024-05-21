package chpool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// Conn.Release is an asynchronous process that returns immediately. There is no signal when the actual work is
// completed. To test something that relies on the actual work for Conn.Release being completed we must simply wait.
// This function wraps the sleep so there is more meaning for the callers.
func waitForReleaseToComplete() {
	time.Sleep(500 * time.Millisecond)
}

type execer interface {
	Exec(ctx context.Context, sql string) error
}

func testExec(t *testing.T, ctx context.Context, db execer) {
	err := db.Exec(ctx, "SET enable_http_compression=1")
	require.NoError(t, err)
}

type selecter interface {
	Select(ctx context.Context, query string, columns ...column.ColumnBasic) (chconn.SelectStmt, error)
}

func testSelect(t *testing.T, db selecter) {
	var (
		num []uint64
	)
	col := column.New[uint64]()
	stmt, err := db.Select(context.Background(), "SELECT * FROM system.numbers LIMIT 5;", col)
	require.NoError(t, err)
	for stmt.Next() {
		assert.NoError(t, err)
		num = col.Read(num)
		assert.NoError(t, err)
	}
	assert.NoError(t, stmt.Err())
	assert.Equal(t, 5, len(num))
	stmt.Close()
	assert.ElementsMatch(t, []uint64{0, 1, 2, 3, 4}, num)
}

func assertConfigsEqual(t *testing.T, expected, actual *Config, testName string) {
	if !assert.NotNil(t, expected) {
		return
	}
	if !assert.NotNil(t, actual) {
		return
	}

	assert.Equalf(t, expected.ConnString(), actual.ConnString(), "%s - ConnString", testName)

	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.AfterConnect == nil, actual.AfterConnect == nil, "%s - AfterConnect", testName)
	assert.Equalf(t, expected.BeforeAcquire == nil, actual.BeforeAcquire == nil, "%s - BeforeAcquire", testName)
	assert.Equalf(t, expected.AfterRelease == nil, actual.AfterRelease == nil, "%s - AfterRelease", testName)

	assert.Equalf(t, expected.MaxConnLifetime, actual.MaxConnLifetime, "%s - MaxConnLifetime", testName)
	assert.Equalf(t, expected.MaxConnIdleTime, actual.MaxConnIdleTime, "%s - MaxConnIdleTime", testName)
	assert.Equalf(t, expected.MaxConns, actual.MaxConns, "%s - MaxConns", testName)
	assert.Equalf(t, expected.MinConns, actual.MinConns, "%s - MinConns", testName)
	assert.Equalf(t, expected.HealthCheckPeriod, actual.HealthCheckPeriod, "%s - HealthCheckPeriod", testName)

	assertConnConfigsEqual(t, expected.ConnConfig, actual.ConnConfig, testName)
}

func assertConnConfigsEqual(t *testing.T, expected, actual *chconn.Config, testName string) {
	if !assert.NotNil(t, expected) {
		return
	}
	if !assert.NotNil(t, actual) {
		return
	}

	assert.Equalf(t, expected.ConnString(), actual.ConnString(), "%s - ConnString", testName)

	assert.Equalf(t, expected.Host, actual.Host, "%s - Host", testName)
	assert.Equalf(t, expected.Database, actual.Database, "%s - Database", testName)
	assert.Equalf(t, expected.Port, actual.Port, "%s - Port", testName)
	assert.Equalf(t, expected.User, actual.User, "%s - User", testName)
	assert.Equalf(t, expected.Password, actual.Password, "%s - Password", testName)
	assert.Equalf(t, expected.ConnectTimeout, actual.ConnectTimeout, "%s - ConnectTimeout", testName)
	assert.Equalf(t, expected.RuntimeParams, actual.RuntimeParams, "%s - RuntimeParams", testName)

	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.ValidateConnect == nil, actual.ValidateConnect == nil, "%s - ValidateConnect", testName)
	assert.Equalf(t, expected.AfterConnect == nil, actual.AfterConnect == nil, "%s - AfterConnect", testName)

	if assert.Equalf(t, expected.TLSConfig == nil, actual.TLSConfig == nil, "%s - TLSConfig", testName) {
		if expected.TLSConfig != nil {
			assert.Equalf(t,
				expected.TLSConfig.InsecureSkipVerify,
				actual.TLSConfig.InsecureSkipVerify,
				"%s - TLSConfig InsecureSkipVerify", testName)
			assert.Equalf(t,
				expected.TLSConfig.ServerName,
				actual.TLSConfig.ServerName,
				"%s - TLSConfig ServerName", testName)
		}
	}

	if assert.Equalf(t, len(expected.Fallbacks), len(actual.Fallbacks), "%s - Fallbacks", testName) {
		for i := range expected.Fallbacks {
			assert.Equalf(t,
				expected.Fallbacks[i].Host,
				actual.Fallbacks[i].Host,
				"%s - Fallback %d - Host", testName, i)
			assert.Equalf(t,
				expected.Fallbacks[i].Port,
				actual.Fallbacks[i].Port,
				"%s - Fallback %d - Port", testName, i)

			if assert.Equalf(t,
				expected.Fallbacks[i].TLSConfig == nil,
				actual.Fallbacks[i].TLSConfig == nil,
				"%s - Fallback %d - TLSConfig", testName, i) {
				if expected.Fallbacks[i].TLSConfig != nil {
					assert.Equalf(t,
						expected.Fallbacks[i].TLSConfig.InsecureSkipVerify,
						actual.Fallbacks[i].TLSConfig.InsecureSkipVerify,
						"%s - Fallback %d - TLSConfig InsecureSkipVerify", testName)
					assert.Equalf(t,
						expected.Fallbacks[i].TLSConfig.ServerName,
						actual.Fallbacks[i].TLSConfig.ServerName,
						"%s - Fallback %d - TLSConfig ServerName", testName)
				}
			}
		}
	}
}
