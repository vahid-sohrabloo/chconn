package chpool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn"
)

// Conn.Release is an asynchronous process that returns immediately. There is no signal when the actual work is
// completed. To test something that relies on the actual work for Conn.Release being completed we must simply wait.
// This function wraps the sleep so there is more meaning for the callers.
func waitForReleaseToComplete() {
	time.Sleep(5 * time.Millisecond)
}

type execer interface {
	Exec(ctx context.Context, sql string) (interface{}, error)
}

func testExec(t *testing.T, db execer) {
	results, err := db.Exec(context.Background(), "SET enable_http_compression=1")
	require.NoError(t, err)
	assert.EqualValues(t, nil, results)
}

type selecter interface {
	Select(ctx context.Context, sql string) (chconn.SelectStmt, error)
}

func testSelect(t *testing.T, db selecter) {
	var (
		nums []uint64
	)

	stmt, err := db.Select(context.Background(), "SELECT * FROM system.numbers LIMIT 5;")
	require.NoError(t, err)

	for stmt.Next() {
		_, err := stmt.NextColumn()
		assert.NoError(t, err)
		err = stmt.Uint64All(&nums)
		assert.NoError(t, err)
	}

	assert.Equal(t, 5, len(nums))
	stmt.Close()
	assert.ElementsMatch(t, []uint64{0, 1, 2, 3, 4}, nums)
}
