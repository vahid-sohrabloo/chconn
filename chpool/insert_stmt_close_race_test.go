package chpool

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// TestInsertStmtCloseAfterFlushRace runs the documented InsertStream sequence --
// acquire a stream, defer Close, write, Flush -- from several goroutines over a
// small pool.
//
// insertStmt.Flush closes the statement and releases the connection, so the
// caller's deferred Close is a second Close on a statement that is already
// finished -- and on a connection the pool may already have handed to another
// goroutine. That second Close must not touch the connection.
func TestInsertStmtCloseAfterFlushRace(t *testing.T) {
	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	config.MaxConns = 2

	pool, err := NewWithConfig(config)
	require.NoError(t, err)
	// Registered before the DROP below so LIFO drops the table, then closes the pool.
	t.Cleanup(pool.Close)

	ctx := context.Background()
	// PID-suffixed so concurrent runs of this package cannot drop each other's table.
	table := "test_insert_stmt_close_race_" + strconv.Itoa(os.Getpid())
	require.NoError(t, pool.Exec(ctx, "DROP TABLE IF EXISTS "+table))
	require.NoError(t, pool.Exec(ctx, "CREATE TABLE "+table+" (n UInt64) ENGINE = Memory"))
	t.Cleanup(func() {
		require.NoError(t, pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table))
	})

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 50 {
				func() {
					stmt, err := pool.InsertStream(ctx, "INSERT INTO "+table+" (n) VALUES")
					if err != nil {
						t.Error(err)
						return
					}
					defer stmt.Close() // second Close: Flush already closed and released

					col := column.New[uint64]()
					col.Append(uint64(g*1000 + i))
					if err := stmt.Write(ctx, col); err != nil {
						t.Error(err)
						return
					}
					if err := stmt.Flush(ctx); err != nil {
						t.Error(err)
					}
				}()
			}
		})
	}
	wg.Wait()
}
