package chpool

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPoolRowsCloseAfterDrainRace runs the ordinary query/drain/Close sequence
// from several goroutines over a small pool.
//
// poolRows.Next closes the rows and releases the connection as soon as it
// returns false, so the caller's deferred Close is a second Close on a
// statement that is already finished -- and on a connection the pool may
// already have handed to another goroutine. That second Close must not touch
// the connection.
func TestPoolRowsCloseAfterDrainRace(t *testing.T) {
	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	config.MaxConns = 2

	pool, err := NewWithConfig(config)
	require.NoError(t, err)
	defer pool.Close()

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 300 {
				func() {
					rows, err := pool.Query(context.Background(),
						"SELECT number FROM system.numbers LIMIT 5")
					if err != nil {
						t.Error(err)
						return
					}
					defer rows.Close() // second Close: Next already closed and released

					for rows.Next() {
						var n uint64
						if err := rows.Scan(&n); err != nil {
							t.Error(err)
							return
						}
					}
					if err := rows.Err(); err != nil {
						t.Error(err)
					}
				}()
			}
		})
	}
	wg.Wait()
}
