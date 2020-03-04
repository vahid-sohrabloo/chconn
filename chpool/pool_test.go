package chpool

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn"
)

func TestConnect(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)
	pool.Close()
}

func TestParseConfigExtractsPoolArguments(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig("pool_max_conns=42 pool_min_conns=1")
	assert.NoError(t, err)
	assert.EqualValues(t, 42, config.MaxConns)
	assert.EqualValues(t, 1, config.MinConns)
	//todo
	assert.NotContains(t, config.Config.RuntimeParams, "pool_max_conns")
	assert.NotContains(t, config.Config.RuntimeParams, "pool_min_conns")
}

func TestConnectCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool, err := Connect(ctx, os.Getenv("CHX_TEST_DATABASE"))
	assert.Nil(t, pool)
	assert.Equal(t, context.Canceled, err)
}

func TestConnectConfigRequiresConnConfigFromParseConfig(t *testing.T) {
	t.Parallel()

	config := &Config{}

	require.PanicsWithValue(t, "config must be created by ParseConfig", func() { ConnectConfig(context.Background(), config) })
}

func TestPoolAcquireAndConnRelease(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	c.Release()
}

//todo
// func TestPoolAfterConnect(t *testing.T) {
// 	t.Parallel()

// 	config, err := ParseConfig(os.Getenv("CHX_TEST_DATABASE"))
// 	require.NoError(t, err)

// 	config.AfterConnect = func(ctx context.Context, c *chconn.Conn) error {
// 		// _, err := c.Prepare(ctx, "ps1", "select 1")
// 		return err
// 	}

// 	db, err := ConnectConfig(context.Background(), config)
// 	require.NoError(t, err)
// 	defer db.Close()

// 	var n int32
// 	err = db.QueryRow(context.Background(), "ps1").Scan(&n)
// 	require.NoError(t, err)
// 	assert.EqualValues(t, 1, n)
// }

func TestPoolBeforeAcquire(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig(os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)

	acquireAttempts := 0

	config.BeforeAcquire = func(ctx context.Context, c *chconn.Conn) bool {
		acquireAttempts += 1
		return acquireAttempts%2 == 0
	}

	db, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	conns := make([]*Conn, 4)
	for i := range conns {
		conns[i], err = db.Acquire(context.Background())
		assert.NoError(t, err)
	}

	for _, c := range conns {
		c.Release()
	}
	waitForReleaseToComplete()

	assert.EqualValues(t, 8, acquireAttempts)

	conns = db.AcquireAllIdle(context.Background())
	assert.Len(t, conns, 2)

	for _, c := range conns {
		c.Release()
	}
	waitForReleaseToComplete()

	assert.EqualValues(t, 12, acquireAttempts)
}

func TestPoolAfterRelease(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig(os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)

	afterReleaseCount := 0

	config.AfterRelease = func(c *chconn.Conn) bool {
		afterReleaseCount += 1
		return afterReleaseCount%2 == 1
	}

	db, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	conns := map[string]struct{}{}

	for i := 0; i < 10; i++ {

		conn, err := db.Acquire(context.Background())
		assert.NoError(t, err)
		conns[conn.Conn().RawConn().LocalAddr().String()] = struct{}{}
		conn.Release()
		waitForReleaseToComplete()
	}

	assert.EqualValues(t, 5, len(conns))
}

func TestPoolAcquireAllIdle(t *testing.T) {
	t.Parallel()

	db, err := Connect(context.Background(), os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)
	defer db.Close()

	conns := db.AcquireAllIdle(context.Background())
	assert.Len(t, conns, 1)

	for _, c := range conns {
		c.Release()
	}
	waitForReleaseToComplete()

	conns = make([]*Conn, 3)
	for i := range conns {
		conns[i], err = db.Acquire(context.Background())
		assert.NoError(t, err)
	}

	for _, c := range conns {
		if c != nil {
			c.Release()
		}
	}
	waitForReleaseToComplete()
	conns = db.AcquireAllIdle(context.Background())
	assert.Len(t, conns, 3)

	for _, c := range conns {
		c.Release()
	}
}

func TestConnReleaseChecksMaxConnLifetime(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig(os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)

	config.MaxConnLifetime = 250 * time.Millisecond

	db, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	c, err := db.Acquire(context.Background())
	require.NoError(t, err)

	time.Sleep(config.MaxConnLifetime)

	c.Release()
	waitForReleaseToComplete()

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
}

// func TestConnReleaseClosesBusyConn(t *testing.T) {
// 	t.Parallel()

// 	db, err := Connect(context.Background(), os.Getenv("CHX_TEST_DATABASE"))
// 	require.NoError(t, err)
// 	defer db.Close()

// 	c, err := db.Acquire(context.Background())
// 	require.NoError(t, err)

// 	_, err = c.Query(context.Background(), "select generate_series(1,10)")
// 	require.NoError(t, err)

// 	c.Release()
// 	waitForReleaseToComplete()

// 	stats := db.Stat()
// 	assert.EqualValues(t, 0, stats.TotalConns())
// }

func TestPoolBackgroundChecksMaxConnLifetime(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig(os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)

	config.MaxConnLifetime = 100 * time.Millisecond
	config.HealthCheckPeriod = 100 * time.Millisecond

	db, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	c, err := db.Acquire(context.Background())
	require.NoError(t, err)
	c.Release()
	time.Sleep(config.MaxConnLifetime + 50*time.Millisecond)

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
}

func TestPoolBackgroundChecksMaxConnIdleTime(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig(os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)

	config.MaxConnLifetime = 1 * time.Minute
	config.MaxConnIdleTime = 100 * time.Millisecond
	config.HealthCheckPeriod = 150 * time.Millisecond

	db, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()
	c, err := db.Acquire(context.Background())
	require.NoError(t, err)
	c.Release()
	time.Sleep(config.HealthCheckPeriod + 50*time.Millisecond)

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
}

func TestPoolBackgroundChecksMinConns(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig(os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)

	config.HealthCheckPeriod = 100 * time.Millisecond
	config.MinConns = 2

	db, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	time.Sleep(config.HealthCheckPeriod + 100*time.Millisecond)

	stats := db.Stat()
	assert.EqualValues(t, 2, stats.TotalConns())
}

func TestPoolExec(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	testExec(t, pool)
}

func TestPoolSelect(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_DATABASE"))
	// pool, err := Connect(context.Background(), "host=127.0.0.1 password=salam")
	require.NoError(t, err)
	defer pool.Close()

	// Test common usage
	testSelect(t, pool)
	waitForReleaseToComplete()

	// Test expected pool behavior
	stmt, err := pool.Select(context.Background(), "SELECT * FROM system.numbers LIMIT 5;")
	require.NoError(t, err)
	for stmt.Next() {
		stmt.NextColumn()
		err := stmt.Uint64(&[]uint64{})
		assert.NoError(t, err)
	}

	stats := pool.Stat()
	assert.EqualValues(t, 1, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())

	stmt.Close()
	waitForReleaseToComplete()

	stats = pool.Stat()
	assert.EqualValues(t, 0, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())

}

func TestPoolInsert(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_DATABASE"))
	// pool, err := Connect(context.Background(), "host=127.0.0.1 password=salam")
	require.NoError(t, err)
	defer pool.Close()

	res, err := pool.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_pool`)
	require.NoError(t, err)
	require.Nil(t, res)
	res, err = pool.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_pool (
				int8  Int8
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	insertStmt, err := pool.Insert(context.Background(), `INSERT INTO clickhouse_test_insert_pool (
				int8
			) VALUES`)
	require.NoError(t, err)
	require.Nil(t, res)
	for i := 1; i <= 10; i++ {
		insertStmt.Block.NumRows++
		insertStmt.Int8(0, int8(-1*i))
	}

	err = insertStmt.Commit(context.Background())
	require.NoError(t, err)

	selectStmt, err := pool.Select(context.Background(), `SELECT 
				int8
	 FROM clickhouse_test_insert_pool`)
	require.NoError(t, err)
	var int8Data []int8
	for selectStmt.Next() {
		selectStmt.NextColumn()
		err := selectStmt.Int8(&int8Data)
		require.NoError(t, err)
	}
	require.NoError(t, selectStmt.LastErr)
	stats := pool.Stat()
	assert.EqualValues(t, 1, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())

	selectStmt.Close()
	waitForReleaseToComplete()

	stats = pool.Stat()
	assert.EqualValues(t, 0, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())

}

func TestConnReleaseClosesConnInFailedTransaction(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := Connect(ctx, os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	pid := c.Conn().RawConn().LocalAddr().String()

	stmt, err := c.Select(ctx, "SELECT * FROM system.numbers2 LIMIT 5;")
	assert.NoError(t, err)
	assert.False(t, stmt.Next())
	assert.Error(t, stmt.LastErr)

	c.Release()
	waitForReleaseToComplete()

	c, err = pool.Acquire(ctx)
	require.NoError(t, err)

	assert.NotEqual(t, pid, c.Conn().RawConn().LocalAddr().String())
	c.Release()
}

func TestConnReleaseDestroysClosedConn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := Connect(ctx, os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	c.Conn().Close(ctx)
	err = c.Conn().Close(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, pool.Stat().TotalConns())
	c.Release()
	waitForReleaseToComplete()
	assert.EqualValues(t, 0, pool.Stat().TotalConns())
}

func TestConnPoolQueryConcurrentLoad(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	n := 100
	done := make(chan bool)

	for i := 0; i < n; i++ {
		go func() {
			defer func() { done <- true }()
			testSelect(t, pool)
		}()
	}

	for i := 0; i < n; i++ {
		<-done
	}
}
