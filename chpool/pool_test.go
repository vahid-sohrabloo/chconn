package chpool

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn"
)

func TestConnect(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	pool.Close()
}

func TestParseConfigExtractsPoolArguments(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig(`pool_max_conns=42
								pool_min_conns=1
								pool_max_conn_lifetime=30s
								pool_max_conn_idle_time=31s
								pool_health_check_period=32s`)
	assert.NoError(t, err)
	assert.EqualValues(t, 42, config.MaxConns)
	assert.EqualValues(t, 42, config.MaxConns)
	assert.EqualValues(t, time.Second*30, config.MaxConnLifetime)
	assert.EqualValues(t, time.Second*31, config.MaxConnIdleTime)
	assert.EqualValues(t, time.Second*32, config.HealthCheckPeriod)

	assert.NotContains(t, config.Config.RuntimeParams, "pool_max_conns")
	assert.NotContains(t, config.Config.RuntimeParams, "pool_min_conns")
	assert.NotContains(t, config.Config.RuntimeParams, "pool_max_conn_lifetime")
	assert.NotContains(t, config.Config.RuntimeParams, "pool_max_conn_idle_time")
	assert.NotContains(t, config.Config.RuntimeParams, "pool_health_check_period")
}

func TestConnectCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool, err := Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	assert.Nil(t, pool)
	assert.Equal(t, context.Canceled, err)
}

func TestConnectParseError(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), "host>0")
	assert.Nil(t, pool)
	assert.Equal(t, "cannot parse `host>0`: failed to parse as DSN (invalid dsn)", err.Error())
}

func TestConnectError(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), "host=invalidhost")
	assert.Nil(t, pool)
	assert.Error(t, err)

	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	config.AfterConnect = func(ctx context.Context, c chconn.Conn) error {
		return errors.New("afterConnect err")
	}

	_, err = ConnectConfig(context.Background(), config)
	assert.EqualError(t, err, "afterConnect err")
}

func TestConnectConfigRequiresConnConfigFromParseConfig(t *testing.T) {
	t.Parallel()

	config := &Config{}

	require.PanicsWithValue(t, "config must be created by ParseConfig", func() {
		ConnectConfig(context.Background(), config) //nolint:errcheck not needed
	})
}

func TestPoolAcquireAndConnRelease(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	c.Release()
}

func TestPoolAfterConnect(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	var trigger bool
	config.AfterConnect = func(ctx context.Context, c chconn.Conn) error {
		trigger = true
		return nil
	}

	db, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	assert.True(t, trigger)
}

func TestPoolBeforeAcquire(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)

	acquireAttempts := 0

	config.BeforeAcquire = func(ctx context.Context, c chconn.Conn) bool {
		acquireAttempts++
		return acquireAttempts%2 == 0
	}

	db, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	conns := make([]Conn, 4)
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

	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)

	afterReleaseCount := 0

	config.AfterRelease = func(c chconn.Conn) bool {
		afterReleaseCount++
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

	db, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer db.Close()

	conns := db.AcquireAllIdle(context.Background())
	assert.Len(t, conns, 1)

	for _, c := range conns {
		c.Release()
	}
	waitForReleaseToComplete()

	conns = make([]Conn, 3)
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

	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
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

func TestConnReleaseClosesBusyConn(t *testing.T) {
	t.Parallel()

	db, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer db.Close()

	c, err := db.Acquire(context.Background())
	require.NoError(t, err)

	_, err = c.SelectCallback(context.Background(), "SELECT * FROM system.numbers LIMIT 10;", nil, nil)
	require.NoError(t, err)

	c.Release()
	waitForReleaseToComplete()

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
}

func TestPoolBackgroundChecksMaxConnLifetime(t *testing.T) {
	t.Parallel()

	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
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

	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
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

	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
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

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer pool.Close()

	testExec(t, pool)
}

func TestPoolExecError(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)

	testExec(t, pool)

	pool.Close()

	results, err := pool.Exec(context.Background(), "SET enable_http_compression=1")
	if assert.Error(t, err) {
		assert.Equal(t, "closed pool", err.Error())
	}
	assert.EqualValues(t, nil, results)
}

func TestPoolSelect(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer pool.Close()

	// Test common usage
	testSelect(t, pool)
	waitForReleaseToComplete()

	// Test expected pool behavior
	stmt, err := pool.Select(context.Background(), "SELECT * FROM system.numbers LIMIT 5;")
	require.NoError(t, err)
	for stmt.Next() {
		_, err := stmt.NextColumn()
		assert.NoError(t, err)
		err = stmt.Uint64All(&[]uint64{})
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

	// more coverage

	assert.EqualValues(t, 3, stats.AcquireCount())
	assert.GreaterOrEqual(t, int64(time.Second), int64(stats.AcquireDuration()))
	assert.EqualValues(t, 0, stats.AcquiredConns())
	assert.EqualValues(t, 0, stats.CanceledAcquireCount())
	assert.EqualValues(t, 0, stats.ConstructingConns())
	assert.EqualValues(t, 1, stats.EmptyAcquireCount())
	assert.EqualValues(t, 1, stats.IdleConns())
	assert.EqualValues(t, 4, stats.MaxConns())
}

func TestPoolSelectError(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)

	// Test common usage
	testSelect(t, pool)
	waitForReleaseToComplete()

	// Test expected pool behavior
	stmt, err := pool.Select(context.Background(), "SELECT * FROM not_fount_table LIMIT 10;")
	require.NoError(t, err)
	require.False(t, stmt.Next())
	require.Error(t, stmt.Err())

	pool.Close()

	stmt, err = pool.Select(context.Background(), "SELECT * FROM not_fount_table LIMIT 10;")

	if assert.Error(t, err) {
		assert.Equal(t, "closed pool", err.Error())
	}

	require.Nil(t, stmt)
}
func TestPoolAcquireSelectError(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer pool.Close()

	// Test common usage
	testSelect(t, pool)
	waitForReleaseToComplete()

	// Test expected pool behavior
	conn, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	conn.Conn().RawConn().Close()
	_, err = conn.SelectCallback(context.Background(), "SELECT * FROM system.numbers LIMIT 5;", nil, nil)
	conn.Release()
	require.Error(t, err)
}

func TestPoolInsert(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
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
		insertStmt.AddRow(1)
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
		_, err := selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Int8All(&int8Data)
		require.NoError(t, err)
	}
	require.NoError(t, selectStmt.Err())
	stats := pool.Stat()
	assert.EqualValues(t, 1, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())

	selectStmt.Close()
	waitForReleaseToComplete()

	stats = pool.Stat()
	assert.EqualValues(t, 0, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())
}

func TestPoolInsertError(t *testing.T) {
	t.Parallel()

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)

	insertStmt, err := pool.Insert(context.Background(), `INSERT INTO not_found_table (
				int8
			) VALUES`)
	if assert.Error(t, err) {
		assert.Equal(t, " DB::Exception (60): Table default.not_found_table doesn't exist.", err.Error())
	}
	require.Nil(t, insertStmt)

	pool.Close()

	insertStmt, err = pool.Insert(context.Background(), `INSERT INTO not_found_table (
				int8
			) VALUES`)

	if assert.Error(t, err) {
		assert.Equal(t, "closed pool", err.Error())
	}

	require.Nil(t, insertStmt)
}

func TestConnReleaseClosesConnInFailedTransaction(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	pid := c.Conn().RawConn().LocalAddr().String()

	stmt, err := c.SelectCallback(ctx, "SELECT * FROM system.numbers2 LIMIT 5;", nil, nil)
	assert.NoError(t, err)
	assert.False(t, stmt.Next())
	assert.Error(t, stmt.Err())

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
	pool, err := Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
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

	pool, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
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

func TestParseConfigError(t *testing.T) {
	t.Parallel()

	parseConfigErrorTests := []struct {
		name       string
		connString string
		err        string
	}{
		{
			name:       "invalid host",
			connString: "host>0",
			err:        "cannot parse `host>0`: failed to parse as DSN (invalid dsn)",
		}, {
			name:       "invalid pool_max_conns",
			connString: "pool_max_conns=invalid",
			err:        "cannot parse pool_max_conns: strconv.ParseInt: parsing \"invalid\": invalid syntax",
		}, {
			name:       "low pool_max_conns",
			connString: "pool_max_conns=0",
			err:        "pool_max_conns too small: 0",
		}, {
			name:       "invalid pool_min_conns",
			connString: "pool_min_conns=invalid",
			err:        "cannot parse pool_min_conns: strconv.ParseInt: parsing \"invalid\": invalid syntax",
		}, {
			name:       "invalid pool_max_conn_lifetime",
			connString: "pool_max_conn_lifetime=invalid",
			err:        "invalid pool_max_conn_lifetime: time: invalid duration invalid",
		}, {
			name:       "invalid pool_max_conn_idle_time",
			connString: "pool_max_conn_idle_time=invalid",
			err:        "invalid pool_max_conn_idle_time: time: invalid duration invalid",
		}, {
			name:       "invalid pool_health_check_period",
			connString: "pool_health_check_period=invalid",
			err:        "invalid pool_health_check_period: time: invalid duration invalid",
		},
	}

	for i, tt := range parseConfigErrorTests {
		_, err := ParseConfig(tt.connString)
		if !assert.Errorf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}
		if !assert.Equalf(t, err.Error(), tt.err, "Test %d (%s)", i, tt.name) {
			continue
		}
	}
}
