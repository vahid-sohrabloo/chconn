package chpool

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	puddle "github.com/jackc/puddle/v2"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

var defaultMaxConns = int32(4)
var defaultMinConns = int32(0)
var defaultMaxConnLifetime = time.Hour
var defaultMaxConnIdleTime = time.Minute * 30
var defaultHealthCheckPeriod = time.Minute

type connResource struct {
	conn       chconn.Conn
	conns      []conn
	poolRows   []poolRow
	poolRowss  []poolRows
	maxAgeTime time.Time
}

func (cr *connResource) getConn(p *pool, res *puddle.Resource[*connResource]) Conn {
	if len(cr.conns) == 0 {
		cr.conns = make([]conn, 128)
	}

	c := &cr.conns[len(cr.conns)-1]
	cr.conns = cr.conns[0 : len(cr.conns)-1]

	c.res = res
	c.p = p

	return c
}

func (cr *connResource) getPoolRow(c Conn, r chconn.Row) *poolRow {
	if len(cr.poolRows) == 0 {
		cr.poolRows = make([]poolRow, 128)
	}

	pr := &cr.poolRows[len(cr.poolRows)-1]
	cr.poolRows = cr.poolRows[0 : len(cr.poolRows)-1]

	pr.c = c
	pr.r = r

	return pr
}

func (cr *connResource) getPoolRows(c Conn, r chconn.Rows) *poolRows {
	if len(cr.poolRowss) == 0 {
		cr.poolRowss = make([]poolRows, 128)
	}

	pr := &cr.poolRowss[len(cr.poolRowss)-1]
	cr.poolRowss = cr.poolRowss[0 : len(cr.poolRowss)-1]

	pr.c = c
	pr.r = r

	return pr
}

// Pool is a connection pool for chconn
type Pool interface {
	// Close closes all connections in the pool and rejects future Acquire calls. Blocks until all connections are returned
	// to pool and closed.
	Close()
	// Acquire returns a connection (Conn) from the Pool
	Acquire(ctx context.Context) (Conn, error)
	// AcquireFunc acquires a Conn and calls f with that Conn. ctx will only affect the Acquire. It has no effect on the
	// call of f. The return value is either an error acquiring the Conn or the return value of f. The Conn is
	// automatically released after the call of f.
	AcquireFunc(ctx context.Context, f func(Conn) error) error
	// AcquireAllIdle atomically acquires all currently idle connections. Its intended use is for health check and
	// keep-alive functionality. It does not update pool statistics.
	AcquireAllIdle(ctx context.Context) []Conn
	// Exec executes a query without returning any rows.
	// NOTE: don't use it for insert and select query
	Exec(ctx context.Context, query string) error
	// ExecWithOption executes a query without returning any rows with Query options.
	// NOTE: don't use it for insert and select query
	ExecWithOption(
		ctx context.Context,
		query string,
		queryOptions *chconn.QueryOptions,
	) error
	// Insert executes a insert query and commit all columns data.
	//
	// If the query is successful, the columns buffer will be reset.
	//
	// NOTE: only use for insert query
	Insert(ctx context.Context, query string, columns ...column.ColumnBasic) error
	// InsertWithOption executes a insert query with a query options and commit all columns data.
	//
	// If the query is successful, the columns buffer will be reset.
	//
	// NOTE: only use for insert query
	InsertWithOption(ctx context.Context, query string, queryOptions *chconn.QueryOptions, columns ...column.ColumnBasic) error
	// Insert executes a insert query and return a InsertStmt.
	//
	// NOTE: only use for insert query
	InsertStream(ctx context.Context, query string) (chconn.InsertStmt, error)
	// InsertWithOption executes a insert query with a query options and return a InsertStmt.
	//
	// If the query is successful, the columns buffer will be reset.
	//
	// NOTE: only use for insert query
	InsertStreamWithOption(
		ctx context.Context,
		query string,
		queryOptions *chconn.QueryOptions) (chconn.InsertStmt, error)
	// Select executes a query and return select stmt.
	//
	// NOTE: only use for select query
	Select(ctx context.Context, query string, columns ...column.ColumnBasic) (chconn.SelectStmt, error)
	// Select executes a query with a query options and return select stmt.
	//
	// NOTE: only use for select query
	SelectWithOption(
		ctx context.Context,
		query string,
		queryOptions *chconn.QueryOptions,
		columns ...column.ColumnBasic,
	) (chconn.SelectStmt, error)
	// Query acquires a connection and executes a (select) query that returns chconn.Rows.
	// See chconn.Rows documentation to close the returned Rows and return the acquired connection to the Pool.
	//
	// If there is an error, the returned chconn.Rows will be returned in an error state.
	// If preferred, ignore the error returned from Query and handle errors using the returned chconn.Rows.
	//
	Query(ctx context.Context, sql string, args ...chconn.Parameter) (chconn.Rows, error)
	// QueryRow acquires a connection and executes a query that is expected
	// to return at most one row (chconn.Row). Errors are deferred until chconn.Row's
	// Scan method is called. If the query selects no rows, chconn.Row's Scan will
	// return ErrNoRows. Otherwise, chconn.Row's Scan scans the first selected row
	// and discards the rest. The acquired connection is returned to the Pool when
	// chconn.Row's Scan method is called.
	QueryRow(ctx context.Context, sql string, args ...chconn.Parameter) chconn.Row
	// Query acquires a connection and executes a (select) query with a query options that returns chconn.Rows.
	// See chconn.Rows documentation to close the returned Rows and return the acquired connection to the Pool.
	//
	// If there is an error, the returned chconn.Rows will be returned in an error state.
	// If preferred, ignore the error returned from Query and handle errors using the returned chconn.Rows.
	//
	QueryWithOption(ctx context.Context, sql string, queryOptions *chconn.QueryOptions, args ...chconn.Parameter) (chconn.Rows, error)
	// QueryRow acquires a connection and executes a query with a query options that is expected
	// to return at most one row (chconn.Row). Errors are deferred until chconn.Row's
	// Scan method is called. If the query selects no rows, chconn.Row's Scan will
	// return ErrNoRows. Otherwise, chconn.Row's Scan scans the first selected row
	// and discards the rest. The acquired connection is returned to the Pool when
	// chconn.Row's Scan method is called.
	QueryRowWithOption(ctx context.Context, sql string, queryOptions *chconn.QueryOptions, args ...chconn.Parameter) chconn.Row
	// Ping acquires a connection from the Pool and send ping
	// If returns without error, the database Ping is considered successful, otherwise, the error is returned.
	Ping(ctx context.Context) error
	// Stat returns a chpool.Stat struct with a snapshot of Pool statistics.
	Stat() *Stat
	// Reset closes all connections, but leaves the pool open. It is intended for use when an error is detected that would
	// disrupt all connections (such as a network interruption or a server state change).
	//
	// It is safe to reset a pool while connections are checked out. Those connections will be closed when they are returned
	// to the pool.
	Reset()
	// Config returns a copy of config that was used to initialize this pool.
	Config() *Config
}
type pool struct {
	// 64 bit fields accessed with atomics must be at beginning of struct to guarantee alignment for certain 32-bit
	// architectures. See BUGS section of https://pkg.go.dev/sync/atomic and https://github.com/jackc/pgx/issues/1288.
	newConnsCount        int64
	lifetimeDestroyCount int64
	idleDestroyCount     int64

	p                     *puddle.Pool[*connResource]
	config                *Config
	beforeConnect         func(context.Context, *chconn.Config) error
	afterConnect          func(context.Context, chconn.Conn) error
	beforeAcquire         func(context.Context, chconn.Conn) bool
	afterRelease          func(chconn.Conn) bool
	beforeClose           func(chconn.Conn)
	minConns              int32
	maxConns              int32
	maxConnLifetime       time.Duration
	maxConnLifetimeJitter time.Duration
	maxConnIdleTime       time.Duration
	healthCheckPeriod     time.Duration

	healthCheckChan chan struct{}

	closeOnce sync.Once
	closeChan chan struct{}
}

// Config is the configuration struct for creating a pool. It must be created by [ParseConfig] and then it can be
// modified.
type Config struct {
	ConnConfig *chconn.Config

	// BeforeConnect is called before a new connection is made. It is passed a copy of the underlying chconn.Config and
	// will not impact any existing open connections.
	BeforeConnect func(context.Context, *chconn.Config) error

	// AfterConnect is called after a connection is established, but before it is added to the pool.
	AfterConnect func(context.Context, chconn.Conn) error

	// BeforeAcquire is called before a connection is acquired from the pool. It must return true to allow the
	// acquision or false to indicate that the connection should be destroyed and a different connection should be
	// acquired.
	BeforeAcquire func(context.Context, chconn.Conn) bool

	// AfterRelease is called after a connection is released, but before it is returned to the pool. It must return true to
	// return the connection to the pool or false to destroy the connection.
	AfterRelease func(chconn.Conn) bool

	// BeforeClose is called right before a connection is closed and removed from the pool.
	BeforeClose func(chconn.Conn)

	// MaxConnLifetime is the duration since creation after which a connection will be automatically closed.
	MaxConnLifetime time.Duration

	// MaxConnLifetimeJitter is the duration after MaxConnLifetime to randomly decide to close a connection.
	// This helps prevent all connections from being closed at the exact same time, starving the pool.
	MaxConnLifetimeJitter time.Duration

	// MaxConnIdleTime is the duration after which an idle connection will be automatically closed by the health check.
	MaxConnIdleTime time.Duration

	// MaxConns is the maximum size of the pool. The default is the greater of 4 or runtime.NumCPU().
	MaxConns int32

	// MinConns is the minimum size of the pool. After connection closes, the pool might dip below MinConns. A low
	// number of MinConns might mean the pool is empty after MaxConnLifetime until the health check has a chance
	// to create new connections.
	MinConns int32

	// HealthCheckPeriod is the duration between checks of the health of idle connections.
	HealthCheckPeriod time.Duration

	createdByParseConfig bool // Used to enforce created by ParseConfig rule.
}

// Copy returns a deep copy of the config that is safe to use and modify.
// The only exception is the tls.Config:
// according to the tls.Config docs it must not be modified after creation.
func (c *Config) Copy() *Config {
	newConfig := new(Config)
	*newConfig = *c
	newConfig.ConnConfig = c.ConnConfig.Copy()
	return newConfig
}

// ConnString returns the connection string as parsed by pgxpool.ParseConfig into pgxpool.Config.
func (c *Config) ConnString() string { return c.ConnConfig.ConnString() }

// New creates a new Pool. See [ParseConfig] for information on connString format.
func New(connString string) (Pool, error) {
	config, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return NewWithConfig(config)
}

// NewWithConfig creates a new Pool. config must have been created by ParseConfig.
func NewWithConfig(config *Config) (Pool, error) {
	// Default values are set in ParseConfig. Enforce initial creation by ParseConfig rather than setting defaults from
	// zero values.
	if !config.createdByParseConfig {
		panic("config must be created by ParseConfig")
	}

	p := &pool{
		config:                config,
		beforeConnect:         config.BeforeConnect,
		afterConnect:          config.AfterConnect,
		beforeAcquire:         config.BeforeAcquire,
		afterRelease:          config.AfterRelease,
		beforeClose:           config.BeforeClose,
		minConns:              config.MinConns,
		maxConns:              config.MaxConns,
		maxConnLifetime:       config.MaxConnLifetime,
		maxConnLifetimeJitter: config.MaxConnLifetimeJitter,
		maxConnIdleTime:       config.MaxConnIdleTime,
		healthCheckPeriod:     config.HealthCheckPeriod,
		healthCheckChan:       make(chan struct{}, 1),
		closeChan:             make(chan struct{}),
	}

	var err error
	p.p, err = puddle.NewPool(
		&puddle.Config[*connResource]{
			Constructor: func(ctx context.Context) (*connResource, error) {
				atomic.AddInt64(&p.newConnsCount, 1)
				connConfig := p.config.ConnConfig.Copy()

				// Connection will continue in background even if Acquire is canceled. Ensure that a connect won't hang forever.
				if connConfig.ConnectTimeout <= 0 {
					connConfig.ConnectTimeout = 2 * time.Minute
				}

				if p.beforeConnect != nil {
					if err := p.beforeConnect(ctx, connConfig); err != nil {
						return nil, err
					}
				}

				c, err := chconn.ConnectConfig(ctx, connConfig)
				if err != nil {
					return nil, err
				}

				if p.afterConnect != nil {
					err := p.afterConnect(ctx, c)
					if err != nil {
						c.Close()
						return nil, err
					}
				}
				//nolint:gosec // it's not a security issue
				jitterSecs := rand.Float64() * config.MaxConnLifetimeJitter.Seconds()
				maxAgeTime := time.Now().Add(config.MaxConnLifetime).Add(time.Duration(jitterSecs) * time.Second)

				cr := &connResource{
					conn:       c,
					conns:      make([]conn, 64),
					maxAgeTime: maxAgeTime,
				}

				return cr, nil
			},
			Destructor: func(value *connResource) {
				if p.beforeClose != nil {
					p.beforeClose(value.conn)
				}
				value.conn.Close()
			},
			MaxSize: config.MaxConns,
		},
	)
	if err != nil {
		return nil, err
	}

	go func() {
		//nolint:errcheck // todo find a way to handle this error
		p.createIdleResources(int(p.minConns))
		p.backgroundHealthCheck()
	}()

	return p, nil
}

// ParseConfig builds a Config from connString. It parses connString with the same behavior as [chconn.ParseConfig] with the
// addition of the following variables:
//
//   - pool_max_conns: integer greater than 0
//   - pool_min_conns: integer 0 or greater
//   - pool_max_conn_lifetime: duration string
//   - pool_max_conn_idle_time: duration string
//   - pool_health_check_period: duration string
//   - pool_max_conn_lifetime_jitter: duration string
//
// See Config for definitions of these arguments.
//
//	# Example DSN
//	user=vahid password=secret host=clickhouse.example.com port=9000 dbname=mydb sslmode=verify-ca pool_max_conns=10
//
//	# Example URL
//	clickhouse://vahid:secret@ch.example.com:9000/mydb?sslmode=verify-ca&pool_max_conns=10
func ParseConfig(connString string) (*Config, error) {
	chConfig, err := chconn.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	config := &Config{
		ConnConfig:           chConfig,
		createdByParseConfig: true,
	}

	if s, ok := config.ConnConfig.RuntimeParams["pool_max_conns"]; ok {
		delete(config.ConnConfig.RuntimeParams, "pool_max_conns")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot parse pool_max_conns: %w", err)
		}
		if n < 1 {
			//nolint:goerr113
			return nil, fmt.Errorf("pool_max_conns too small: %d", n)
		}
		config.MaxConns = int32(n)
	} else {
		config.MaxConns = defaultMaxConns
		if numCPU := int32(runtime.NumCPU()); numCPU > config.MaxConns {
			config.MaxConns = numCPU
		}
	}

	if s, ok := config.ConnConfig.RuntimeParams["pool_min_conns"]; ok {
		delete(config.ConnConfig.RuntimeParams, "pool_min_conns")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot parse pool_min_conns: %w", err)
		}
		config.MinConns = int32(n)
	} else {
		config.MinConns = defaultMinConns
	}

	if s, ok := config.ConnConfig.RuntimeParams["pool_max_conn_lifetime"]; ok {
		delete(config.ConnConfig.RuntimeParams, "pool_max_conn_lifetime")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid pool_max_conn_lifetime: %w", err)
		}
		config.MaxConnLifetime = d
	} else {
		config.MaxConnLifetime = defaultMaxConnLifetime
	}

	if s, ok := config.ConnConfig.RuntimeParams["pool_max_conn_idle_time"]; ok {
		delete(config.ConnConfig.RuntimeParams, "pool_max_conn_idle_time")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid pool_max_conn_idle_time: %w", err)
		}
		config.MaxConnIdleTime = d
	} else {
		config.MaxConnIdleTime = defaultMaxConnIdleTime
	}

	if s, ok := config.ConnConfig.RuntimeParams["pool_health_check_period"]; ok {
		delete(config.ConnConfig.RuntimeParams, "pool_health_check_period")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid pool_health_check_period: %w", err)
		}
		config.HealthCheckPeriod = d
	} else {
		config.HealthCheckPeriod = defaultHealthCheckPeriod
	}

	if s, ok := config.ConnConfig.RuntimeParams["pool_max_conn_lifetime_jitter"]; ok {
		delete(config.ConnConfig.RuntimeParams, "pool_max_conn_lifetime_jitter")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("invalid pool_max_conn_lifetime_jitter: %w", err)
		}
		config.MaxConnLifetimeJitter = d
	}

	return config, nil
}

// Close closes all connections in the pool and rejects future Acquire calls. Blocks until all connections are returned
// to pool and closed.
func (p *pool) Close() {
	p.closeOnce.Do(func() {
		close(p.closeChan)
		p.p.Close()
	})
}

func (p *pool) isExpired(res *puddle.Resource[*connResource]) bool {
	return time.Now().After(res.Value().maxAgeTime)
}

func (p *pool) triggerHealthCheck() {
	go func() {
		// Destroy is asynchronous so we give it time to actually remove itself from
		// the pool otherwise we might try to check the pool size too soon
		time.Sleep(500 * time.Millisecond)
		select {
		case p.healthCheckChan <- struct{}{}:
		default:
		}
	}()
}

func (p *pool) backgroundHealthCheck() {
	ticker := time.NewTicker(p.healthCheckPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-p.closeChan:
			return
		case <-p.healthCheckChan:
			p.checkHealth()
		case <-ticker.C:
			p.checkHealth()
		}
	}
}

func (p *pool) checkHealth() {
	for {
		// If checkMinConns failed we don't destroy any connections since we couldn't
		// even get to minConns
		if err := p.checkMinConns(); err != nil {
			// Should we log this error somewhere?
			break
		}
		if !p.checkConnsHealth() {
			// Since we didn't destroy any connections we can stop looping
			break
		}
		// Technically Destroy is asynchronous but 500ms should be enough for it to
		// remove it from the underlying pool
		select {
		case <-p.closeChan:
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// checkConnsHealth will check all idle connections, destroy a connection if
// it's idle or too old, and returns true if any were destroyed
func (p *pool) checkConnsHealth() bool {
	var destroyed bool
	totalConns := p.Stat().TotalConns()
	resources := p.p.AcquireAllIdle()
	for _, res := range resources {
		// We're okay going under minConns if the lifetime is up
		if p.isExpired(res) && totalConns >= p.minConns {
			atomic.AddInt64(&p.lifetimeDestroyCount, 1)
			res.Destroy()
			destroyed = true
			// Since Destroy is async we manually decrement totalConns.
			totalConns--
		} else if res.IdleDuration() > p.maxConnIdleTime && totalConns > p.minConns {
			atomic.AddInt64(&p.idleDestroyCount, 1)
			res.Destroy()
			destroyed = true
			// Since Destroy is async we manually decrement totalConns.
			totalConns--
		} else {
			res.ReleaseUnused()
		}
	}
	return destroyed
}

func (p *pool) checkMinConns() error {
	// TotalConns can include ones that are being destroyed but we should have
	// sleep(500ms) around all of the destroys to help prevent that from throwing
	// off this check
	toCreate := p.minConns - p.Stat().TotalConns()
	if toCreate > 0 {
		return p.createIdleResources(int(toCreate))
	}
	return nil
}

func (p *pool) createIdleResources(targetResources int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errs := make(chan error, targetResources)

	for i := 0; i < targetResources; i++ {
		go func() {
			err := p.p.CreateResource(ctx)
			// Ignore ErrNotAvailable since it means that the pool has become full since we started creating resource.
			if err == puddle.ErrNotAvailable {
				err = nil
			}
			errs <- err
		}()
	}

	var firstError error
	for i := 0; i < targetResources; i++ {
		err := <-errs
		if err != nil && firstError == nil {
			cancel()
			firstError = err
		}
	}

	return firstError
}

// Acquire returns a connection (Conn) from the Pool
func (p *pool) Acquire(ctx context.Context) (Conn, error) {
	for {
		res, err := p.p.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("acquire: %w", err)
		}

		cr := res.Value()

		if res.IdleDuration() > time.Second {
			err := cr.conn.Ping(ctx)
			if err != nil {
				res.Destroy()
				continue
			}
		}

		if p.beforeAcquire == nil || p.beforeAcquire(ctx, cr.conn) {
			return cr.getConn(p, res), nil
		}

		res.Destroy()
	}
}

// AcquireFunc acquires a Conn and calls f with that Conn. ctx will only affect the Acquire. It has no effect on the
// call of f. The return value is either an error acquiring the Conn or the return value of f. The Conn is
// automatically released after the call of f.
func (p *pool) AcquireFunc(ctx context.Context, f func(Conn) error) error {
	conn, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	return f(conn)
}

// AcquireAllIdle atomically acquires all currently idle connections. Its intended use is for health check and
// keep-alive functionality. It does not update pool statistics.
func (p *pool) AcquireAllIdle(ctx context.Context) []Conn {
	resources := p.p.AcquireAllIdle()
	conns := make([]Conn, 0, len(resources))
	for _, res := range resources {
		cr := res.Value()
		if p.beforeAcquire == nil || p.beforeAcquire(ctx, cr.conn) {
			conns = append(conns, cr.getConn(p, res))
		} else {
			res.Destroy()
		}
	}

	return conns
}

// Reset closes all connections, but leaves the pool open. It is intended for use when an error is detected that would
// disrupt all connections (such as a network interruption or a server state change).
//
// It is safe to reset a pool while connections are checked out. Those connections will be closed when they are returned
// to the pool.
func (p *pool) Reset() {
	p.p.Reset()
}

// Config returns a copy of config that was used to initialize this pool.
func (p *pool) Config() *Config { return p.config.Copy() }

// Stat returns a chpool.Stat struct with a snapshot of Pool statistics.
func (p *pool) Stat() *Stat {
	return &Stat{
		s:                    p.p.Stat(),
		newConnsCount:        atomic.LoadInt64(&p.newConnsCount),
		lifetimeDestroyCount: atomic.LoadInt64(&p.lifetimeDestroyCount),
		idleDestroyCount:     atomic.LoadInt64(&p.idleDestroyCount),
	}
}

// Exec acquires a connection from the Pool and executes the given SQL.
// SQL can be either a prepared statement name or an SQL string.
// The acquired connection is returned to the pool when the Exec function returns.
func (p *pool) Exec(ctx context.Context, query string) error {
	return p.ExecWithOption(ctx, query, nil)
}

func (p *pool) ExecWithOption(
	ctx context.Context,
	query string,
	queryOptions *chconn.QueryOptions,
) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	err = c.ExecWithOption(ctx, query, queryOptions)
	c.Release()
	return err
}

// Query acquires a connection and executes a query that returns chconn.Rows.
// See chconn.Rows documentation to close the returned Rows and return the acquired connection to the Pool.
//
// For better performance use Select instead of Query when possible. specially when you want to read al lot of data.
//
// If there is an error, the returned chconn.Rows will be returned in an error state.
// If preferred, ignore the error returned from Query and handle errors using the returned chconn.Rows.
func (p *pool) Query(ctx context.Context, sql string, args ...chconn.Parameter) (chconn.Rows, error) {
	return p.QueryWithOption(ctx, sql, nil, args...)
}

// QueryRow acquires a connection and executes a query that is expected
// to return at most one row (chconn.Row). Errors are deferred until chconn.Row's
// Scan method is called. If the query selects no rows, chconn.Row's Scan will
// return ErrNoRows. Otherwise, chconn.Row's Scan scans the first selected row
// and discards the rest. The acquired connection is returned to the Pool when
// chconn.Row's Scan method is called.
func (p *pool) QueryRow(ctx context.Context, sql string, args ...chconn.Parameter) chconn.Row {
	return p.QueryRowWithOption(ctx, sql, nil, args...)
}

// Query acquires a connection and executes a query with a query option that returns chconn.Rows.
// See chconn.Rows documentation to close the returned Rows and return the acquired connection to the Pool.
//
// For better performance use Select instead of Query when possible. specially when you want to read al lot of data.
//
// If there is an error, the returned chconn.Rows will be returned in an error state.
// If preferred, ignore the error returned from Query and handle errors using the returned chconn.Rows.
func (p *pool) QueryWithOption(
	ctx context.Context,
	sql string,
	queryOption *chconn.QueryOptions,
	args ...chconn.Parameter,
) (chconn.Rows, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return errRows{err: err}, err
	}

	rows, err := c.QueryWithOption(ctx, sql, queryOption, args...)
	if err != nil {
		c.Release()
		return errRows{err: err}, err
	}

	return c.getPoolRows(rows), nil
}

// QueryRow acquires a connection and executes a query with a query option that is expected
// to return at most one row (chconn.Row). Errors are deferred until chconn.Row's
// Scan method is called. If the query selects no rows, chconn.Row's Scan will
// return ErrNoRows. Otherwise, chconn.Row's Scan scans the first selected row
// and discards the rest. The acquired connection is returned to the Pool when
// chconn.Row's Scan method is called.
func (p *pool) QueryRowWithOption(ctx context.Context, sql string, queryOption *chconn.QueryOptions, args ...chconn.Parameter) chconn.Row {
	c, err := p.Acquire(ctx)
	if err != nil {
		return errRow{err: err}
	}

	row := c.QueryRowWithOption(ctx, sql, queryOption, args...)
	return c.getPoolRow(row)
}

func (p *pool) Select(ctx context.Context, query string, columns ...column.ColumnBasic) (chconn.SelectStmt, error) {
	return p.SelectWithOption(ctx, query, nil, columns...)
}

func (p *pool) SelectWithOption(
	ctx context.Context,
	query string,
	queryOptions *chconn.QueryOptions,
	columns ...column.ColumnBasic,
) (chconn.SelectStmt, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	s, err := c.SelectWithOption(ctx, query, queryOptions, columns...)
	if err != nil {
		c.Release()
		return nil, err
	}
	return s, nil
}

func (p *pool) Insert(ctx context.Context, query string, columns ...column.ColumnBasic) error {
	return p.InsertWithOption(ctx, query, nil, columns...)
}

func (p *pool) InsertWithOption(ctx context.Context, query string, queryOptions *chconn.QueryOptions, columns ...column.ColumnBasic) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}

	err = c.InsertWithOption(ctx, query, queryOptions, columns...)
	c.Release()
	return err
}

func (p *pool) InsertStream(ctx context.Context, query string) (chconn.InsertStmt, error) {
	return p.InsertStreamWithOption(ctx, query, nil)
}

func (p *pool) InsertStreamWithOption(ctx context.Context, query string, queryOptions *chconn.QueryOptions) (chconn.InsertStmt, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	s, err := c.InsertStreamWithOption(ctx, query, queryOptions)
	if err != nil {
		c.Release()
		return nil, err
	}
	return s, nil
}

// Ping acquires a connection from the Pool and send ping
// If returns without error, the database Ping is considered successful, otherwise, the error is returned.
func (p *pool) Ping(ctx context.Context) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	err = c.Ping(ctx)
	c.Release()
	return err
}
