package chpool

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/puddle"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
	"github.com/vahid-sohrabloo/chconn/setting"
)

var defaultMaxConns = int32(4)
var defaultMinConns = int32(0)
var defaultMaxConnLifetime = time.Hour
var defaultMaxConnIdleTime = time.Minute * 30
var defaultHealthCheckPeriod = time.Minute

type connResource struct {
	conn  chconn.Conn
	conns []conn
}

func (cr *connResource) getConn(p *pool, res *puddle.Resource) Conn {
	if len(cr.conns) == 0 {
		cr.conns = make([]conn, 128)
	}

	c := &cr.conns[len(cr.conns)-1]
	cr.conns = cr.conns[0 : len(cr.conns)-1]

	c.res = res
	c.p = p

	return c
}

// Pool is a connection pool for chconn
type Pool interface {
	// Close closes all connections in the pool and rejects future Acquire calls. Blocks until all connections are returned
	// to pool and closed.
	Close()
	Acquire(ctx context.Context) (Conn, error)
	// AcquireFunc acquires a *Conn and calls f with that *Conn. ctx will only affect the Acquire. It has no effect on the
	// call of f. The return value is either an error acquiring the Conn or the return value of f. The Conn is
	// automatically released after the call of f.
	AcquireFunc(ctx context.Context, f func(Conn) error) error
	// AcquireAllIdle atomically acquires all currently idle connections. Its intended use is for health check and
	// keep-alive functionality. It does not update pool statistics.
	AcquireAllIdle(ctx context.Context) []Conn
	// Exec executes a query without returning any rows.
	// NOTE: don't use it for insert and select query
	Exec(ctx context.Context, sql string) (interface{}, error)
	// ExecWithSetting executes a query without returning any rows with the setting option.
	// NOTE: don't use it for insert and select query
	ExecWithSetting(ctx context.Context, query string, settings *setting.Settings) (interface{}, error)
	// ExecCallback executes a query without returning any rows with the setting option and on progress callback.
	// NOTE: don't use it for insert and select query
	ExecCallback(
		ctx context.Context,
		sql string,
		settings *setting.Settings,
		queryID string,
		onProgress func(*chconn.Progress),
	) (interface{}, error)
	// Select executes a query and return select stmt.
	// NOTE: only use for select query
	Select(ctx context.Context, query string) (chconn.SelectStmt, error)
	// Select executes a query with the setting option and return select stmt.
	// NOTE: only use for select query
	SelectWithSetting(ctx context.Context, query string, settings *setting.Settings) (chconn.SelectStmt, error)
	// Select executes a query with the setting option, on progress callback, on profile callback and return select stmt.
	// NOTE: only use for select query
	SelectCallback(
		ctx context.Context,
		query string,
		settings *setting.Settings,
		queryID string,
		onProgress func(*chconn.Progress),
		onProfile func(*chconn.Profile),
	) (chconn.SelectStmt, error)
	// Insert executes a query and commit all columns
	// NOTE: only use for insert query
	Insert(ctx context.Context, query string, columns ...column.Column) error
	// InsertWithSetting executes a query with the setting option and commit all columns
	// NOTE: only use for insert query
	InsertWithSetting(ctx context.Context, query string, settings *setting.Settings, queryID string, columns ...column.Column) error
	// Ping sends a ping to check that the connection to the server is alive.
	Ping(ctx context.Context) error
	Stat() *Stat
}
type pool struct {
	p                 *puddle.Pool
	config            *Config
	beforeConnect     func(context.Context, *chconn.Config) error
	afterConnect      func(context.Context, chconn.Conn) error
	beforeAcquire     func(context.Context, chconn.Conn) bool
	afterRelease      func(chconn.Conn) bool
	minConns          int32
	maxConnLifetime   time.Duration
	maxConnIdleTime   time.Duration
	healthCheckPeriod time.Duration

	closeOnce sync.Once
	closeChan chan struct{}
}

// Config is the configuration struct for creating a pool. It must be created by ParseConfig and then it can be
// modified. A manually initialized Config will cause ConnectConfig to panic.
type Config struct {
	ConnConfig *chconn.Config
	// BeforeConnect is called before a new connection is made. It is passed a copy of the underlying chconn.Config and
	// will not impact any existing open connections.
	BeforeConnect func(context.Context, *chconn.Config) error

	// AfterConnect is called after a connection is established, but before it is added to the pool.
	AfterConnect func(context.Context, chconn.Conn) error

	// BeforeAcquire is called before before a connection is acquired from the pool. It must return true to allow the
	// acquire or false to indicate that the connection should be destroyed and a different connection should be
	// acquired.
	BeforeAcquire func(context.Context, chconn.Conn) bool

	// AfterRelease is called after a connection is released, but before it is returned to the pool. It must return true to
	// return the connection to the pool or false to destroy the connection.
	AfterRelease func(chconn.Conn) bool

	// MaxConnLifetime is the duration since creation after which a connection will be automatically closed.
	MaxConnLifetime time.Duration

	// MaxConnIdleTime is the duration after which an idle connection will be automatically closed by the health check.
	MaxConnIdleTime time.Duration

	// MaxConns is the maximum size of the pool.
	MaxConns int32

	// MinConns is the minimum size of the pool. The health check will increase the number of connections to this
	// amount if it had dropped below.
	MinConns int32

	// HealthCheckPeriod is the duration between checks of the health of idle connections.
	HealthCheckPeriod time.Duration

	// If set to true, pool doesn't do any I/O operation on initialization.
	// And connects to the server only when the pool starts to be used.
	// The default is false.
	LazyConnect bool

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

// ConnString returns the original connection string used to connect to the ClickHouse server.
func (c *Config) ConnString() string { return c.ConnConfig.ConnString() }

// Connect creates a new Pool and immediately establishes one connection. ctx can be used to cancel this initial
// connection. See ParseConfig for information on connString format.
func Connect(ctx context.Context, connString string) (Pool, error) {
	config, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return ConnectConfig(ctx, config)
}

// ConnectConfig creates a new Pool and immediately establishes one connection. ctx can be used to cancel this initial
// connection. config must have been created by ParseConfig.
func ConnectConfig(ctx context.Context, config *Config) (Pool, error) {
	// Default values are set in ParseConfig. Enforce initial creation by ParseConfig rather than setting defaults from
	// zero values.
	if !config.createdByParseConfig {
		panic("config must be created by ParseConfig")
	}

	p := &pool{
		config:            config,
		beforeConnect:     config.BeforeConnect,
		afterConnect:      config.AfterConnect,
		beforeAcquire:     config.BeforeAcquire,
		afterRelease:      config.AfterRelease,
		minConns:          config.MinConns,
		maxConnLifetime:   config.MaxConnLifetime,
		maxConnIdleTime:   config.MaxConnIdleTime,
		healthCheckPeriod: config.HealthCheckPeriod,
		closeChan:         make(chan struct{}),
	}

	p.p = puddle.NewPool(
		func(ctx context.Context) (interface{}, error) {
			connConfig := config.ConnConfig
			if p.beforeConnect != nil {
				connConfig = p.config.ConnConfig.Copy()
				if err := p.beforeConnect(ctx, connConfig); err != nil {
					return nil, err
				}
			}

			c, err := chconn.ConnectConfig(ctx, connConfig)
			if err != nil {
				return nil, err
			}

			if p.afterConnect != nil {
				err = p.afterConnect(ctx, c)
				if err != nil {
					c.Close(ctx)
					return nil, err
				}
			}

			cr := &connResource{
				conn:  c,
				conns: make([]conn, 64),
			}

			return cr, nil
		},
		func(value interface{}) {
			ctxDestroy, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			conn := value.(*connResource).conn
			conn.Close(ctxDestroy)
			cancel()
		},
		config.MaxConns,
	)

	go p.backgroundHealthCheck()

	if !config.LazyConnect {
		// Initially establish one connection
		res, err := p.p.Acquire(ctx)
		if err != nil {
			p.Close()
			return nil, err
		}
		res.Release()
	}

	return p, nil
}

// ParseConfig builds a Config from connString. It parses connString with the same behavior as chconn.ParseConfig with the
// addition of the following variables:
//
// pool_max_conns: integer greater than 0
// pool_min_conns: integer 0 or greater
// pool_max_conn_lifetime: duration string
// pool_max_conn_idle_time: duration string
// pool_health_check_period: duration string
//
// See Config for definitions of these arguments.
//
//   # Example DSN
//   user=vahid password=secret host=clickhouse.example.com port=9000 dbname=mydb sslmode=verify-ca pool_max_conns=10
//
//   # Example URL
//   clickhouse://vahid:secret@ch.example.com:9000/mydb?sslmode=verify-ca&pool_max_conns=10
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

func (p *pool) backgroundHealthCheck() {
	ticker := time.NewTicker(p.healthCheckPeriod)

	for {
		select {
		case <-p.closeChan:
			ticker.Stop()
			return
		case <-ticker.C:
			p.checkIdleConnsHealth()
			p.checkMinConns()
		}
	}
}

func (p *pool) checkIdleConnsHealth() {
	resources := p.p.AcquireAllIdle()

	now := time.Now()
	for _, res := range resources {
		if now.Sub(res.CreationTime()) > p.maxConnLifetime {
			res.Destroy()
		} else if res.IdleDuration() > p.maxConnIdleTime {
			res.Destroy()
		} else {
			res.ReleaseUnused()
		}
	}
}

func (p *pool) checkMinConns() {
	for i := p.minConns - p.Stat().TotalConns(); i > 0; i-- {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			p.p.CreateResource(ctx) //nolint:errcheck //no needed
		}()
	}
}

// Acquire returns a connection (Conn) from the Pool
func (p *pool) Acquire(ctx context.Context) (Conn, error) {
	for {
		res, err := p.p.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("acquire: %w", err)
		}

		cr := res.Value().(*connResource)
		if p.beforeAcquire == nil || p.beforeAcquire(ctx, cr.conn) {
			return cr.getConn(p, res), nil
		}

		res.Destroy()
	}
}

// AcquireFunc acquires a *Conn and calls f with that *Conn. ctx will only affect the Acquire. It has no effect on the
// call of f. The return value is either an error acquiring the *Conn or the return value of f. The *Conn is
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
		cr := res.Value().(*connResource)
		if p.beforeAcquire == nil || p.beforeAcquire(ctx, cr.conn) {
			conns = append(conns, cr.getConn(p, res))
		} else {
			res.Destroy()
		}
	}

	return conns
}

// Config returns a copy of config that was used to initialize this pool.
func (p *pool) Config() *Config { return p.config.Copy() }

func (p *pool) Stat() *Stat {
	return &Stat{s: p.p.Stat()}
}

func (p *pool) Exec(ctx context.Context, sql string) (interface{}, error) {
	return p.ExecCallback(ctx, sql, nil, "", nil)
}

func (p *pool) ExecWithSetting(ctx context.Context, sql string, settings *setting.Settings) (interface{}, error) {
	return p.ExecCallback(ctx, sql, settings, "", nil)
}

func (p *pool) ExecCallback(
	ctx context.Context,
	sql string,
	settings *setting.Settings,
	queryID string,
	onProgress func(*chconn.Progress)) (interface{}, error) {
	for {
		c, err := p.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		res, err := c.ExecCallback(ctx, sql, settings, queryID, onProgress)
		c.Release()
		if errors.Is(err, syscall.EPIPE) {
			continue
		}
		return res, err
	}
}

func (p *pool) Select(ctx context.Context, query string) (chconn.SelectStmt, error) {
	return p.SelectCallback(ctx, query, nil, "", nil, nil)
}

func (p *pool) SelectWithSetting(ctx context.Context, query string, settings *setting.Settings) (chconn.SelectStmt, error) {
	return p.SelectCallback(ctx, query, settings, "", nil, nil)
}

func (p *pool) SelectCallback(
	ctx context.Context,
	query string,
	settings *setting.Settings,
	queryID string,
	onProgress func(*chconn.Progress),
	onProfile func(*chconn.Profile),
) (chconn.SelectStmt, error) {
	for {
		c, err := p.Acquire(ctx)
		if err != nil {
			return nil, err
		}

		s, err := c.SelectCallback(ctx, query, settings, queryID, onProgress, onProfile)
		if err != nil {
			c.Release()
			if errors.Is(err, syscall.EPIPE) {
				continue
			}
			return nil, err
		}
		return s, nil
	}
}

func (p *pool) Insert(ctx context.Context, query string, columns ...column.Column) error {
	return p.InsertWithSetting(ctx, query, nil, "", columns...)
}

func (p *pool) InsertWithSetting(
	ctx context.Context,
	query string,
	settings *setting.Settings,
	queryID string,
	columns ...column.Column) error {
	for {
		c, err := p.Acquire(ctx)
		if err != nil {
			return err
		}

		err = c.InsertWithSetting(ctx, query, settings, queryID, columns...)
		c.Release()
		if err != nil && errors.Is(err, syscall.EPIPE) {
			continue
		}
		return err
	}
}

// Ping acquires a connection from the Pool and send ping
// If returns without error, the database Ping is considered successful, otherwise, the error is returned.
func (p *pool) Ping(ctx context.Context) error {
	for {
		c, err := p.Acquire(ctx)
		if err != nil {
			return err
		}
		err = c.Ping(ctx)
		c.Release()
		if errors.Is(err, syscall.EPIPE) {
			continue
		}
		return err
	}
}
