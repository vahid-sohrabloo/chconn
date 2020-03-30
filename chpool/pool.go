package chpool

import (
	"context"
	"runtime"
	"strconv"
	"time"

	"github.com/jackc/puddle"
	"github.com/vahid-sohrabloo/chconn"
	errors "golang.org/x/xerrors"
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

type Pool interface {
	Close()
	Acquire(ctx context.Context) (Conn, error)
	AcquireAllIdle(ctx context.Context) []Conn
	Exec(ctx context.Context, sql string) (interface{}, error)
	ExecCallback(ctx context.Context, sql string, onProgress func(*chconn.Progress)) (interface{}, error)
	Select(ctx context.Context, query string) (chconn.SelectStmt, error)
	SelectCallback(
		ctx context.Context,
		query string,
		onProgress func(*chconn.Progress),
		onProfile func(*chconn.Profile),
	) (chconn.SelectStmt, error)
	Insert(ctx context.Context, query string) (chconn.InsertStmt, error)
	Stat() *Stat
}
type pool struct {
	p                 *puddle.Pool
	afterConnect      func(context.Context, chconn.Conn) error
	beforeAcquire     func(context.Context, chconn.Conn) bool
	afterRelease      func(chconn.Conn) bool
	minConns          int32
	maxConnLifetime   time.Duration
	maxConnIdleTime   time.Duration
	healthCheckPeriod time.Duration
	closeChan         chan struct{}
}

// Config is the configuration struct for creating a pool. It must be created by ParseConfig and then it can be
// modified. A manually initialized Config will cause ConnectConfig to panic.
type Config struct {
	Config *chconn.Config

	// AfterConnect is called after a connection is established, but before it is added to the pool.
	AfterConnect func(context.Context, chconn.Conn) error

	// BeforeAcquire is called before before a connection is acquired from the pool. It must return true to allow the
	// acquision or false to indicate that the connection should be destroyed and a different connection should be
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

	createdByParseConfig bool // Used to enforce created by ParseConfig rule.
}

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
			c, err := chconn.ConnectConfig(ctx, config.Config)
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
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				value.(*connResource).conn.Close(ctx)
				cancel()
			}()
		},
		config.MaxConns,
	)

	go p.backgroundHealthCheck()

	// Initially establish one connection
	res, err := p.p.Acquire(ctx)
	if err != nil {
		p.p.Close()
		return nil, err
	}
	res.Release()

	return p, nil
}

// ParseConfig builds a Config from connString. It parses connString with the same behavior as pgx.ParseConfig with the
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
//   user=jack password=secret host=clickhouse.example.com port=9000 dbname=mydb sslmode=verify-ca pool_max_conns=10
//
//   # Example URL
//   clickhouse://jack:secret@ch.example.com:9000/mydb?sslmode=verify-ca&pool_max_conns=10
func ParseConfig(connString string) (*Config, error) {
	chConfig, err := chconn.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	config := &Config{
		Config:               chConfig,
		createdByParseConfig: true,
	}

	if s, ok := config.Config.RuntimeParams["pool_max_conns"]; ok {
		delete(config.Config.RuntimeParams, "pool_max_conns")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, errors.Errorf("cannot parse pool_max_conns: %w", err)
		}
		if n < 1 {
			return nil, errors.Errorf("pool_max_conns too small: %d", n)
		}
		config.MaxConns = int32(n)
	} else {
		config.MaxConns = defaultMaxConns
		if numCPU := int32(runtime.NumCPU()); numCPU > config.MaxConns {
			config.MaxConns = numCPU
		}
	}

	if s, ok := config.Config.RuntimeParams["pool_min_conns"]; ok {
		delete(config.Config.RuntimeParams, "pool_min_conns")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, errors.Errorf("cannot parse pool_min_conns: %w", err)
		}
		config.MinConns = int32(n)
	} else {
		config.MinConns = defaultMinConns
	}

	if s, ok := config.Config.RuntimeParams["pool_max_conn_lifetime"]; ok {
		delete(config.Config.RuntimeParams, "pool_max_conn_lifetime")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, errors.Errorf("invalid pool_max_conn_lifetime: %w", err)
		}
		config.MaxConnLifetime = d
	} else {
		config.MaxConnLifetime = defaultMaxConnLifetime
	}

	if s, ok := config.Config.RuntimeParams["pool_max_conn_idle_time"]; ok {
		delete(config.Config.RuntimeParams, "pool_max_conn_idle_time")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, errors.Errorf("invalid pool_max_conn_idle_time: %w", err)
		}
		config.MaxConnIdleTime = d
	} else {
		config.MaxConnIdleTime = defaultMaxConnIdleTime
	}

	if s, ok := config.Config.RuntimeParams["pool_health_check_period"]; ok {
		delete(config.Config.RuntimeParams, "pool_health_check_period")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, errors.Errorf("invalid pool_health_check_period: %w", err)
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
	close(p.closeChan)
	p.p.Close()
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
			p.p.CreateResource(ctx) //nolint:errcheck not needed
		}()
	}
}

func (p *pool) Acquire(ctx context.Context) (Conn, error) {
	for {
		res, err := p.p.Acquire(ctx)
		if err != nil {
			return nil, err
		}

		cr := res.Value().(*connResource)
		if p.beforeAcquire == nil || p.beforeAcquire(ctx, cr.conn) {
			return cr.getConn(p, res), nil
		}

		res.Destroy()
	}
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

func (p *pool) Stat() *Stat {
	return &Stat{s: p.p.Stat()}
}

func (p *pool) Exec(ctx context.Context, sql string) (interface{}, error) {
	return p.ExecCallback(ctx, sql, nil)
}
func (p *pool) ExecCallback(ctx context.Context, sql string, onProgress func(*chconn.Progress)) (interface{}, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Release()

	return c.ExecCallback(ctx, sql, onProgress)
}

func (p *pool) Select(ctx context.Context, query string) (chconn.SelectStmt, error) {
	return p.SelectCallback(ctx, query, nil, nil)
}
func (p *pool) SelectCallback(
	ctx context.Context,
	query string,
	onProgress func(*chconn.Progress),
	onProfile func(*chconn.Profile),
) (chconn.SelectStmt, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	s, err := c.SelectCallback(ctx, query, onProgress, onProfile)
	if err != nil {
		c.Release()
		return nil, err
	}

	return s, nil
}

func (p *pool) Insert(ctx context.Context, query string) (chconn.InsertStmt, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	s, err := c.Insert(ctx, query)
	if err != nil {
		c.Release()
		return nil, err
	}

	return s, nil
}
