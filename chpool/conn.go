package chpool

import (
	"context"
	"sync/atomic"

	puddle "github.com/jackc/puddle/v2"
	"github.com/vahid-sohrabloo/chconn/v2"
	"github.com/vahid-sohrabloo/chconn/v2/column"
)

// Conn is an acquired *chconn.Conn from a Pool.
type Conn interface {
	Release()
	// ExecWithOption executes a query without returning any rows with Query options.
	// NOTE: don't use it for insert and select query
	ExecWithOption(
		ctx context.Context,
		query string,
		queryOptions *chconn.QueryOptions,
	) error
	// Select executes a query with the the query options and return select stmt.
	// NOTE: only use for select query
	SelectWithOption(
		ctx context.Context,
		query string,
		queryOptions *chconn.QueryOptions,
		columns ...column.ColumnBasic,
	) (chconn.SelectStmt, error)
	// InsertWithSetting executes a query with the query options and commit all columns data.
	// NOTE: only use for insert query
	InsertWithOption(ctx context.Context, query string, queryOptions *chconn.QueryOptions, columns ...column.ColumnBasic) error
	// InsertWithSetting executes a query with the query options and commit all columns data.
	// NOTE: only use for insert query
	InsertStreamWithOption(ctx context.Context, query string, queryOptions *chconn.QueryOptions) (chconn.InsertStmt, error)
	// Conn get the underlying chconn.Conn
	Conn() chconn.Conn
	// Hijack assumes ownership of the connection from the pool. Caller is responsible for closing the connection. Hijack
	// will panic if called on an already released or hijacked connection.
	Hijack() chconn.Conn
	Ping(ctx context.Context) error
}
type conn struct {
	res *puddle.Resource[*connResource]
	p   *pool
}

// Release returns c to the pool it was acquired from. Once Release has been called, other methods must not be called.
// However, it is safe to call Release multiple times. Subsequent calls after the first will be ignored.
func (c *conn) Release() {
	if c.res == nil {
		return
	}

	conn := c.Conn()
	res := c.res
	c.res = nil

	if conn.IsClosed() || conn.IsBusy() {
		res.Destroy()
		// Signal to the health check to run since we just destroyed a connections
		// and we might be below minConns now
		c.p.triggerHealthCheck()
		return
	}

	// If the pool is consistently being used, we might never get to check the
	// lifetime of a connection since we only check idle connections in checkConnsHealth
	// so we also check the lifetime here and force a health check
	if c.p.isExpired(res) {
		atomic.AddInt64(&c.p.lifetimeDestroyCount, 1)
		res.Destroy()
		// Signal to the health check to run since we just destroyed a connections
		// and we might be below minConns now
		c.p.triggerHealthCheck()
		return
	}

	if c.p.afterRelease == nil {
		res.Release()
		return
	}

	go func() {
		if c.p.afterRelease(conn) {
			res.Release()
		} else {
			res.Destroy()
			// Signal to the health check to run since we just destroyed a connections
			// and we might be below minConns now
			c.p.triggerHealthCheck()
		}
	}()
}

// Hijack assumes ownership of the connection from the pool. Caller is responsible for closing the connection. Hijack
// will panic if called on an already released or hijacked connection.
func (c *conn) Hijack() chconn.Conn {
	if c.res == nil {
		panic("cannot hijack already released or hijacked connection")
	}

	conn := c.Conn()
	res := c.res
	c.res = nil

	res.Hijack()

	return conn
}

func (c *conn) ExecWithOption(
	ctx context.Context,
	query string,
	queryOptions *chconn.QueryOptions,
) error {
	return c.Conn().ExecWithOption(ctx, query, queryOptions)
}

func (c *conn) Ping(ctx context.Context) error {
	return c.Conn().Ping(ctx)
}

func (c *conn) SelectWithOption(
	ctx context.Context,
	query string,
	queryOptions *chconn.QueryOptions,
	columns ...column.ColumnBasic,
) (chconn.SelectStmt, error) {
	s, err := c.Conn().SelectWithOption(ctx, query, queryOptions, columns...)
	if err != nil {
		return nil, err
	}
	return &selectStmt{
		SelectStmt: s,
		conn:       c,
	}, nil
}

func (c *conn) InsertWithOption(ctx context.Context, query string, queryOptions *chconn.QueryOptions, columns ...column.ColumnBasic) error {
	return c.Conn().InsertWithOption(ctx, query, queryOptions, columns...)
}

func (c *conn) InsertStreamWithOption(ctx context.Context, query string, queryOptions *chconn.QueryOptions) (chconn.InsertStmt, error) {
	s, err := c.Conn().InsertStreamWithOption(ctx, query, queryOptions)
	if err != nil {
		return nil, err
	}
	return &insertStmt{
		InsertStmt: s,
		conn:       c,
	}, nil
}

func (c *conn) Conn() chconn.Conn {
	return c.connResource().conn
}

func (c *conn) connResource() *connResource {
	return c.res.Value()
}
