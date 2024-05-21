package chpool

import (
	"context"
	"sync/atomic"

	puddle "github.com/jackc/puddle/v2"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
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
	Query(ctx context.Context, sql string, args ...chconn.Parameter) (chconn.Rows, error)
	QueryWithOption(ctx context.Context, sql string, queryOptions *chconn.QueryOptions, args ...chconn.Parameter) (chconn.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...chconn.Parameter) chconn.Row
	QueryRowWithOption(ctx context.Context, sql string, queryOptions *chconn.QueryOptions, args ...chconn.Parameter) chconn.Row

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

	getPoolRow(r chconn.Row) *poolRow
	getPoolRows(r chconn.Rows) *poolRows
}
type conn struct {
	res *puddle.Resource[*connResource]
	p   *pool
}

// Release returns c to the pool it was acquired from. Once Release has been called, other methods must not be called.
// However, it is safe to call Release multiple times. Subsequent calls after the first will be ignored.
func (ch *conn) Release() {
	if ch.res == nil {
		return
	}

	conn := ch.Conn()
	res := ch.res
	ch.res = nil

	if conn.IsClosed() || conn.IsBusy() {
		res.Destroy()
		// Signal to the health check to run since we just destroyed a connections
		// and we might be below minConns now
		ch.p.triggerHealthCheck()
		return
	}

	// If the pool is consistently being used, we might never get to check the
	// lifetime of a connection since we only check idle connections in checkConnsHealth
	// so we also check the lifetime here and force a health check
	if ch.p.isExpired(res) {
		atomic.AddInt64(&ch.p.lifetimeDestroyCount, 1)
		res.Destroy()
		// Signal to the health check to run since we just destroyed a connections
		// and we might be below minConns now
		ch.p.triggerHealthCheck()
		return
	}

	if ch.p.afterRelease == nil {
		res.Release()
		return
	}

	go func() {
		if ch.p.afterRelease(conn) {
			res.Release()
		} else {
			res.Destroy()
			// Signal to the health check to run since we just destroyed a connections
			// and we might be below minConns now
			ch.p.triggerHealthCheck()
		}
	}()
}

// Hijack assumes ownership of the connection from the pool. Caller is responsible for closing the connection. Hijack
// will panic if called on an already released or hijacked connection.
func (ch *conn) Hijack() chconn.Conn {
	if ch.res == nil {
		panic("cannot hijack already released or hijacked connection")
	}

	conn := ch.Conn()
	res := ch.res
	ch.res = nil

	res.Hijack()

	return conn
}

func (ch *conn) ExecWithOption(
	ctx context.Context,
	query string,
	queryOptions *chconn.QueryOptions,
) error {
	return ch.Conn().ExecWithOption(ctx, query, queryOptions)
}

func (ch *conn) Query(ctx context.Context, sql string, args ...chconn.Parameter) (chconn.Rows, error) {
	return ch.Conn().Query(ctx, sql, args...)
}

func (ch *conn) QueryWithOption(
	ctx context.Context,
	sql string,
	queryOptions *chconn.QueryOptions,
	args ...chconn.Parameter,
) (chconn.Rows, error) {
	return ch.Conn().QueryWithOption(ctx, sql, queryOptions, args...)
}

func (ch *conn) QueryRow(ctx context.Context, sql string, args ...chconn.Parameter) chconn.Row {
	return ch.Conn().QueryRow(ctx, sql, args...)
}

func (ch *conn) QueryRowWithOption(
	ctx context.Context,
	sql string,
	queryOptions *chconn.QueryOptions,
	args ...chconn.Parameter,
) chconn.Row {
	return ch.Conn().QueryRowWithOption(ctx, sql, queryOptions, args...)
}

func (ch *conn) Ping(ctx context.Context) error {
	return ch.Conn().Ping(ctx)
}

func (ch *conn) SelectWithOption(
	ctx context.Context,
	query string,
	queryOptions *chconn.QueryOptions,
	columns ...column.ColumnBasic,
) (chconn.SelectStmt, error) {
	s, err := ch.Conn().SelectWithOption(ctx, query, queryOptions, columns...)
	if err != nil {
		return nil, err
	}
	return &selectStmt{
		SelectStmt: s,
		conn:       ch,
	}, nil
}

func (ch *conn) InsertWithOption(
	ctx context.Context,
	query string,
	queryOptions *chconn.QueryOptions,
	columns ...column.ColumnBasic,
) error {
	return ch.Conn().InsertWithOption(ctx, query, queryOptions, columns...)
}

func (ch *conn) InsertStreamWithOption(
	ctx context.Context,
	query string,
	queryOptions *chconn.QueryOptions,
) (chconn.InsertStmt, error) {
	s, err := ch.Conn().InsertStreamWithOption(ctx, query, queryOptions)
	if err != nil {
		return nil, err
	}
	return &insertStmt{
		InsertStmt: s,
		conn:       ch,
	}, nil
}

func (ch *conn) Conn() chconn.Conn {
	return ch.connResource().conn
}

func (ch *conn) connResource() *connResource {
	return ch.res.Value()
}

func (ch *conn) getPoolRow(r chconn.Row) *poolRow {
	return ch.connResource().getPoolRow(ch, r)
}

func (ch *conn) getPoolRows(r chconn.Rows) *poolRows {
	return ch.connResource().getPoolRows(ch, r)
}
