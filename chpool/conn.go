package chpool

import (
	"context"
	"time"

	"github.com/jackc/puddle"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/setting"
)

// Conn is an acquired *chconn.Conn from a Pool.
type Conn interface {
	Release()
	// ExecCallback executes a query without returning any rows with the setting option and on progress callback.
	// NOTE: don't use it for insert and select query
	ExecCallback(
		ctx context.Context,
		query string,
		settings *setting.Settings,
		queryID string,
		onProgress func(*chconn.Progress)) (interface{}, error)
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
	// InsertWithSetting executes a query with the setting option and return insert stmt.
	// NOTE: only use for insert query
	InsertWithSetting(ctx context.Context, query string, settings *setting.Settings, queryID string) (chconn.InsertStmt, error)
	Conn() chconn.Conn
	Ping(ctx context.Context) error
}
type conn struct {
	res *puddle.Resource
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

	now := time.Now()
	if conn.IsClosed() || conn.IsBusy() || (now.Sub(res.CreationTime()) > c.p.maxConnLifetime) {
		res.Destroy()
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
		}
	}()
}

func (c *conn) ExecCallback(
	ctx context.Context,
	query string,
	settings *setting.Settings,
	queryID string,
	onProgress func(*chconn.Progress)) (interface{}, error) {
	return c.Conn().ExecCallback(ctx, query, settings, queryID, onProgress)
}

func (c *conn) Ping(ctx context.Context) error {
	return c.Conn().Ping(ctx)
}

func (c *conn) SelectCallback(
	ctx context.Context,
	query string,
	settings *setting.Settings,
	queryID string,
	onProgress func(*chconn.Progress),
	onProfile func(*chconn.Profile),
) (chconn.SelectStmt, error) {
	s, err := c.Conn().SelectCallback(ctx, query, settings, queryID, onProgress, onProfile)
	if err != nil {
		return nil, err
	}
	return &selectStmt{
		SelectStmt: s,
		conn:       c,
	}, nil
}

func (c *conn) InsertWithSetting(ctx context.Context, query string, settings *setting.Settings, queryID string) (chconn.InsertStmt, error) {
	s, err := c.Conn().InsertWithSetting(ctx, query, settings, queryID)
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
	return c.res.Value().(*connResource)
}
