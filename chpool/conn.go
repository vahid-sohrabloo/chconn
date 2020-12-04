package chpool

import (
	"context"
	"time"

	"github.com/jackc/puddle"
	"github.com/vahid-sohrabloo/chconn"
)

// Conn is an acquired *chconn.Conn from a Pool.
type Conn interface {
	Release()
	ExecCallback(
		ctx context.Context,
		query string,
		setting *chconn.Settings,
		onProgress func(*chconn.Progress)) (interface{}, error)
	SelectCallback(
		ctx context.Context,
		query string,
		setting *chconn.Settings,
		onProgress func(*chconn.Progress),
		onProfile func(*chconn.Profile),
	) (chconn.SelectStmt, error)
	InsertWithSetting(ctx context.Context, query string, setting *chconn.Settings) (chconn.InsertStmt, error)
	Conn() chconn.Conn
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
	setting *chconn.Settings,
	onProgress func(*chconn.Progress)) (interface{}, error) {
	return c.Conn().ExecCallback(ctx, query, setting, onProgress)
}
func (c *conn) SelectCallback(
	ctx context.Context,
	query string,
	setting *chconn.Settings,
	onProgress func(*chconn.Progress),
	onProfile func(*chconn.Profile),
) (chconn.SelectStmt, error) {
	s, err := c.Conn().SelectCallback(ctx, query, setting, onProgress, onProfile)
	if err != nil {
		return nil, err
	}
	return &selectStmt{
		SelectStmt: s,
		conn:       c,
	}, nil
}

func (c *conn) InsertWithSetting(ctx context.Context, query string, setting *chconn.Settings) (chconn.InsertStmt, error) {
	s, err := c.Conn().InsertWithSetting(ctx, query, setting)
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
