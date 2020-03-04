package chpool

import (
	"context"
	"time"

	"github.com/jackc/puddle"
	"github.com/vahid-sohrabloo/chconn"
)

// Conn is an acquired *pgx.Conn from a Pool.
type Conn struct {
	res *puddle.Resource
	p   *Pool
}

// Release returns c to the pool it was acquired from. Once Release has been called, other methods must not be called.
// However, it is safe to call Release multiple times. Subsequent calls after the first will be ignored.
func (c *Conn) Release() {
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

func (c *Conn) Exec(ctx context.Context, query string) (interface{}, error) {
	return c.Conn().Exec(ctx, query)
}
func (c *Conn) Select(ctx context.Context, query string) (*SelectStmt, error) {
	s, err := c.Conn().Select(ctx, query)
	if err != nil {
		return nil, err
	}
	return &SelectStmt{
		SelectStmt: s,
		conn:       c,
	}, nil
}

func (c *Conn) Insert(ctx context.Context, query string) (*InsertStmt, error) {
	s, err := c.Conn().Insert(ctx, query)
	if err != nil {
		return nil, err
	}
	return &InsertStmt{
		InsertStmt: s,
		conn:       c,
	}, nil
}

// func (c *Conn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
// 	return c.Conn().Query(ctx, sql, args...)
// }

// func (c *Conn) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
// 	return c.Conn().QueryRow(ctx, sql, args...)
// }

// func (c *Conn) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
// 	return c.Conn().SendBatch(ctx, b)
// }

// func (c *Conn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
// 	return c.Conn().CopyFrom(ctx, tableName, columnNames, rowSrc)
// }

// func (c *Conn) Begin(ctx context.Context) (pgx.Tx, error) {
// 	return c.Conn().Begin(ctx)
// }

// func (c *Conn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
// 	return c.Conn().BeginTx(ctx, txOptions)
// }

func (c *Conn) Conn() *chconn.Conn {
	return c.connResource().conn
}

func (c *Conn) connResource() *connResource {
	return c.res.Value().(*connResource)
}

// func (c *Conn) getPoolRow(r pgx.Row) *poolRow {
// 	return c.connResource().getPoolRow(c, r)
// }

// func (c *Conn) getPoolRows(r pgx.Rows) *poolRows {
// 	return c.connResource().getPoolRows(c, r)
// }
