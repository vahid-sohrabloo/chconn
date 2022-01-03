package chpool

import (
	"context"

	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
)

type insertStmt struct {
	chconn.InsertStmt
	conn Conn
}

func (s *insertStmt) Commit(ctx context.Context, columns ...column.Column) error {
	defer s.conn.Release()
	return s.InsertStmt.Commit(ctx, columns...)
}
