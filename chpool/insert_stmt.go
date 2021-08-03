package chpool

import (
	"context"

	"github.com/vahid-sohrabloo/chconn"
)

type insertStmt struct {
	chconn.InsertStmt
	conn Conn
}

func (s *insertStmt) Commit(ctx context.Context, writer *chconn.InsertWriter) error {
	defer s.conn.Release()
	return s.InsertStmt.Commit(ctx, writer)
}
