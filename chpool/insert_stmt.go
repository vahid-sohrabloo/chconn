package chpool

import (
	"context"

	"github.com/vahid-sohrabloo/chconn"
)

type InsertStmt struct {
	*chconn.InsertStmt
	conn *Conn
}

func (s *InsertStmt) Commit(ctx context.Context) error {
	defer s.conn.Release()
	return s.InsertStmt.Commit(ctx)
}
