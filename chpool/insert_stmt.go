package chpool

import (
	"context"

	"github.com/vahid-sohrabloo/chconn"
)

type insertStmt struct {
	chconn.InsertStmt
	conn Conn
}

func (s *insertStmt) Commit(ctx context.Context) error {
	defer s.conn.Release()
	return s.InsertStmt.Commit(ctx)
}

func (s *insertStmt) Flush(ctx context.Context) error {
	err := s.InsertStmt.Flush(ctx)
	if err != nil {
		s.conn.Release()
	}
	return err
}
