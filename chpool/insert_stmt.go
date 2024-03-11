package chpool

import (
	"context"

	"github.com/vahid-sohrabloo/chconn/v3"
)

type insertStmt struct {
	chconn.InsertStmt
	conn Conn
}

func (s *insertStmt) Flush(ctx context.Context) error {
	if s.conn == nil {
		return nil
	}
	defer s.Close()
	return s.InsertStmt.Flush(ctx)
}

func (s *insertStmt) Close() {
	if s.conn == nil {
		return
	}
	s.InsertStmt.Close()
	s.conn.Release()
}
