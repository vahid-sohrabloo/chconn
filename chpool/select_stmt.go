package chpool

import (
	"github.com/vahid-sohrabloo/chconn"
)

type selectStmt struct {
	chconn.SelectStmt
	conn Conn
}

func (s *selectStmt) Next() bool {
	next := s.SelectStmt.Next()
	if s.SelectStmt.Err() != nil {
		s.conn.Release()
	}
	return next
}

func (s *selectStmt) Close() {
	s.SelectStmt.Close()
	s.conn.Release()
}
