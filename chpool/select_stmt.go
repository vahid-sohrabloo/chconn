package chpool

import (
	"github.com/vahid-sohrabloo/chconn/v3"
)

type selectStmt struct {
	chconn.SelectStmt
	conn Conn
}

func (s *selectStmt) Next() bool {
	if s.conn == nil {
		return false
	}
	next := s.SelectStmt.Next()
	if s.SelectStmt.Err() != nil && s.conn != nil {
		s.Close()
	}
	if !next && s.conn != nil {
		s.Close()
	}
	return next
}

func (s *selectStmt) Close() {
	if s.conn == nil {
		return
	}
	s.SelectStmt.Close()
	s.conn.Release()
	s.conn = nil
}
