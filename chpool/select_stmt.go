package chpool

import (
	"github.com/vahid-sohrabloo/chconn"
)

type SelectStmt struct {
	*chconn.SelectStmt
	conn *Conn
}

func (s *SelectStmt) Close() {
	s.SelectStmt.Close()
	s.conn.Release()
}
