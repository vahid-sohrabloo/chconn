package chpool

import (
	"iter"

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
	if s.Err() != nil && s.conn != nil {
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

func (s *selectStmt) Iter() iter.Seq2[int, error] {
	return func(yield func(int, error) bool) {
		defer s.Close()
		for s.Next() {
			if !yield(s.RowsInBlock(), nil) {
				return
			}
		}
		if s.Err() != nil {
			yield(0, s.Err())
		}
	}
}

func (s *selectStmt) RowIter() iter.Seq2[int, error] {
	return func(yield func(int, error) bool) {
		defer s.Close()
		for s.Next() {
			for i := range s.RowsInBlock() {
				if !yield(i, nil) {
					return
				}
			}
		}
		if s.Err() != nil {
			yield(0, s.Err())
		}
	}
}
