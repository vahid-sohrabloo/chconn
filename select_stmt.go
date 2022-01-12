package chconn

import (
	"context"

	"github.com/vahid-sohrabloo/chconn/column"
)

// SelectStmt is a interface for select statement
type SelectStmt interface {
	// Next get the next block, if available return true else return false
	// if the server sends an error return false and we can get the last error with Err() function
	Next() bool
	// Err When calls Next() func if server send error we can get error from this function
	Err() error
	// RowsInBlock return number of rows in this current block
	RowsInBlock() int
	// Close after reads all data should call this function to unlock connection
	// NOTE: You shoud read all data and then call this function
	Close()
	// NextColumn get the next column of block
	NextColumn(colData column.Column) error
}
type selectStmt struct {
	block            *block
	conn             *conn
	query            string
	queryID          string
	clientInfo       *ClientInfo
	onProgress       func(*Progress)
	onProfile        func(*Profile)
	lastErr          error
	ProfileInfo      *Profile
	Progress         *Progress
	closed           bool
	numberColumnRead int
}

var _ SelectStmt = &selectStmt{}

// Next get the next block, if available return true else return false
// if the server sends an error return false and we can get the last error with Err() function
func (s *selectStmt) Next() bool {
	if s.lastErr == nil &&
		s.block != nil &&
		s.numberColumnRead != int(s.block.NumColumns) {
		s.lastErr = &ColumnNumberReadError{
			Read:      s.numberColumnRead,
			Available: s.block.NumColumns,
		}
		return false
	}

	s.conn.reader.SetCompress(false)
	res, err := s.conn.receiveAndProccessData(nil)
	if err != nil {
		s.lastErr = err
		return false
	}
	s.conn.reader.SetCompress(s.conn.compress)
	if block, ok := res.(*block); ok {
		if block.NumRows == 0 {
			err = block.readColumns(s.conn)
			if err != nil {
				s.lastErr = err
				return false
			}
			return s.Next()
		}
		s.numberColumnRead = 0
		s.block = block
		return true
	}

	if profile, ok := res.(*Profile); ok {
		s.ProfileInfo = profile
		if s.onProfile != nil {
			s.onProfile(profile)
		}
		return s.Next()
	}
	if progress, ok := res.(*Progress); ok {
		s.Progress = progress
		if s.onProgress != nil {
			s.onProgress(progress)
		}
		return s.Next()
	}

	if res == nil {
		return false
	}

	s.lastErr = &unexpectedPacket{expected: "serverData", actual: res}
	return false
}

// RowsInBlock return number of rows in this current block
func (s *selectStmt) RowsInBlock() int {
	return int(s.block.NumRows)
}

// Err When calls Next() func if server send error we can get error from this function
func (s *selectStmt) Err() error {
	return s.lastErr
}

// Close after reads all data should call this function to unlock connection
// NOTE: You shoud read all data and then call this function
func (s *selectStmt) Close() {
	s.conn.reader.SetCompress(false)
	if !s.closed {
		s.closed = true
		s.conn.unlock()
		if s.Err() != nil {
			s.conn.Close(context.Background())
		}
	}
	s.numberColumnRead = 0
}

// NextColumn get the next column of block
func (s *selectStmt) NextColumn(colData column.Column) error {
	s.numberColumnRead++
	if s.numberColumnRead > int(s.block.NumColumns) {
		return &ColumnNumberReadError{
			Read:      s.numberColumnRead,
			Available: s.block.NumColumns,
		}
	}
	_, err := s.block.nextColumn(s.conn)
	if err != nil {
		s.Close()
		s.conn.Close(context.Background())
		return err
	}
	err = colData.HeaderReader(s.conn.reader)
	if err != nil {
		s.Close()
		s.conn.Close(context.Background())
		return err
	}
	err = colData.ReadRaw(s.RowsInBlock(), s.conn.reader)
	if err != nil {
		s.Close()
		s.conn.Close(context.Background())
	}
	return err
}
