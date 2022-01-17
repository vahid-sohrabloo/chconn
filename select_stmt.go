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
	// Deprecated: use ReadColumns instead
	NextColumn(colData column.Column) error
	// ReadColumns read all columns of block
	ReadColumns(columns ...column.Column) error
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
	readAll          bool
}

var _ SelectStmt = &selectStmt{}

// Next get the next block, if available return true else return false
// if the server sends an error return false and we can get the last error with Err() function
func (s *selectStmt) Next() bool {
	if s.lastErr == nil &&
		s.block != nil &&
		s.numberColumnRead < int(s.block.NumColumns) {
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
		s.readAll = true
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
// NOTE: You should read all blocks and then call this function
func (s *selectStmt) Close() {
	s.conn.reader.SetCompress(false)
	if !s.closed {
		s.closed = true
		s.conn.unlock()
		if s.Err() != nil || !s.readAll {
			s.conn.Close(context.Background())
		}
	}
	s.numberColumnRead = 0
	s.readAll = false
}

// Deprecated: use ReadColumns instead
func (s *selectStmt) NextColumn(colData column.Column) error {
	s.numberColumnRead++
	if s.numberColumnRead > int(s.block.NumColumns) {
		err := &ColumnNumberReadError{
			Read:      s.numberColumnRead,
			Available: s.block.NumColumns,
		}
		s.Close()
		s.conn.Close(context.Background())
		return err
	}
	err := colData.HeaderReader(s.conn.reader)
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

// ReadColumns read all columns of block
func (s *selectStmt) ReadColumns(columns ...column.Column) error {
	if int(s.block.NumColumns) != len(columns) {
		s.Close()
		s.conn.Close(context.Background())
		return &ColumnNumberReadError{
			Read:      len(columns),
			Available: s.block.NumColumns,
		}
	}
	s.numberColumnRead = int(s.block.NumColumns)
	// todo: validate number of bytes

	for _, col := range columns {
		err := col.HeaderReader(s.conn.reader)
		if err != nil {
			s.Close()
			s.conn.Close(context.Background())
			return err
		}
		err = col.ReadRaw(s.RowsInBlock(), s.conn.reader)
		if err != nil {
			s.Close()
			s.conn.Close(context.Background())
			return err
		}
	}
	return nil
}
