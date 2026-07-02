package chconn

import (
	"context"
	"iter"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// Select executes a query and return select stmt.
//
// NOTE: only use for select query
//
// For queries that return no block (e.g. SYSTEM commands, DDL), the returned
// SelectStmt is already closed and Next() will return false immediately.
func (ch *conn) Select(ctx context.Context, query string, columns ...column.ColumnCore) (SelectStmt, error) {
	return ch.SelectWithOption(ctx, query, nil, columns...)
}

// Select executes a query with the the query options and return select stmt.
//
// NOTE: only use for select query
//
// For queries that return no block (e.g. SYSTEM commands, DDL), the returned
// SelectStmt is already closed and Next() will return false immediately.
func (ch *conn) SelectWithOption(
	ctx context.Context,
	query string,
	queryOptions *QueryOptions,
	columns ...column.ColumnCore,
) (SelectStmt, error) {
	if queryOptions == nil {
		queryOptions = emptyQueryOptions
	}
	s := &selectStmt{
		conn:           ch,
		query:          query,
		queryOptions:   queryOptions,
		clientInfo:     nil,
		ctx:            ctx,
		columnsForRead: columns,
	}

	err := ch.lock()
	if err != nil {
		s.lastErr = err
		return s, s.lastErr
	}

	var hasError bool
	defer func() {
		if hasError {
			ch.Close()
		}
	}()

	if ctx != context.Background() {
		select {
		case <-ctx.Done():
			s.lastErr = newContextAlreadyDoneError(ctx)
			return s, s.lastErr
		default:
		}
		ch.contextWatcher.Watch(ctx)
	}

	err = ch.sendQueryWithOption(query, queryOptions.QueryID, queryOptions.Settings, queryOptions.Parameters)
	if err != nil {
		hasError = true
		s.lastErr = preferContextOverNetTimeoutError(ctx, err)
		return s, s.lastErr
	}
	res, err := s.conn.receiveAndProcessData(s.queryOptions)
	if err != nil {
		s.lastErr = err
		s.Close()
		return s, s.lastErr
	}
	if res == nil {
		s.finishSelect = true
		s.columnsForRead = nil
		s.Close()
		return s, nil
	}
	if block, ok := res.(*block); ok {
		if block.NumRows == 0 {
			err = s.skipBlock(block)
			if err != nil {
				s.lastErr = err
				s.Close()
				return s, s.lastErr
			}
			return s, nil
		}
	}
	s.lastErr = &unexpectedPacket{expected: "serverData with zero len", actual: res}
	return s, s.lastErr
}

// SelectStmt is a interface for select statement
type SelectStmt interface {
	// Next read the next block of data for reading.
	// It returns true on success, or false if there is no next result row or an error happened while preparing it.
	// Err should be consulted to distinguish between the two cases.
	Next() bool
	// Err returns the error, if any, that was encountered during iteration.
	// Err may be called after an explicit or implicit Close.
	Err() error
	// RowsInBlock return number of rows in this current block
	RowsInBlock() int
	// Columns return the columns of this select statement.
	Columns() []column.ColumnCore
	// Close close the statement and release the connection
	// If Next is called and returns false and there are no further blocks,
	// the Rows are closed automatically and it will suffice to check the result of Err.
	// Close is idempotent and does not affect the result of Err.
	Close()
	// Rows return the rows of this select statement.
	Rows() Rows
	// Iter returns a block-level iterator. Each iteration yields the number of rows
	// in the current block. The statement is closed when iteration completes or breaks.
	Iter() iter.Seq2[int, error]
	// RowIter returns a row-level iterator that flattens block boundaries.
	// Each iteration yields the row index within the current block (matching col.Row(i)).
	// The statement is closed when iteration completes or breaks.
	RowIter() iter.Seq2[int, error]
}

type selectStmt struct {
	block          *block
	conn           *conn
	query          string
	queryOptions   *QueryOptions
	clientInfo     *ClientInfo
	lastErr        error
	closed         bool
	columnsForRead []column.ColumnCore
	ctx            context.Context
	finishSelect   bool
	validateData   bool
}

var _ SelectStmt = &selectStmt{}

func (s *selectStmt) skipBlock(b *block) error {
	err := b.readColumnsHeader()
	if err != nil {
		s.lastErr = err
		s.Close()
		return err
	}
	// TODO check for dynamic columns
	if len(s.columnsForRead) == 0 {
		s.columnsForRead, err = b.getColumnsByChType()
		if err != nil {
			s.lastErr = err
			s.Close()
			return err
		}
	} else if len(s.columnsForRead[0].Name()) != 0 {
		s.columnsForRead, err = b.reorderColumns(s.columnsForRead)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *selectStmt) Next() bool {
	// protect after close
	if s.closed {
		return false
	}
	s.conn.reader.SetCompress(false)
	res, err := s.conn.receiveAndProcessData(s.queryOptions)
	if err != nil {
		s.lastErr = err
		s.Close()
		return false
	}

	if block, ok := res.(*block); ok {
		if block.NumRows == 0 {
			err = s.skipBlock(block)
			if err != nil {
				return false
			}
			return s.Next()
		}
		s.block = block

		needValidateData := !s.validateData
		s.validateData = false
		if needValidateData {
			if errValidate := s.validate(); errValidate != nil {
				s.lastErr = errValidate
				s.Close()
				return false
			}
		}
		err = block.readColumnsData(needValidateData, s.columnsForRead...)
		if err != nil {
			s.lastErr = preferContextOverNetTimeoutError(s.ctx, err)
			s.Close()
			return false
		}
		return true
	}

	if res == nil {
		s.finishSelect = true
		s.columnsForRead = nil
		s.Close()
		return false
	}

	s.lastErr = &unexpectedPacket{expected: "serverData", actual: res}
	s.Close()
	return false
}

func (s *selectStmt) validate() error {
	if int(s.block.NumColumns) != len(s.columnsForRead) {
		return &ColumnNumberReadError{
			Read:      len(s.columnsForRead),
			Available: s.block.NumColumns,
		}
	}
	return nil
}

// RowsInBlock return number of rows in this current block
func (s *selectStmt) RowsInBlock() int {
	return int(s.block.NumRows)
}

// Err returns the error, if any, that was encountered during iteration.
// Err may be called after an explicit or implicit Close.
func (s *selectStmt) Err() error {
	return preferContextOverNetTimeoutError(s.ctx, s.lastErr)
}

// Close close the statement and release the connection
// If Next is called and returns false and there are no further blocks,
// the Select are closed automatically and it will suffice to check the result of Err.
// Close is idempotent and does not affect the result of Err.
func (s *selectStmt) Close() {
	s.conn.reader.SetCompress(false)
	if !s.closed {
		s.closed = true
		s.conn.contextWatcher.Unwatch()
		s.conn.unlock()
		if s.Err() != nil || !s.finishSelect {
			s.conn.Close()
		}
	}
}

func (s *selectStmt) Columns() []column.ColumnCore {
	return s.columnsForRead
}

func (s *selectStmt) Rows() Rows {
	return &baseRows{
		selectStmt: s,
	}
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
