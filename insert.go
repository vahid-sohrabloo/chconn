package chconn

import (
	"context"
	"fmt"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// InsertStmt is a interface for insert stream statement
type InsertStmt interface {
	// Write writes a columns (a block of data) to the clickhouse server
	// after each write you need to reset the columns. it will not reset automatically
	Write(ctx context.Context, columns ...column.ColumnBasic) error
	// Append appends values of a row to the insert statement.
	Append(values ...any) error
	// Flush flushes the data to the clickhouse server and close the statement
	Flush(ctx context.Context) error
	// Close close the statement and release the connection
	// close will be called automatically after Flush
	Close()
}

type insertStmt struct {
	block        *block
	conn         *conn
	columns      []column.ColumnBasic
	query        string
	queryOptions *QueryOptions
	clientInfo   *ClientInfo
	hasError     bool
	closed       bool
	finishInsert bool
}

func (s *insertStmt) Flush(ctx context.Context) error {
	defer s.Close()
	if s.columns != nil {
		err := s.Write(ctx, s.columns...)
		if err != nil {
			s.hasError = true
			return err
		}

		s.columns = nil
	}

	s.finishInsert = true

	if ctx != context.Background() {
		select {
		case <-ctx.Done():
			return newContextAlreadyDoneError(ctx)
		default:
		}
		s.conn.contextWatcher.Watch(ctx)
		defer s.conn.contextWatcher.Unwatch()
	}

	err := s.conn.sendEmptyBlock()

	if err != nil {
		s.hasError = true
		return &InsertError{
			err:        err,
			remoteAddr: s.conn.RawConn().RemoteAddr(),
		}
	}

	var res any
	for {
		res, err = s.conn.receiveAndProcessData(emptyOnProgress)

		if err != nil {
			s.hasError = true
			return err
		}

		if res == nil {
			return nil
		}

		if profile, ok := res.(*Profile); ok {
			if s.queryOptions.OnProfile != nil {
				s.queryOptions.OnProfile(profile)
			}
			continue
		}
		if progress, ok := res.(*Progress); ok {
			if s.queryOptions.OnProgress != nil {
				s.queryOptions.OnProgress(progress)
			}
			continue
		}
		if profileEvent, ok := res.(*ProfileEvent); ok {
			if s.queryOptions.OnProfileEvent != nil {
				s.queryOptions.OnProfileEvent(profileEvent)
			}
			continue
		}
		s.hasError = true
		return &unexpectedPacket{expected: "serverData", actual: res}
	}
}

// Close close the statement and release the connection
// If Next is called and returns false and there are no further blocks,
// the Rows are closed automatically and it will suffice to check the result of Err.
// Close is idempotent and does not affect the result of Err.
func (s *insertStmt) Close() {
	s.conn.reader.SetCompress(false)
	if !s.closed {
		s.closed = true
		s.conn.contextWatcher.Unwatch()
		s.conn.unlock()
		if s.hasError || !s.finishInsert {
			s.conn.Close()
		}
	}
}

func (s *insertStmt) Write(ctx context.Context, columns ...column.ColumnBasic) error {
	if int(s.block.NumColumns) != len(columns) {
		return &InsertError{
			err: &ColumnNumberWriteError{
				WriteColumn: len(columns),
				NeedColumn:  s.block.NumColumns,
			},
			remoteAddr: s.conn.RawConn().RemoteAddr(),
		}
	}

	var err error
	if len(columns[0].Name()) != 0 {
		columns, err = s.block.reorderColumns(columns)
		if err != nil {
			s.hasError = true
			return &InsertError{
				err:        err,
				remoteAddr: s.conn.RawConn().RemoteAddr(),
			}
		}
	}
	for i, col := range columns {
		col.SetType(s.block.Columns[i].ChType)
		if errValidate := col.Validate(true); errValidate != nil {
			s.hasError = true
			return fmt.Errorf("column at index %d: %w", i, errValidate)
		}
	}

	if ctx != context.Background() {
		select {
		case <-ctx.Done():
			return newContextAlreadyDoneError(ctx)
		default:
		}
		s.conn.contextWatcher.Watch(ctx)
		defer s.conn.contextWatcher.Unwatch()
	}

	err = s.conn.sendData(s.block, columns[0].NumRow())
	if err != nil {
		s.hasError = true
		return &InsertError{
			err:        err,
			remoteAddr: s.conn.RawConn().RemoteAddr(),
		}
	}

	err = s.block.writeColumnsBuffer(columns...)
	if err != nil {
		s.hasError = true
		return &InsertError{
			err:        err,
			remoteAddr: s.conn.RawConn().RemoteAddr(),
		}
	}
	return nil
}

func (s *insertStmt) Append(values ...any) error {
	if s.columns == nil {
		columns, err := s.block.getColumnsByChType()
		if err != nil {
			return fmt.Errorf("could not get columns for insert statement: %w", err)
		}
		s.columns = columns
	}

	if len(values) != len(s.columns) {
		return &InsertError{
			err: &ColumnNumberWriteError{
				WriteColumn: len(values),
				NeedColumn:  s.block.NumColumns,
			},
			remoteAddr: s.conn.RawConn().RemoteAddr(),
		}
	}

	for i, value := range values {
		err := s.columns[i].AppendAny(value)
		if err != nil {
			return fmt.Errorf("could not append value at index %d: %w", i, err)
		}
	}

	return nil
}

// Insert send query for insert and commit columns
func (ch *conn) Insert(ctx context.Context, query string, columns ...column.ColumnBasic) error {
	return ch.InsertWithOption(ctx, query, nil, columns...)
}

// Insert send query for insert and prepare insert stmt with setting option
func (ch *conn) InsertWithOption(
	ctx context.Context,
	query string,
	queryOptions *QueryOptions,
	columns ...column.ColumnBasic) error {
	stmt, err := ch.InsertStreamWithOption(ctx, query, queryOptions)
	if err != nil {
		return err
	}

	if stmt == nil {
		ch.reader.SetCompress(false)
		ch.contextWatcher.Unwatch()
		ch.unlock()
		return nil
	}
	defer stmt.Close()
	err = stmt.Write(ctx, columns...)
	if err != nil {
		return err
	}
	err = stmt.Flush(ctx)
	if err != nil {
		return err
	}
	for _, col := range columns {
		col.Reset()
	}
	return nil
}

func (ch *conn) InsertStream(ctx context.Context, query string) (InsertStmt, error) {
	return ch.InsertStreamWithOption(ctx, query, nil)
}

// Insert send query for insert and prepare insert stmt with setting option
func (ch *conn) InsertStreamWithOption(
	ctx context.Context,
	query string,
	queryOptions *QueryOptions) (InsertStmt, error) {
	err := ch.lock()
	if err != nil {
		return nil, err
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
			return nil, newContextAlreadyDoneError(ctx)
		default:
		}
		ch.contextWatcher.Watch(ctx)
		defer ch.contextWatcher.Unwatch()
	}

	if queryOptions == nil {
		queryOptions = emptyQueryOptions
	}

	err = ch.sendQueryWithOption(query, queryOptions.QueryID, queryOptions.Settings, queryOptions.Parameters)
	if err != nil {
		hasError = true
		return nil, preferContextOverNetTimeoutError(ctx, err)
	}
	var blockData *block
	for {
		var res any
		res, err = ch.receiveAndProcessData(queryOptions.OnProgress)
		if err != nil {
			hasError = true
			return nil, preferContextOverNetTimeoutError(ctx, err)
		}
		if b, ok := res.(*block); ok {
			blockData = b
			break
		}

		if profile, ok := res.(*Profile); ok {
			if queryOptions.OnProfile != nil {
				queryOptions.OnProfile(profile)
			}
			continue
		}
		if progress, ok := res.(*Progress); ok {
			if queryOptions.OnProgress != nil {
				queryOptions.OnProgress(progress)
			}
			continue
		}
		if profileEvent, ok := res.(*ProfileEvent); ok {
			if queryOptions.OnProfileEvent != nil {
				queryOptions.OnProfileEvent(profileEvent)
			}
			continue
		}
		if res == nil {
			return nil, nil
		}
		hasError = true
		return nil, &unexpectedPacket{expected: "serverData", actual: res}
	}

	err = blockData.readColumns()
	if err != nil {
		hasError = true
		return nil, preferContextOverNetTimeoutError(ctx, err)
	}

	s := &insertStmt{
		conn:         ch,
		query:        query,
		block:        blockData,
		queryOptions: queryOptions,
		clientInfo:   nil,
	}

	return s, nil
}
