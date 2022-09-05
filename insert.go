package chconn

import (
	"context"

	"github.com/vahid-sohrabloo/chconn/v2/column"
)

func (ch *conn) commit(queryOptions *QueryOptions, b *block, columns ...column.ColumnBasic) error {
	if int(b.NumColumns) != len(columns) {
		return &InsertError{
			err: &ColumnNumberWriteError{
				WriteColumn: len(columns),
				NeedColumn:  b.NumColumns,
			},
			remoteAddr: ch.RawConn().RemoteAddr(),
		}
	}

	var err error
	if len(columns[0].Name()) != 0 {
		columns, err = b.reorderColumns(columns)
		if err != nil {
			return &InsertError{
				err:        err,
				remoteAddr: ch.RawConn().RemoteAddr(),
			}
		}
	}
	for i, col := range columns {
		col.SetType(b.Columns[i].ChType)
		if errValidate := col.Validate(); errValidate != nil {
			return errValidate
		}
	}
	err = ch.sendData(b, columns[0].NumRow())
	if err != nil {
		return &InsertError{
			err:        err,
			remoteAddr: ch.RawConn().RemoteAddr(),
		}
	}

	err = b.writeColumnsBuffer(ch, columns...)
	if err != nil {
		return &InsertError{
			err:        err,
			remoteAddr: ch.RawConn().RemoteAddr(),
		}
	}

	err = ch.sendEmptyBlock()

	if err != nil {
		return &InsertError{
			err:        err,
			remoteAddr: ch.RawConn().RemoteAddr(),
		}
	}

	for {
		var res interface{}
		res, err = ch.receiveAndProcessData(emptyOnProgress)

		if err != nil {
			return err
		}

		if res == nil {
			return nil
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
		return &unexpectedPacket{expected: "serverData", actual: res}
	}
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
	err := ch.lock()
	if err != nil {
		return err
	}
	var hasError bool
	defer func() {
		ch.unlock()
		if hasError {
			ch.Close()
		}
	}()

	if ctx != context.Background() {
		select {
		case <-ctx.Done():
			return newContextAlreadyDoneError(ctx)
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
		return preferContextOverNetTimeoutError(ctx, err)
	}

	var blockData *block
	for {
		var res interface{}
		res, err = ch.receiveAndProcessData(emptyOnProgress)
		if err != nil {
			hasError = true
			return preferContextOverNetTimeoutError(ctx, err)
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
		hasError = true
		return &unexpectedPacket{expected: "serverData", actual: res}
	}

	err = blockData.readColumns(ch)
	if err != nil {
		hasError = true
		return preferContextOverNetTimeoutError(ctx, err)
	}

	err = ch.commit(queryOptions, blockData, columns...)
	if err != nil {
		hasError = true
		return preferContextOverNetTimeoutError(ctx, err)
	}
	for _, column := range columns {
		column.Reset()
	}
	return nil
}
