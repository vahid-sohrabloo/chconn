package chconn

import (
	"context"

	"github.com/vahid-sohrabloo/chconn/column"
	"github.com/vahid-sohrabloo/chconn/setting"
)

type InsertStmt interface {
	Commit(ctx context.Context, columns ...column.Column) error
	GetBlock() *Block
}
type insertStmt struct {
	block      *Block
	conn       *conn
	query      string
	queryID    string
	stage      queryProcessingStage
	settings   *setting.Settings
	clientInfo *ClientInfo
}

func (s *insertStmt) commit(columns ...column.Column) error {
	if len(columns) == 0 {
		return ErrInsertMinColumn
	}
	err := s.conn.sendData(s.block, columns[0].NumRow())
	if err != nil {
		return &InsertError{
			err:   err,
			Block: s.block,
		}
	}

	err = s.block.writeColumsBuffer(s.conn, columns...)
	if err != nil {
		return err
	}

	err = s.conn.sendData(newBlock(), 0)

	if err != nil {
		return err
	}

	res, err := s.conn.reciveAndProccessData(emptyOnProgress)
	if err != nil {
		return err
	}

	if res != nil {
		return &unexpectedPacket{expected: "serverEndOfStream", actual: res}
	}

	return nil
}

// Commit all columns to ClickHoouse
func (s *insertStmt) Commit(ctx context.Context, columns ...column.Column) error {
	s.conn.contextWatcher.Watch(ctx)
	defer s.conn.contextWatcher.Unwatch()
	defer s.conn.unlock()
	err := s.commit(columns...)
	if err != nil {
		s.conn.Close(context.Background())
	}
	return err
}

// GetBlock Get current block
func (s *insertStmt) GetBlock() *Block {
	return s.block
}
