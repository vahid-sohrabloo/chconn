package chconn

type SelectStmt struct {
	Block       *Block
	conn        *Conn
	query       string
	queryID     string
	stage       QueryProcessingStage
	settings    *Setting
	clientInfo  *ClientInfo
	LastErr     error
	ProfileInfo *Profile
	Progress    *Progress
	nulls       []uint8
}

func (s *SelectStmt) Next() bool {
	res, err := s.conn.ReciveAndProccessData()
	if err != nil {
		// todo wrap this error
		s.LastErr = err
		return false
	}

	if block, ok := res.(*Block); ok {
		if block.NumRows == 0 {
			err = block.readColumns(s.conn)
			if err != nil {
				s.LastErr = err
				return false
			}
			return s.Next()
		}
		s.Block = block
		return true
	}

	if profile, ok := res.(*Profile); ok {
		s.ProfileInfo = profile
		return s.Next()
	}
	if progress, ok := res.(*Progress); ok {
		s.Progress = progress
		return s.Next()
	}
	if _, ok := res.(ServerInfo); ok {
		return s.Next()
	}
	if res == nil {
		return false
	}

	return false
}

func (s *SelectStmt) Close() {
	s.conn.unlock()
}

func (s *SelectStmt) NextColumn() (*Column, error) {
	return s.Block.NextColumn(s.conn)
}
