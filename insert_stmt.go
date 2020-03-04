package chconn

import (
	"context"
	"net"
	"time"

	errors "golang.org/x/xerrors"
)

// Table of powers of 10 for fast casting from floating types to decimal type
// representations.
var factors10 = []float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13,
	1e14, 1e15, 1e16, 1e17, 1e18,
}

type InsertStmt struct {
	Block      *Block
	conn       *Conn
	query      string
	queryID    string
	stage      QueryProcessingStage
	settings   *Setting
	clientInfo *ClientInfo
}

func (s *InsertStmt) Flush(ctx context.Context) error {
	err := s.conn.SendData(s.Block, "")
	if err != nil {
		return err
	}

	err = s.conn.SendData(NewBlock(), "")

	if err != nil {
		return err
	}

	res, err := s.conn.ReciveAndProccessData()
	if err != nil {
		return err
	}

	if res != nil {
		// todo send error
	}

	_, err = s.conn.Exec(ctx, s.query)
	if err != nil {
		return err
	}

	// todo check response is block and block is the same old

	for range s.Block.Columns {

		if _, err = s.conn.reader.String(); err != nil {
			return err
		}
		if _, err = s.conn.reader.String(); err != nil {
			return err
		}
	}
	for _, buf := range s.Block.ColumnsBuffer {
		buf.Reset()
	}

	return nil

}

func (s *InsertStmt) Commit(ctx context.Context) error {
	defer s.conn.unlock()
	err := s.conn.SendData(s.Block, "")
	if err != nil {
		return err
	}

	err = s.conn.SendData(NewBlock(), "")

	if err != nil {
		return err
	}

	res, err := s.conn.ReciveAndProccessData()
	if err != nil {
		return err
	}

	if res != nil {
		// todo send error
	}

	return nil

}

func (s *InsertStmt) Int8(bufferIndex int, value int8) error {
	return s.Block.ColumnsBuffer[bufferIndex].Int8(value)
}

func (s *InsertStmt) Int16(bufferIndex int, value int16) error {
	return s.Block.ColumnsBuffer[bufferIndex].Int16(value)
}

func (s *InsertStmt) Int32(bufferIndex int, value int32) error {
	return s.Block.ColumnsBuffer[bufferIndex].Int32(value)
}

func (s *InsertStmt) Int64(bufferIndex int, value int64) error {
	return s.Block.ColumnsBuffer[bufferIndex].Int64(value)
}

func (s *InsertStmt) Uint8(bufferIndex int, value uint8) error {
	return s.Block.ColumnsBuffer[bufferIndex].Uint8(value)
}
func (s *InsertStmt) Uint16(bufferIndex int, value uint16) error {
	return s.Block.ColumnsBuffer[bufferIndex].Uint16(value)
}

func (s *InsertStmt) Uint32(bufferIndex int, value uint32) error {
	return s.Block.ColumnsBuffer[bufferIndex].Uint32(value)
}

func (s *InsertStmt) Uint64(bufferIndex int, value uint64) error {
	return s.Block.ColumnsBuffer[bufferIndex].Uint64(value)
}

func (s *InsertStmt) Float32(bufferIndex int, value float32) error {
	return s.Block.ColumnsBuffer[bufferIndex].Float32(value)
}

func (s *InsertStmt) Float64(bufferIndex int, value float64) error {
	return s.Block.ColumnsBuffer[bufferIndex].Float64(value)
}

func (s *InsertStmt) String(bufferIndex int, value string) error {
	return s.Block.ColumnsBuffer[bufferIndex].String(value)
}

func (s *InsertStmt) Buffer(bufferIndex int, value []byte) error {
	return s.Block.ColumnsBuffer[bufferIndex].Buffer(value)
}

func (s *InsertStmt) FixedString(bufferIndex int, value []byte) error {
	return s.Block.ColumnsBuffer[bufferIndex].Write(value)
}

func (s *InsertStmt) Decimal32(bufferIndex int, value float64, scale int) error {
	return s.Block.ColumnsBuffer[bufferIndex].Int32(int32(value * factors10[scale]))
}

func (s *InsertStmt) Decimal64(bufferIndex int, value float64, scale int) error {
	return s.Block.ColumnsBuffer[bufferIndex].Int64(int64(value * factors10[scale]))
}

func (s *InsertStmt) Date(bufferIndex int, value time.Time) error {
	_, offset := value.Zone()
	timestamp := value.Unix() + int64(offset)
	return s.Block.ColumnsBuffer[bufferIndex].Uint16(uint16(timestamp / 24 / 3600))
}

func (s *InsertStmt) DateTime(bufferIndex int, value time.Time) error {
	return s.Block.ColumnsBuffer[bufferIndex].Uint32(uint32(value.Unix()))
}

func (s *InsertStmt) UUID(bufferIndex int, value [16]byte) error {
	return s.Block.ColumnsBuffer[bufferIndex].Write(swapUUID(value[:]))
}

func (s *InsertStmt) AddOffset(bufferIndex int, value uint64) error {
	return s.Block.ColumnsBuffer[bufferIndex].AddOffset(value)
}

func swapUUID(src []byte) []byte {
	_ = src[15]
	src[0], src[7] = src[7], src[0]
	src[1], src[6] = src[6], src[1]
	src[2], src[5] = src[5], src[2]
	src[3], src[4] = src[4], src[3]
	src[8], src[15] = src[15], src[8]
	src[9], src[14] = src[14], src[9]
	src[10], src[13] = src[13], src[10]
	src[11], src[12] = src[12], src[11]
	return src
}

func (s *InsertStmt) IPv4(bufferIndex int, value net.IP) error {
	if len(value) != 4 {
		return errors.New("invalid ipv4")
	}
	return s.Block.ColumnsBuffer[bufferIndex].Write([]byte{value[3], value[2], value[1], value[0]})
}
func (s *InsertStmt) IPv6(bufferIndex int, value net.IP) error {
	if len(value) != 16 {
		return errors.New("invalid ipv6")
	}
	return s.Block.ColumnsBuffer[bufferIndex].Write(value)
}
