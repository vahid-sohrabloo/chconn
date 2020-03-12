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
	settings   []byte
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

	res, err := s.conn.reciveAndProccessData()
	if err != nil {
		return err
	}

	if res != nil {
		// todo send error
	}

	err = s.conn.SendQueryWithOption(ctx, s.query, "", QueryProcessingStageComplete, nil, nil)
	if err != nil {
		return err
	}
	// todo check response is block and block is the same old

	_, err = s.conn.reciveAndProccessData()

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

	res, err := s.conn.reciveAndProccessData()
	if err != nil {
		return err
	}

	if res != nil {
		// todo send error
	}

	return nil
}

func (s *InsertStmt) Int8(bufferIndex int, value int8) {
	s.Block.ColumnsBuffer[bufferIndex].Int8(value)
}

func (s *InsertStmt) Int16(bufferIndex int, value int16) {
	s.Block.ColumnsBuffer[bufferIndex].Int16(value)
}

func (s *InsertStmt) Int32(bufferIndex int, value int32) {
	s.Block.ColumnsBuffer[bufferIndex].Int32(value)
}

func (s *InsertStmt) Int64(bufferIndex int, value int64) {
	s.Block.ColumnsBuffer[bufferIndex].Int64(value)
}

func (s *InsertStmt) Uint8(bufferIndex int, value uint8) {
	s.Block.ColumnsBuffer[bufferIndex].Uint8(value)
}
func (s *InsertStmt) Uint16(bufferIndex int, value uint16) {
	s.Block.ColumnsBuffer[bufferIndex].Uint16(value)
}

func (s *InsertStmt) Uint32(bufferIndex int, value uint32) {
	s.Block.ColumnsBuffer[bufferIndex].Uint32(value)
}

func (s *InsertStmt) Uint64(bufferIndex int, value uint64) {
	s.Block.ColumnsBuffer[bufferIndex].Uint64(value)
}

func (s *InsertStmt) Float32(bufferIndex int, value float32) {
	s.Block.ColumnsBuffer[bufferIndex].Float32(value)
}

func (s *InsertStmt) Float64(bufferIndex int, value float64) {
	s.Block.ColumnsBuffer[bufferIndex].Float64(value)
}

func (s *InsertStmt) String(bufferIndex int, value string) {
	s.Block.ColumnsBuffer[bufferIndex].String(value)
}

func (s *InsertStmt) Buffer(bufferIndex int, value []byte) {
	s.Block.ColumnsBuffer[bufferIndex].Buffer(value)
}

func (s *InsertStmt) FixedString(bufferIndex int, value []byte) {
	s.Block.ColumnsBuffer[bufferIndex].Write(value)
}

func (s *InsertStmt) Decimal32(bufferIndex int, value float64, scale int) {
	s.Block.ColumnsBuffer[bufferIndex].Int32(int32(value * factors10[scale]))
}

func (s *InsertStmt) Decimal64(bufferIndex int, value float64, scale int) {
	s.Block.ColumnsBuffer[bufferIndex].Int64(int64(value * factors10[scale]))
}

func (s *InsertStmt) Date(bufferIndex int, value time.Time) {
	_, offset := value.Zone()
	timestamp := value.Unix() + int64(offset)
	s.Block.ColumnsBuffer[bufferIndex].Uint16(uint16(timestamp / 24 / 3600))
}

func (s *InsertStmt) DateTime(bufferIndex int, value time.Time) {
	s.Block.ColumnsBuffer[bufferIndex].Uint32(uint32(value.Unix()))
}

func (s *InsertStmt) UUID(bufferIndex int, value [16]byte) {
	s.Block.ColumnsBuffer[bufferIndex].Write(swapUUID(value[:]))
}

func (s *InsertStmt) AddLen(bufferIndex int, value uint64) {
	s.Block.ColumnsBuffer[bufferIndex].AddLen(value)
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
	s.Block.ColumnsBuffer[bufferIndex].Write([]byte{value[3], value[2], value[1], value[0]})
	return nil
}
func (s *InsertStmt) IPv6(bufferIndex int, value net.IP) error {
	if len(value) != 16 {
		return errors.New("invalid ipv6")
	}
	s.Block.ColumnsBuffer[bufferIndex].Write(value)
	return nil
}

func (s *InsertStmt) Int8P(bufferIndex int, value *int8) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Int8(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Int8(*value)
}

func (s *InsertStmt) Int16P(bufferIndex int, value *int16) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Int16(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Int16(*value)
}

func (s *InsertStmt) Int32P(bufferIndex int, value *int32) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Int32(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Int32(*value)
}

func (s *InsertStmt) Int64P(bufferIndex int, value *int64) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Int64(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Int64(*value)
}

func (s *InsertStmt) Uint8P(bufferIndex int, value *uint8) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Uint8(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Uint8(*value)
}
func (s *InsertStmt) Uint16P(bufferIndex int, value *uint16) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Uint16(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Uint16(*value)
}

func (s *InsertStmt) Uint32P(bufferIndex int, value *uint32) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Uint32(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Uint32(*value)
}

func (s *InsertStmt) Uint64P(bufferIndex int, value *uint64) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Uint64(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Uint64(*value)
}

func (s *InsertStmt) Float32P(bufferIndex int, value *float32) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Float32(8)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Float32(*value)
}

func (s *InsertStmt) Float64P(bufferIndex int, value *float64) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Float64(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Float64(*value)
}

func (s *InsertStmt) StringP(bufferIndex int, value *string) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].String("")
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].String(*value)
}

func (s *InsertStmt) BufferP(bufferIndex int, value *[]byte) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Buffer([]byte{})
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Buffer(*value)
}

func (s *InsertStmt) FixedStringP(bufferIndex int, empty, value []byte) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Write(empty)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Write(value)
}

func (s *InsertStmt) Decimal32P(bufferIndex int, value *float64, scale int) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Int32(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Int32(int32(*value * factors10[scale]))
}

func (s *InsertStmt) Decimal64P(bufferIndex int, value *float64, scale int) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Int64(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Int64(int64(*value * factors10[scale]))
}

func (s *InsertStmt) DateP(bufferIndex int, value *time.Time) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Uint16(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	_, offset := value.Zone()
	timestamp := value.Unix() + int64(offset)
	s.Block.ColumnsBuffer[bufferIndex+1].Uint16(uint16(timestamp / 24 / 3600))
}

func (s *InsertStmt) DateTimeP(bufferIndex int, value *time.Time) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Uint32(0)
		return
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Uint32(uint32(value.Unix()))
}

var emptyUUID = make([]byte, 16)

func (s *InsertStmt) UUIDP(bufferIndex int, value *[16]byte) {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Write(emptyUUID[:])
		return
	}
	// copy data to not change main value by swapUUID
	val := *value
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Write(swapUUID(val[:]))
}

var emptyIPV4 = make([]byte, 4)

func (s *InsertStmt) IPv4P(bufferIndex int, value *net.IP) error {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Write(emptyIPV4)
		return nil
	}
	val := *value
	if len(val) != 4 {
		return errors.New("invalid ipv4")
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Write([]byte{val[3], val[2], val[1], val[0]})
	return nil
}

var emptyIPV6 = make([]byte, 16)

func (s *InsertStmt) IPv6P(bufferIndex int, value *net.IP) error {
	if value == nil {
		s.Block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.Block.ColumnsBuffer[bufferIndex+1].Write(emptyIPV6)
		return nil
	}
	if len(*value) != 16 {
		return errors.New("invalid ipv6")
	}
	s.Block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.Block.ColumnsBuffer[bufferIndex+1].Write(*value)
	return nil
}
