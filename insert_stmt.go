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

type InsertStmt interface {
	Flush(ctx context.Context) error
	Commit(ctx context.Context) error
	Int8(bufferIndex int, value int8)
	Int16(bufferIndex int, value int16)
	Int32(bufferIndex int, value int32)
	Int64(bufferIndex int, value int64)
	Uint8(bufferIndex int, value uint8)
	Uint16(bufferIndex int, value uint16)
	Uint32(bufferIndex int, value uint32)
	Uint64(bufferIndex int, value uint64)
	Float32(bufferIndex int, value float32)
	Float64(bufferIndex int, value float64)
	String(bufferIndex int, value string)
	Buffer(bufferIndex int, value []byte)
	FixedString(bufferIndex int, value []byte)
	Decimal32(bufferIndex int, value float64, scale int)
	Decimal64(bufferIndex int, value float64, scale int)
	Date(bufferIndex int, value time.Time)
	DateTime(bufferIndex int, value time.Time)
	UUID(bufferIndex int, value [16]byte)
	AddLen(bufferIndex int, value uint64)
	IPv4(bufferIndex int, value net.IP) error
	IPv6(bufferIndex int, value net.IP) error
	Int8P(bufferIndex int, value *int8)
	Int16P(bufferIndex int, value *int16)
	Int32P(bufferIndex int, value *int32)
	Int64P(bufferIndex int, value *int64)
	Uint8P(bufferIndex int, value *uint8)
	Uint16P(bufferIndex int, value *uint16)
	Uint32P(bufferIndex int, value *uint32)
	Uint64P(bufferIndex int, value *uint64)
	Float32P(bufferIndex int, value *float32)
	Float64P(bufferIndex int, value *float64)
	StringP(bufferIndex int, value *string)
	BufferP(bufferIndex int, value *[]byte)
	FixedStringP(bufferIndex int, empty, value []byte)
	Decimal32P(bufferIndex int, value *float64, scale int)
	Decimal64P(bufferIndex int, value *float64, scale int)
	DateP(bufferIndex int, value *time.Time)
	DateTimeP(bufferIndex int, value *time.Time)
	UUIDP(bufferIndex int, value *[16]byte)
	IPv4P(bufferIndex int, value *net.IP) error
	IPv6P(bufferIndex int, value *net.IP) error
	AddRow(num uint64)
}
type insertStmt struct {
	block      *block
	conn       *conn
	query      string
	queryID    string
	stage      QueryProcessingStage
	settings   []byte
	clientInfo *ClientInfo
}

func (s *insertStmt) commit() error {
	err := s.conn.sendData(s.block)
	if err != nil {
		return err
	}

	err = s.conn.sendData(newBlock())

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

func (s *insertStmt) Flush(ctx context.Context) error {
	s.conn.contextWatcher.Watch(ctx)
	defer s.conn.contextWatcher.Unwatch()
	err := s.commit()
	if err != nil {
		return err
	}

	err = s.conn.sendQueryWithOption(ctx, s.query, "")
	if err != nil {
		return err
	}

	// todo check response block is the same old
	res, err := s.conn.reciveAndProccessData(emptyOnProgress)
	if err != nil {
		return err
	}
	if _, ok := res.(*block); !ok {
		return &unexpectedPacket{expected: "serverData", actual: res}
	}

	for _, buf := range s.block.ColumnsBuffer {
		buf.Reset()
	}

	for _, column := range s.block.Columns {
		_, err = res.(*block).nextColumn(s.conn)
		if err != nil {
			return err
		}
		// write header
		s.block.ColumnsBuffer[column.BufferIndex].String(column.Name)
		s.block.ColumnsBuffer[column.BufferIndex].String(column.ChType)
	}

	return nil
}

func (s *insertStmt) Commit(ctx context.Context) error {
	s.conn.contextWatcher.Watch(ctx)
	defer s.conn.contextWatcher.Unwatch()
	defer s.conn.unlock()
	return s.commit()
}

func (s *insertStmt) Int8(bufferIndex int, value int8) {
	s.block.ColumnsBuffer[bufferIndex].Int8(value)
}

func (s *insertStmt) Int16(bufferIndex int, value int16) {
	s.block.ColumnsBuffer[bufferIndex].Int16(value)
}

func (s *insertStmt) Int32(bufferIndex int, value int32) {
	s.block.ColumnsBuffer[bufferIndex].Int32(value)
}

func (s *insertStmt) Int64(bufferIndex int, value int64) {
	s.block.ColumnsBuffer[bufferIndex].Int64(value)
}

func (s *insertStmt) Uint8(bufferIndex int, value uint8) {
	s.block.ColumnsBuffer[bufferIndex].Uint8(value)
}
func (s *insertStmt) Uint16(bufferIndex int, value uint16) {
	s.block.ColumnsBuffer[bufferIndex].Uint16(value)
}

func (s *insertStmt) Uint32(bufferIndex int, value uint32) {
	s.block.ColumnsBuffer[bufferIndex].Uint32(value)
}

func (s *insertStmt) Uint64(bufferIndex int, value uint64) {
	s.block.ColumnsBuffer[bufferIndex].Uint64(value)
}

func (s *insertStmt) Float32(bufferIndex int, value float32) {
	s.block.ColumnsBuffer[bufferIndex].Float32(value)
}

func (s *insertStmt) Float64(bufferIndex int, value float64) {
	s.block.ColumnsBuffer[bufferIndex].Float64(value)
}

func (s *insertStmt) String(bufferIndex int, value string) {
	s.block.ColumnsBuffer[bufferIndex].String(value)
}

func (s *insertStmt) Buffer(bufferIndex int, value []byte) {
	s.block.ColumnsBuffer[bufferIndex].Buffer(value)
}

func (s *insertStmt) FixedString(bufferIndex int, value []byte) {
	s.block.ColumnsBuffer[bufferIndex].Write(value)
}

func (s *insertStmt) Decimal32(bufferIndex int, value float64, scale int) {
	s.block.ColumnsBuffer[bufferIndex].Int32(int32(value * factors10[scale]))
}

func (s *insertStmt) Decimal64(bufferIndex int, value float64, scale int) {
	s.block.ColumnsBuffer[bufferIndex].Int64(int64(value * factors10[scale]))
}

func (s *insertStmt) Date(bufferIndex int, value time.Time) {
	_, offset := value.Zone()
	timestamp := value.Unix() + int64(offset)
	s.block.ColumnsBuffer[bufferIndex].Uint16(uint16(timestamp / 24 / 3600))
}

func (s *insertStmt) DateTime(bufferIndex int, value time.Time) {

	s.block.ColumnsBuffer[bufferIndex].Uint32(uint32(value.Unix()))
}

func (s *insertStmt) UUID(bufferIndex int, value [16]byte) {
	s.block.ColumnsBuffer[bufferIndex].Write(swapUUID(value[:]))
}

func (s *insertStmt) AddLen(bufferIndex int, value uint64) {
	s.block.ColumnsBuffer[bufferIndex].AddLen(value)
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

func (s *insertStmt) IPv4(bufferIndex int, value net.IP) error {
	if len(value) != 4 {
		return errors.New("invalid ipv4")
	}
	s.block.ColumnsBuffer[bufferIndex].Write([]byte{value[3], value[2], value[1], value[0]})
	return nil
}
func (s *insertStmt) IPv6(bufferIndex int, value net.IP) error {
	if len(value) != 16 {
		return errors.New("invalid ipv6")
	}
	s.block.ColumnsBuffer[bufferIndex].Write(value)
	return nil
}

func (s *insertStmt) Int8P(bufferIndex int, value *int8) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Int8(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Int8(*value)
}

func (s *insertStmt) Int16P(bufferIndex int, value *int16) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Int16(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Int16(*value)
}

func (s *insertStmt) Int32P(bufferIndex int, value *int32) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Int32(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Int32(*value)
}

func (s *insertStmt) Int64P(bufferIndex int, value *int64) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Int64(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Int64(*value)
}

func (s *insertStmt) Uint8P(bufferIndex int, value *uint8) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Uint8(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Uint8(*value)
}
func (s *insertStmt) Uint16P(bufferIndex int, value *uint16) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Uint16(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Uint16(*value)
}

func (s *insertStmt) Uint32P(bufferIndex int, value *uint32) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Uint32(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Uint32(*value)
}

func (s *insertStmt) Uint64P(bufferIndex int, value *uint64) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Uint64(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Uint64(*value)
}

func (s *insertStmt) Float32P(bufferIndex int, value *float32) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Float32(8)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Float32(*value)
}

func (s *insertStmt) Float64P(bufferIndex int, value *float64) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Float64(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Float64(*value)
}

func (s *insertStmt) StringP(bufferIndex int, value *string) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].String("")
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].String(*value)
}

func (s *insertStmt) BufferP(bufferIndex int, value *[]byte) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Buffer([]byte{})
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Buffer(*value)
}

func (s *insertStmt) FixedStringP(bufferIndex int, empty, value []byte) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Write(empty)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Write(value)
}

func (s *insertStmt) Decimal32P(bufferIndex int, value *float64, scale int) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Int32(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Int32(int32(*value * factors10[scale]))
}

func (s *insertStmt) Decimal64P(bufferIndex int, value *float64, scale int) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Int64(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Int64(int64(*value * factors10[scale]))
}

func (s *insertStmt) DateP(bufferIndex int, value *time.Time) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Uint16(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	_, offset := value.Zone()
	timestamp := value.Unix() + int64(offset)
	s.block.ColumnsBuffer[bufferIndex+1].Uint16(uint16(timestamp / 24 / 3600))
}

func (s *insertStmt) DateTimeP(bufferIndex int, value *time.Time) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Uint32(0)
		return
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Uint32(uint32(value.Unix()))
}

var emptyUUID = make([]byte, 16)

func (s *insertStmt) UUIDP(bufferIndex int, value *[16]byte) {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Write(emptyUUID)
		return
	}
	// copy data to not change main value by swapUUID
	val := *value
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Write(swapUUID(val[:]))
}

var emptyIPV4 = make([]byte, 4)

func (s *insertStmt) IPv4P(bufferIndex int, value *net.IP) error {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Write(emptyIPV4)
		return nil
	}
	val := *value
	if len(val) != 4 {
		return errors.New("invalid ipv4")
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Write([]byte{val[3], val[2], val[1], val[0]})
	return nil
}

var emptyIPV6 = make([]byte, 16)

func (s *insertStmt) IPv6P(bufferIndex int, value *net.IP) error {
	if value == nil {
		s.block.ColumnsBuffer[bufferIndex].Uint8(1)
		s.block.ColumnsBuffer[bufferIndex+1].Write(emptyIPV6)
		return nil
	}
	if len(*value) != 16 {
		return errors.New("invalid ipv6")
	}
	s.block.ColumnsBuffer[bufferIndex].Uint8(0)
	s.block.ColumnsBuffer[bufferIndex+1].Write(*value)
	return nil
}

func (s *insertStmt) AddRow(num uint64) {
	s.block.NumRows += num
}
