package chconn

import (
	"context"
	"net"
	"time"
)

// Table of powers of 10 for fast casting from floating types to decimal type
// representations.
var factors10 = []float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13,
	1e14, 1e15, 1e16, 1e17, 1e18,
}

type InsertStmt interface {
	Commit(ctx context.Context, writer *InsertWriter) error
	GetBlock() *block
	Writer() *InsertWriter
	NumBuffer() int
}
type insertStmt struct {
	block      *block
	conn       *conn
	query      string
	queryID    string
	stage      QueryProcessingStage
	settings   *Settings
	clientInfo *ClientInfo
}

func (s *insertStmt) commit(writer *InsertWriter) error {
	err := s.conn.sendData(s.block, writer.NumRows)
	if err != nil {
		return &InsertError{
			err:   err,
			Block: s.block,
		}
	}

	err = s.block.writeColumsBuffer(s.conn.writerto, writer)
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

func (s *insertStmt) Commit(ctx context.Context, writer *InsertWriter) error {
	s.conn.contextWatcher.Watch(ctx)
	defer s.conn.contextWatcher.Unwatch()
	defer s.conn.unlock()
	return s.commit(writer)
}

func (s *insertStmt) GetBlock() *block {
	return s.block
}

// Writer Get new writer
// if you know the number of buffer and number of columms you can use NewInsertWriter
func (s *insertStmt) Writer() *InsertWriter {
	return NewInsertWriter(s.NumBuffer())
}

// NumColumn() uint64
// 	NumBuffer() uint64
func (s *insertStmt) NumColumn() uint64 {
	return s.block.NumColumns
}

// NumColumn() uint64
// 	NumBuffer() uint64
func (s *insertStmt) NumBuffer() int {
	return int(s.block.NumBuffer)
}

type InsertWriter struct {
	NumRows       uint64
	ColumnsBuffer []*Writer
}

func NewInsertWriter(numBuffer int) *InsertWriter {
	columnsBuffer := make([]*Writer, numBuffer)
	for i := 0; i < numBuffer; i++ {
		columnsBuffer[i] = NewWriter()
	}
	return &InsertWriter{
		ColumnsBuffer: columnsBuffer,
	}
}

func (s *InsertWriter) Int8(bufferIndex int, value int8) {
	s.ColumnsBuffer[bufferIndex].Int8(value)
}

func (s *InsertWriter) Int16(bufferIndex int, value int16) {
	s.ColumnsBuffer[bufferIndex].Int16(value)
}

func (s *InsertWriter) Int32(bufferIndex int, value int32) {
	s.ColumnsBuffer[bufferIndex].Int32(value)
}

func (s *InsertWriter) Int64(bufferIndex int, value int64) {
	s.ColumnsBuffer[bufferIndex].Int64(value)
}

func (s *InsertWriter) Uint8(bufferIndex int, value uint8) {
	s.ColumnsBuffer[bufferIndex].Uint8(value)
}
func (s *InsertWriter) Uint16(bufferIndex int, value uint16) {
	s.ColumnsBuffer[bufferIndex].Uint16(value)
}

func (s *InsertWriter) Uint32(bufferIndex int, value uint32) {
	s.ColumnsBuffer[bufferIndex].Uint32(value)
}

func (s *InsertWriter) Uint64(bufferIndex int, value uint64) {
	s.ColumnsBuffer[bufferIndex].Uint64(value)
}

func (s *InsertWriter) Float32(bufferIndex int, value float32) {
	s.ColumnsBuffer[bufferIndex].Float32(value)
}

func (s *InsertWriter) Float64(bufferIndex int, value float64) {
	s.ColumnsBuffer[bufferIndex].Float64(value)
}

func (s *InsertWriter) String(bufferIndex int, value string) {
	s.ColumnsBuffer[bufferIndex].String(value)
}

func (s *InsertWriter) Buffer(bufferIndex int, value []byte) {
	s.ColumnsBuffer[bufferIndex].Buffer(value)
}

func (s *InsertWriter) FixedString(bufferIndex int, value []byte) {
	s.ColumnsBuffer[bufferIndex].Write(value)
}

func (s *InsertWriter) Decimal32(bufferIndex int, value float64, scale int) {
	s.ColumnsBuffer[bufferIndex].Int32(int32(value * factors10[scale]))
}

func (s *InsertWriter) Decimal64(bufferIndex int, value float64, scale int) {
	s.ColumnsBuffer[bufferIndex].Int64(int64(value * factors10[scale]))
}

func (s *InsertWriter) Date(bufferIndex int, value time.Time) {
	if value.Unix() < 0 {
		s.ColumnsBuffer[bufferIndex].Uint16(0)
		return
	}
	_, offset := value.Zone()
	timestamp := value.Unix() + int64(offset)
	s.ColumnsBuffer[bufferIndex].Uint16(uint16(timestamp / 24 / 3600))
}

func (s *InsertWriter) DateTime(bufferIndex int, value time.Time) {
	if value.Unix() < 0 {
		s.ColumnsBuffer[bufferIndex].Uint32(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint32(uint32(value.Unix()))
}

func (s *InsertWriter) UUID(bufferIndex int, value [16]byte) {
	s.ColumnsBuffer[bufferIndex].Write(swapUUID(value[:]))
}

func (s *InsertWriter) AddLen(bufferIndex int, value uint64) {
	s.ColumnsBuffer[bufferIndex].AddLen(value)
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

func (s *InsertWriter) IPv4(bufferIndex int, value net.IP) error {
	if len(value) != 4 {
		return ErrInvalidIPv4
	}
	s.ColumnsBuffer[bufferIndex].Write([]byte{value[3], value[2], value[1], value[0]})
	return nil
}
func (s *InsertWriter) IPv6(bufferIndex int, value net.IP) error {
	if len(value) != 16 {
		return ErrInvalidIPv6
	}
	s.ColumnsBuffer[bufferIndex].Write(value)
	return nil
}

func (s *InsertWriter) Int8P(bufferIndex int, value *int8) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int8(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int8(*value)
}

func (s *InsertWriter) Int16P(bufferIndex int, value *int16) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int16(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int16(*value)
}

func (s *InsertWriter) Int32P(bufferIndex int, value *int32) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int32(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int32(*value)
}

func (s *InsertWriter) Int64P(bufferIndex int, value *int64) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int64(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int64(*value)
}

func (s *InsertWriter) Uint8P(bufferIndex int, value *uint8) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Uint8(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Uint8(*value)
}
func (s *InsertWriter) Uint16P(bufferIndex int, value *uint16) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Uint16(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Uint16(*value)
}

func (s *InsertWriter) Uint32P(bufferIndex int, value *uint32) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Uint32(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Uint32(*value)
}

func (s *InsertWriter) Uint64P(bufferIndex int, value *uint64) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Uint64(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Uint64(*value)
}

func (s *InsertWriter) Float32P(bufferIndex int, value *float32) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Float32(8)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Float32(*value)
}

func (s *InsertWriter) Float64P(bufferIndex int, value *float64) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Float64(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Float64(*value)
}

func (s *InsertWriter) StringP(bufferIndex int, value *string) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].String("")
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].String(*value)
}

func (s *InsertWriter) BufferP(bufferIndex int, value *[]byte) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Buffer([]byte{})
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Buffer(*value)
}

func (s *InsertWriter) FixedStringP(bufferIndex int, empty, value []byte) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Write(empty)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Write(value)
}

func (s *InsertWriter) Decimal32P(bufferIndex int, value *float64, scale int) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int32(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int32(int32(*value * factors10[scale]))
}

func (s *InsertWriter) Decimal64P(bufferIndex int, value *float64, scale int) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int64(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int64(int64(*value * factors10[scale]))
}

func (s *InsertWriter) DateP(bufferIndex int, value *time.Time) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Uint16(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	_, offset := value.Zone()
	timestamp := value.Unix() + int64(offset)
	s.ColumnsBuffer[bufferIndex+1].Uint16(uint16(timestamp / 24 / 3600))
}

func (s *InsertWriter) DateTimeP(bufferIndex int, value *time.Time) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Uint32(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Uint32(uint32(value.Unix()))
}

var emptyUUID = make([]byte, 16)

func (s *InsertWriter) UUIDP(bufferIndex int, value *[16]byte) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Write(emptyUUID)
		return
	}
	// copy data to not change main value by swapUUID
	val := *value
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Write(swapUUID(val[:]))
}

var emptyIPV4 = make([]byte, 4)

func (s *InsertWriter) IPv4P(bufferIndex int, value *net.IP) error {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Write(emptyIPV4)
		return nil
	}
	val := *value
	if len(val) != 4 {
		return ErrInvalidIPv4
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Write([]byte{val[3], val[2], val[1], val[0]})
	return nil
}

var emptyIPV6 = make([]byte, 16)

func (s *InsertWriter) IPv6P(bufferIndex int, value *net.IP) error {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Write(emptyIPV6)
		return nil
	}
	if len(*value) != 16 {
		return ErrInvalidIPv6
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Write(*value)
	return nil
}

func (s *InsertWriter) AddStringLowCardinality(bufferIndex int, value string) {
	s.ColumnsBuffer[bufferIndex].AddStringLowCardinality(value)
}

func (s *InsertWriter) AddFixedStringLowCardinality(bufferIndex int, value []byte) {
	s.ColumnsBuffer[bufferIndex].AddFixedStringLowCardinality(value)
}

func (s *InsertWriter) AddRow(num uint64) {
	s.NumRows += num
}

func (s *InsertWriter) Reset() {
	s.NumRows = 0
	for _, buf := range s.ColumnsBuffer {
		buf.Reset()
	}
}
