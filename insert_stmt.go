package chconn

import (
	"context"
	"math"
	"net"
	"time"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/setting"
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
	stage      queryProcessingStage
	settings   *setting.Settings
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

	err = s.block.writeColumsBuffer(s.conn, writer)
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
func (s *insertStmt) Commit(ctx context.Context, writer *InsertWriter) error {
	s.conn.contextWatcher.Watch(ctx)
	defer s.conn.contextWatcher.Unwatch()
	defer s.conn.unlock()
	err := s.commit(writer)
	if err != nil {
		s.conn.Close(context.Background())
	}
	return err
}

// GetBlock Get current block
func (s *insertStmt) GetBlock() *block {
	return s.block
}

// Writer Get new writer
// if you know the number of buffer and number of columms you can use NewInsertWriter
func (s *insertStmt) Writer() *InsertWriter {
	return NewInsertWriter(s.NumBuffer())
}

// NumColumn get number of columns
func (s *insertStmt) NumColumn() uint64 {
	return s.block.NumColumns
}

// NumBuffer get number of buffer needed for this insert block
func (s *insertStmt) NumBuffer() int {
	return int(s.block.NumBuffer)
}

// InsertWriter is a writer (without clickhouse connection) that can write to buffer
type InsertWriter struct {
	NumRows       uint64
	ColumnsBuffer []*readerwriter.Writer
}

// NewInsertWriter return new InsertWriter
// You shoud know the number of buffer.
// To get the number of buffer that need it for query.
// You can use `InsertStmt.NumBuffer()` or use code generator
func NewInsertWriter(numBuffer int) *InsertWriter {
	columnsBuffer := make([]*readerwriter.Writer, numBuffer)
	for i := 0; i < numBuffer; i++ {
		columnsBuffer[i] = readerwriter.NewWriter()
	}
	return &InsertWriter{
		ColumnsBuffer: columnsBuffer,
	}
}

// Int8 write Int8 data
func (s *InsertWriter) Int8(bufferIndex int, value int8) {
	s.ColumnsBuffer[bufferIndex].Int8(value)
}

// Int16 write Int16 data
func (s *InsertWriter) Int16(bufferIndex int, value int16) {
	s.ColumnsBuffer[bufferIndex].Int16(value)
}

// Int32 write Int32 data
func (s *InsertWriter) Int32(bufferIndex int, value int32) {
	s.ColumnsBuffer[bufferIndex].Int32(value)
}

// Int64 write Int64 data
func (s *InsertWriter) Int64(bufferIndex int, value int64) {
	s.ColumnsBuffer[bufferIndex].Int64(value)
}

// Uint8 write Uint8 data
func (s *InsertWriter) Uint8(bufferIndex int, value uint8) {
	s.ColumnsBuffer[bufferIndex].Uint8(value)
}

// Uint16 write Uint16 data
func (s *InsertWriter) Uint16(bufferIndex int, value uint16) {
	s.ColumnsBuffer[bufferIndex].Uint16(value)
}

// Uint32 write Uint32 data
func (s *InsertWriter) Uint32(bufferIndex int, value uint32) {
	s.ColumnsBuffer[bufferIndex].Uint32(value)
}

// Uint64 write Uint64 data
func (s *InsertWriter) Uint64(bufferIndex int, value uint64) {
	s.ColumnsBuffer[bufferIndex].Uint64(value)
}

// Float32 write Float32 data
func (s *InsertWriter) Float32(bufferIndex int, value float32) {
	s.ColumnsBuffer[bufferIndex].Float32(value)
}

// Float64 write Float64 data
func (s *InsertWriter) Float64(bufferIndex int, value float64) {
	s.ColumnsBuffer[bufferIndex].Float64(value)
}

// String write String data
func (s *InsertWriter) String(bufferIndex int, value string) {
	s.ColumnsBuffer[bufferIndex].String(value)
}

// Buffer write Buffer data
func (s *InsertWriter) Buffer(bufferIndex int, value []byte) {
	s.ColumnsBuffer[bufferIndex].Buffer(value)
}

// FixedString write FixedString data
// NOTE: byte slice size should be equal to FixedString size.
func (s *InsertWriter) FixedString(bufferIndex int, value []byte) {
	s.ColumnsBuffer[bufferIndex].Write(value)
}

// Decimal32 write Decimal32 data
func (s *InsertWriter) Decimal32(bufferIndex int, value float64, scale int) {
	s.ColumnsBuffer[bufferIndex].Int32(int32(value * factors10[scale]))
}

// Decimal64 write Decimal64 data
func (s *InsertWriter) Decimal64(bufferIndex int, value float64, scale int) {
	s.ColumnsBuffer[bufferIndex].Int64(int64(value * factors10[scale]))
}

// Date write Date data
func (s *InsertWriter) Date(bufferIndex int, value time.Time) {
	if value.Unix() < 0 {
		s.ColumnsBuffer[bufferIndex].Uint16(0)
		return
	}
	_, offset := value.Zone()
	timestamp := value.Unix() + int64(offset)
	s.ColumnsBuffer[bufferIndex].Uint16(uint16(timestamp / 24 / 3600))
}

// DateTime write DateTime data
func (s *InsertWriter) DateTime(bufferIndex int, value time.Time) {
	if value.Unix() < 0 {
		s.ColumnsBuffer[bufferIndex].Uint32(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint32(uint32(value.Unix()))
}

// DateTime write DateTime data
func (s *InsertWriter) DateTime64(bufferIndex, precision int, value time.Time) {
	if value.Unix() < 0 {
		s.ColumnsBuffer[bufferIndex].Uint32(0)
		return
	}
	timestamp := value.UnixNano() / int64(math.Pow10(9-precision))
	s.ColumnsBuffer[bufferIndex].Int64(timestamp)
}

// UUID write UUID data
func (s *InsertWriter) UUID(bufferIndex int, value [16]byte) {
	s.ColumnsBuffer[bufferIndex].Write(swapUUID(value[:]))
}

// AddLen add len of array
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

// IPv4 write IPv4 data
func (s *InsertWriter) IPv4(bufferIndex int, value net.IP) error {
	if len(value) != 4 {
		return ErrInvalidIPv4
	}
	s.ColumnsBuffer[bufferIndex].Write([]byte{value[3], value[2], value[1], value[0]})
	return nil
}

// IPv6 write IPv6 data
func (s *InsertWriter) IPv6(bufferIndex int, value net.IP) error {
	if len(value) != 16 {
		return ErrInvalidIPv6
	}
	s.ColumnsBuffer[bufferIndex].Write(value)
	return nil
}

// Int8P write nullable Int8 data
func (s *InsertWriter) Int8P(bufferIndex int, value *int8) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int8(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int8(*value)
}

// Int16P write nullable Int16 data
func (s *InsertWriter) Int16P(bufferIndex int, value *int16) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int16(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int16(*value)
}

// Int32P write nullable Int32 data
func (s *InsertWriter) Int32P(bufferIndex int, value *int32) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int32(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int32(*value)
}

// Int64P write nullable Int64 data
func (s *InsertWriter) Int64P(bufferIndex int, value *int64) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int64(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int64(*value)
}

// Uint8P write nullable Uint8 data
func (s *InsertWriter) Uint8P(bufferIndex int, value *uint8) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Uint8(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Uint8(*value)
}

// Uint16P write nullable Uint16 data
func (s *InsertWriter) Uint16P(bufferIndex int, value *uint16) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Uint16(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Uint16(*value)
}

// Uint32P write nullable Uint32 data
func (s *InsertWriter) Uint32P(bufferIndex int, value *uint32) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Uint32(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Uint32(*value)
}

// Uint64P write nullable Uint64 data
func (s *InsertWriter) Uint64P(bufferIndex int, value *uint64) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Uint64(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Uint64(*value)
}

// Float32P write nullable Float32 data
func (s *InsertWriter) Float32P(bufferIndex int, value *float32) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Float32(8)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Float32(*value)
}

// Float64P write nullable Float64 data
func (s *InsertWriter) Float64P(bufferIndex int, value *float64) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Float64(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Float64(*value)
}

// StringP write nullable String data
func (s *InsertWriter) StringP(bufferIndex int, value *string) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].String("")
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].String(*value)
}

// BufferP write nullable Buffer data
func (s *InsertWriter) BufferP(bufferIndex int, value *[]byte) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Buffer([]byte{})
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Buffer(*value)
}

// FixedStringP write nullable FixedString data
// NOTE: byte slice size should be equal to FixedString size.
func (s *InsertWriter) FixedStringP(bufferIndex int, empty, value []byte) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Write(empty)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Write(value)
}

// Decimal32P write nullable Decimal32 data
func (s *InsertWriter) Decimal32P(bufferIndex int, value *float64, scale int) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int32(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int32(int32(*value * factors10[scale]))
}

// Decimal64P write nullable Decimal64 data
func (s *InsertWriter) Decimal64P(bufferIndex int, value *float64, scale int) {
	if value == nil {
		s.ColumnsBuffer[bufferIndex].Uint8(1)
		s.ColumnsBuffer[bufferIndex+1].Int64(0)
		return
	}
	s.ColumnsBuffer[bufferIndex].Uint8(0)
	s.ColumnsBuffer[bufferIndex+1].Int64(int64(*value * factors10[scale]))
}

// DateP write nullable Date data
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

// DateTimeP write nullable DateTime data
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

// UUIDP write nullable UUID data
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

// IPv4P write nullable IPv4 data
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

// IPv6P write nullable IPv6 data
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

// AddStringLowCardinality write string LowCardinality data
func (s *InsertWriter) AddStringLowCardinality(bufferIndex int, value string) {
	s.ColumnsBuffer[bufferIndex].AddStringLowCardinality(value)
}

// AddStringLowCardinality write FixedString LowCardinality data
// NOTE: byte slice size should be equal to FixedString size.
func (s *InsertWriter) AddFixedStringLowCardinality(bufferIndex int, value []byte) {
	s.ColumnsBuffer[bufferIndex].AddFixedStringLowCardinality(value)
}

// AddRow Add row.
func (s *InsertWriter) AddRow(num uint64) {
	s.NumRows += num
}

// Reset all buffers and ready to insert new sata
func (s *InsertWriter) Reset() {
	s.NumRows = 0
	for _, buf := range s.ColumnsBuffer {
		buf.Reset()
	}
}
