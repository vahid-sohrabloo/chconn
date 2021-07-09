package chconn

import (
	"context"
	"net"
	"time"
)

type SelectStmt interface {
	Next() bool
	Err() error
	RowsInBlock() uint64
	Close()
	NextColumn() (*Column, error)
	Int8() (int8, error)
	Int16() (int16, error)
	Int32() (int32, error)
	Decimal32(scale int) (float64, error)
	Decimal64(scale int) (float64, error)
	Int64() (int64, error)
	Uint8() (uint8, error)
	Uint16() (uint16, error)
	Uint32() (uint32, error)
	Uint64() (uint64, error)
	Float32() (float32, error)
	Float64() (float64, error)
	String() (string, error)
	ByteArray() ([]byte, error)
	FixedString(strlen int) ([]byte, error)
	Date() (time.Time, error)
	DateTime() (time.Time, error)
	UUID() ([16]byte, error)
	IPv4() (net.IP, error)
	IPv6() (net.IP, error)

	Int8S(num uint64, value *[]int8) error
	Int16S(num uint64, value *[]int16) error
	Int32S(num uint64, value *[]int32) error
	Decimal32S(num uint64, value *[]float64, scale int) error
	Decimal64S(num uint64, value *[]float64, scale int) error
	Int64S(num uint64, value *[]int64) error
	Uint8S(num uint64, value *[]uint8) error
	Uint16S(num uint64, value *[]uint16) error
	Uint32S(num uint64, value *[]uint32) error
	Uint64S(num uint64, value *[]uint64) error
	Float32S(num uint64, value *[]float32) error
	Float64S(num uint64, value *[]float64) error
	StringS(num uint64, value *[]string) error
	ByteArrayS(num uint64, value *[][]byte) error
	FixedStringS(num uint64, value *[][]byte, strlen int) error
	DateS(num uint64, value *[]time.Time) error
	DateTimeS(num uint64, value *[]time.Time) error
	UUIDS(num uint64, value *[][16]byte) error
	IPv4S(num uint64, value *[]net.IP) error
	IPv6S(num uint64, value *[]net.IP) error
	LenS(num uint64, value *[]int) (lastOffset uint64, err error)
	Int8All(value *[]int8) error
	Int16All(value *[]int16) error
	Int32All(value *[]int32) error
	Decimal32All(value *[]float64, scale int) error
	Decimal64All(value *[]float64, scale int) error
	Int64All(value *[]int64) error
	Uint8All(value *[]uint8) error
	Uint16All(value *[]uint16) error
	Uint32All(value *[]uint32) error
	Uint64All(value *[]uint64) error
	Float32All(value *[]float32) error
	Float64All(value *[]float64) error
	StringAll(value *[]string) error
	ByteArrayAll(value *[][]byte) error
	FixedStringAll(value *[][]byte, strlen int) error
	DateAll(value *[]time.Time) error
	DateTimeAll(value *[]time.Time) error
	UUIDAll(value *[][16]byte) error
	IPv4All(value *[]net.IP) error
	IPv6All(value *[]net.IP) error
	LenAll(value *[]int) (uint64, error)

	Int8PS(num uint64, nulls []uint8, value *[]*int8) error
	Int16PS(num uint64, nulls []uint8, value *[]*int16) error
	Int32PS(num uint64, nulls []uint8, value *[]*int32) error
	Decimal32PS(num uint64, nulls []uint8, value *[]*float64, scale int) error
	Decimal64PS(num uint64, nulls []uint8, value *[]*float64, scale int) error
	Int64PS(num uint64, nulls []uint8, value *[]*int64) error
	Uint8PS(num uint64, nulls []uint8, value *[]*uint8) error
	Uint16PS(num uint64, nulls []uint8, value *[]*uint16) error
	Uint32PS(num uint64, nulls []uint8, value *[]*uint32) error
	Uint64PS(num uint64, nulls []uint8, value *[]*uint64) error
	Float32PS(num uint64, nulls []uint8, value *[]*float32) error
	Float64PS(num uint64, nulls []uint8, value *[]*float64) error
	StringPS(num uint64, nulls []uint8, value *[]*string) error
	ByteArrayPS(num uint64, nulls []uint8, value *[][]byte) error
	FixedStringPS(num uint64, nulls []uint8, value *[][]byte, strlen int) error
	DatePS(num uint64, nulls []uint8, value *[]*time.Time) error
	DateTimePS(num uint64, nulls []uint8, value *[]*time.Time) error
	UUIDPS(num uint64, nulls []uint8, value *[]*[16]byte) error
	IPv4PS(num uint64, nulls []uint8, value *[]*net.IP) error
	IPv6PS(num uint64, nulls []uint8, value *[]*net.IP) error
	Int8PAll(value *[]*int8) error
	Int16PAll(value *[]*int16) error
	Int32PAll(value *[]*int32) error
	Decimal32PAll(value *[]*float64, scale int) error
	Decimal64PAll(value *[]*float64, scale int) error
	Int64PAll(value *[]*int64) error
	Uint8PAll(value *[]*uint8) error
	Uint16PAll(value *[]*uint16) error
	Uint32PAll(value *[]*uint32) error
	Uint64PAll(value *[]*uint64) error
	Float32PAll(value *[]*float32) error
	Float64PAll(value *[]*float64) error
	StringPAll(value *[]*string) error
	ByteArrayPAll(value *[][]byte) error
	FixedStringPAll(value *[][]byte, strlen int) error
	DatePAll(value *[]*time.Time) error
	DateTimePAll(value *[]*time.Time) error
	UUIDPAll(value *[]*[16]byte) error
	IPv4PAll(value *[]*net.IP) error
	IPv6PAll(value *[]*net.IP) error
	GetNullS(num uint64) ([]uint8, error)
	GetNullSAll() ([]uint8, error)
	LowCardinalityString(values *[]string) error
	LowCardinalityFixedString(values *[][]byte, length int) error
}
type selectStmt struct {
	block       *block
	conn        *conn
	query       string
	queryID     string
	clientInfo  *ClientInfo
	onProgress  func(*Progress)
	onProfile   func(*Profile)
	lastErr     error
	ProfileInfo *Profile
	Progress    *Progress
	nulls       []uint8
	setting     *Settings
	closed      bool
}

// Next get  next block of available return true else return false
// if server send error return false and fill LastErr
func (s *selectStmt) Next() bool {
	res, err := s.conn.reciveAndProccessData(nil)
	if err != nil {
		s.lastErr = err
		return false
	}

	if block, ok := res.(*block); ok {
		if block.NumRows == 0 {
			err = block.readColumns(s.conn)
			if err != nil {
				s.lastErr = err
				return false
			}
			return s.Next()
		}
		block.setting = s.setting
		s.block = block
		return true
	}

	if profile, ok := res.(*Profile); ok {
		s.ProfileInfo = profile
		if s.onProfile != nil {
			s.onProfile(profile)
		}
		return s.Next()
	}
	if progress, ok := res.(*Progress); ok {
		s.Progress = progress
		if s.onProgress != nil {
			s.onProgress(progress)
		}
		return s.Next()
	}

	if res == nil {
		return false
	}

	s.lastErr = &unexpectedPacket{expected: "serverData", actual: res}
	return false
}

func (s *selectStmt) RowsInBlock() uint64 {
	return s.block.NumRows
}

func (s *selectStmt) Err() error {
	return s.lastErr
}

func (s *selectStmt) Close() {
	if !s.closed {
		s.closed = true
		s.conn.unlock()
	}
}

func (s *selectStmt) NextColumn() (*Column, error) {
	column, err := s.block.nextColumn(s.conn)
	if err != nil {
		s.Close()
		s.conn.Close(context.Background())
	}
	return column, err
}

func (s *selectStmt) Int8() (int8, error) {
	return s.conn.reader.Int8()
}

func (s *selectStmt) Int16() (int16, error) {
	return s.conn.reader.Int16()
}

func (s *selectStmt) Int32() (int32, error) {
	return s.conn.reader.Int32()
}

func (s *selectStmt) Decimal32(scale int) (float64, error) {
	val, err := s.conn.reader.Int32()
	return float64(val) / factors10[scale], err
}

func (s *selectStmt) Decimal64(scale int) (float64, error) {
	val, err := s.conn.reader.Int64()
	return float64(val) / factors10[scale], err
}

func (s *selectStmt) Int64() (int64, error) {
	return s.conn.reader.Int64()
}

func (s *selectStmt) Uint8() (uint8, error) {
	return s.conn.reader.Uint8()
}

func (s *selectStmt) Uint16() (uint16, error) {
	return s.conn.reader.Uint16()
}

func (s *selectStmt) Uint32() (uint32, error) {
	return s.conn.reader.Uint32()
}

func (s *selectStmt) Uint64() (uint64, error) {
	return s.conn.reader.Uint64()
}

func (s *selectStmt) Float32() (float32, error) {
	return s.conn.reader.Float32()
}

func (s *selectStmt) Float64() (float64, error) {
	return s.conn.reader.Float64()
}

func (s *selectStmt) String() (string, error) {
	return s.conn.reader.String()
}

func (s *selectStmt) ByteArray() ([]byte, error) {
	return s.conn.reader.ByteArray()
}

func (s *selectStmt) FixedString(strlen int) ([]byte, error) {
	return s.conn.reader.FixedString(strlen)
}

func (s *selectStmt) Date() (time.Time, error) {
	val, err := s.conn.reader.Int16()
	return time.Unix(int64(val)*24*3600, 0), err
}

func (s *selectStmt) DateTime() (time.Time, error) {
	val, err := s.conn.reader.Uint32()
	return time.Unix(int64(val), 0), err
}

func (s *selectStmt) UUID() ([16]byte, error) {
	var (
		val [16]byte
		err error
	)
	uuidData := make([]byte, 16)
	_, err = s.conn.reader.Read(uuidData)
	uuidData = swapUUID(uuidData)
	copy(val[:], uuidData)
	return val, err
}

func (s *selectStmt) IPv4() (net.IP, error) {
	var (
		val []byte
		err error
	)
	val, err = s.conn.reader.FixedString(4)
	return net.IPv4(val[3], val[2], val[1], val[0]).To4(), err
}

func (s *selectStmt) IPv6() (net.IP, error) {
	var (
		val []byte
		err error
	)

	val, err = s.conn.reader.FixedString(16)
	return net.IP(val), err
}
