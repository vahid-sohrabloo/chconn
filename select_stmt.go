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
	// Int8 read Int8 value
	Int8() (int8, error)
	// Int16 read Int16 value
	Int16() (int16, error)
	// Int32 read Int32 value
	Int32() (int32, error)
	// Decimal32 read Decimal32 value
	Decimal32(scale int) (float64, error)
	// Decimal64 read Decimal64 value
	Decimal64(scale int) (float64, error)
	// Int64 read Int64 value
	Int64() (int64, error)
	// Uint8 read Uint8 value
	Uint8() (uint8, error)
	// Uint16 read Uint16 value
	Uint16() (uint16, error)
	// Uint32 read Uint32 value
	Uint32() (uint32, error)
	// Uint64 read Uint64 value
	Uint64() (uint64, error)
	// Float32 read Float32 value
	Float32() (float32, error)
	// Float64 read Float64 value
	Float64() (float64, error)
	// String read String value
	String() (string, error)
	// ByteArray read String value as []byte
	ByteArray() ([]byte, error)
	// FixedString read FixedString value
	FixedString(strlen int) ([]byte, error)
	// Date read Date value
	Date() (time.Time, error)
	// DateTime read DateTime value
	DateTime() (time.Time, error)
	// UUID read UUID value
	UUID() ([16]byte, error)
	// IPv4 read IPv4 value
	IPv4() (net.IP, error)
	// IPv6 read IPv6 value
	IPv6() (net.IP, error)

	// Int8S read num of Int8 values from a block
	Int8S(num uint64, value *[]int8) error
	// Int16S read num of Int16 values from a block
	Int16S(num uint64, value *[]int16) error
	// Int32S read num of Int32 values from a block
	Int32S(num uint64, value *[]int32) error
	// Decimal32S read num of Decimal32 values from a block
	Decimal32S(num uint64, value *[]float64, scale int) error
	// Decimal64S read num of Decimal64 values from a block
	Decimal64S(num uint64, value *[]float64, scale int) error
	// Int64S read num of Int64 values from a block
	Int64S(num uint64, value *[]int64) error
	// Uint8S read num of Uint8 values from a block
	Uint8S(num uint64, value *[]uint8) error
	// Uint16S read num of Uint16 values from a block
	Uint16S(num uint64, value *[]uint16) error
	// Uint32S read num of Uint32 values from a block
	Uint32S(num uint64, value *[]uint32) error
	// Uint64S read num of Uint64 values from a block
	Uint64S(num uint64, value *[]uint64) error
	// Float32S read num of Float32 values from a block
	Float32S(num uint64, value *[]float32) error
	// Float64S read num of Float64 values from a block
	Float64S(num uint64, value *[]float64) error
	// StringS read num of String values from a block
	StringS(num uint64, value *[]string) error
	// ByteArrayS read num of String values as []byte from a block
	ByteArrayS(num uint64, value *[][]byte) error
	// FixedStringS read num of FixedString values from a block
	FixedStringS(num uint64, value *[][]byte, strlen int) error
	// DateS read num of Date values from a block
	DateS(num uint64, value *[]time.Time) error
	// DateTimeS read num of DateTime values from a block
	DateTimeS(num uint64, value *[]time.Time) error
	// UUIDS read num of UUID values from a block
	UUIDS(num uint64, value *[][16]byte) error
	// IPv4S read num of IPv4 values from a block
	IPv4S(num uint64, value *[]net.IP) error
	// IPv6S read num of IPv6 values from a block
	IPv6S(num uint64, value *[]net.IP) error
	// LenS Read num of len of Array
	LenS(num uint64, value *[]int) (lastOffset uint64, err error)
	// Int8All read all Int8 values from a block
	Int8All(value *[]int8) error
	// Int16All read all Int16 values from a block
	Int16All(value *[]int16) error
	// Int32All read all Int32 values from a block
	Int32All(value *[]int32) error
	// Decimal32All read all Decimal32 values from a block
	Decimal32All(value *[]float64, scale int) error
	// Decimal64All read all Decimal64 values from a block
	Decimal64All(value *[]float64, scale int) error
	// Int64All read all Int64 values from a block
	Int64All(value *[]int64) error
	// Uint8All read all Uint8 values from a block
	Uint8All(value *[]uint8) error
	// Uint16All read all Uint16 values from a block
	Uint16All(value *[]uint16) error
	// Uint32All read all Uint32 values from a block
	Uint32All(value *[]uint32) error
	// Uint64All read all Uint64 values from a block
	Uint64All(value *[]uint64) error
	// Float32All read all Float32 values from a block
	Float32All(value *[]float32) error
	// Float64All read all Float64 values from a block
	Float64All(value *[]float64) error
	// StringAll read all String values from a block
	StringAll(value *[]string) error
	// ByteArrayAll read all ByteArray values from a block
	ByteArrayAll(value *[][]byte) error
	// FixedStringAll read all FixedString values from a block
	FixedStringAll(value *[][]byte, strlen int) error
	// DateAll read all Date values from a block
	DateAll(value *[]time.Time) error
	// DateTimeAll read all DateTime values from a block
	DateTimeAll(value *[]time.Time) error
	// UUIDAll read all UUID values from a block
	UUIDAll(value *[][16]byte) error
	// IPv4All read all IPv4 values from a block
	IPv4All(value *[]net.IP) error
	// IPv6All read all IPv6 values from a block
	IPv6All(value *[]net.IP) error
	// LenAll read all Array Len values from a block
	LenAll(value *[]int) (uint64, error)
	// LowCardinalityString read LowCardinality String values from a block
	LowCardinalityString(values *[]string) error
	// LowCardinalityFixedString read LowCardinality Fixed String values from a block
	LowCardinalityFixedString(values *[][]byte, strlne int) error

	// Int8PS read num of Int8 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Int8PS(num uint64, nulls []uint8, value *[]*int8) error
	// Int16PS read num of Int16 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Int16PS(num uint64, nulls []uint8, value *[]*int16) error
	// Int32PS read num of Int32 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Int32PS(num uint64, nulls []uint8, value *[]*int32) error
	// Decimal32PS read num of Decimal32 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Decimal32PS(num uint64, nulls []uint8, value *[]*float64, scale int) error
	// Decimal64PS read num of Decimal64 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Decimal64PS(num uint64, nulls []uint8, value *[]*float64, scale int) error
	// Int64PS read num of Int64 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Int64PS(num uint64, nulls []uint8, value *[]*int64) error
	// Uint8PS read num of Uint8 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Uint8PS(num uint64, nulls []uint8, value *[]*uint8) error
	// Uint16PS read num of Uint16 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Uint16PS(num uint64, nulls []uint8, value *[]*uint16) error
	// Uint32PS read num of Uint32 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Uint32PS(num uint64, nulls []uint8, value *[]*uint32) error
	// Uint64PS read num of Uint64 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Uint64PS(num uint64, nulls []uint8, value *[]*uint64) error
	// Float32PS read num of Float32 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Float32PS(num uint64, nulls []uint8, value *[]*float32) error
	// Float64PS read num of Float64 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	Float64PS(num uint64, nulls []uint8, value *[]*float64) error
	// StringPS read num of String nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	StringPS(num uint64, nulls []uint8, value *[]*string) error
	// ByteArrayPS read num of String  nullable values as []byte from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	ByteArrayPS(num uint64, nulls []uint8, value *[][]byte) error
	// FixedStringPS read num of FixedString nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	FixedStringPS(num uint64, nulls []uint8, value *[][]byte, strlen int) error
	// DatePS read num of Date nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	DatePS(num uint64, nulls []uint8, value *[]*time.Time) error
	// DateTimePS read num of DateTime nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	DateTimePS(num uint64, nulls []uint8, value *[]*time.Time) error
	// UUIDPS read num of UUID nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	UUIDPS(num uint64, nulls []uint8, value *[]*[16]byte) error
	// IPv4PS read num of IPv4 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	IPv4PS(num uint64, nulls []uint8, value *[]*net.IP) error
	// IPv6PS read num of IPv6 nullable values from a block
	// NOTE: Should read nulls with GetNullS or GetNullAll and pass to this function
	IPv6PS(num uint64, nulls []uint8, value *[]*net.IP) error
	// Int8PAll read all Int8 nullable values from a block
	Int8PAll(value *[]*int8) error
	// Int16PAll read all Int16 nullable values from a block
	Int16PAll(value *[]*int16) error
	// Int32PAll read all Int32 nullable values from a block
	Int32PAll(value *[]*int32) error
	// Decimal32PAll read all Decimal32 nullable values from a block
	Decimal32PAll(value *[]*float64, scale int) error
	// Decimal64PAll read all Decimal64 nullable values from a block
	Decimal64PAll(value *[]*float64, scale int) error
	// Int64PAll read all Int64 nullable values from a block
	Int64PAll(value *[]*int64) error
	// Uint8PAll read all Uint8 nullable values from a block
	Uint8PAll(value *[]*uint8) error
	// Uint16PAll read all Uint16 nullable values from a block
	Uint16PAll(value *[]*uint16) error
	// Uint32PAll read all Uint32 nullable values from a block
	Uint32PAll(value *[]*uint32) error
	// Uint64PAll read all Uint64 nullable values from a block
	Uint64PAll(value *[]*uint64) error
	// Float32PAll read all Float32 nullable values from a block
	Float32PAll(value *[]*float32) error
	// Float64PAll read all Float64 nullable values from a block
	Float64PAll(value *[]*float64) error
	// StringPAll read all String nullable values from a block
	StringPAll(value *[]*string) error
	// ByteArrayPAll read all String nullable values as []byte from a block
	ByteArrayPAll(value *[][]byte) error
	// FixedStringPAll read all FixedString nullable values from a block
	FixedStringPAll(value *[][]byte, strlen int) error
	// DatePAll read all Date nullable values from a block
	DatePAll(value *[]*time.Time) error
	// DateTimePAll read all DateTime nullable values from a block
	DateTimePAll(value *[]*time.Time) error
	// UUIDPAll read all UUID nullable values from a block
	UUIDPAll(value *[]*[16]byte) error
	// IPv4PAll read all IPv4 nullable values from a block
	IPv4PAll(value *[]*net.IP) error
	// IPv6PAll read all IPv6 nullable values from a block
	IPv6PAll(value *[]*net.IP) error
	// GetNullS read num of nullable values from a block
	GetNullS(num uint64) ([]uint8, error)
	// GetNullS read all nullable values from a block
	GetNullSAll() ([]uint8, error)
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
	closed      bool
}

// Next get the next block, if available return true else return false
// if the server sends an error return false and we can get the last error with Err() function
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

// RowsInBlock return number of rows in this current block
func (s *selectStmt) RowsInBlock() uint64 {
	return s.block.NumRows
}

// Err When calls Next() func if server send error we can get error from thhis function
func (s *selectStmt) Err() error {
	return s.lastErr
}

// Close after reads all data should call this function to unlock connection
// NOTE: You shoud read all data and then call this function
func (s *selectStmt) Close() {
	if !s.closed {
		s.closed = true
		s.conn.unlock()
	}
}

// NextColumn get the next column of block
func (s *selectStmt) NextColumn() (*Column, error) {
	column, err := s.block.nextColumn(s.conn)
	if err != nil {
		s.Close()
		s.conn.Close(context.Background())
	}
	return column, err
}

// Int8 read Int8 value
func (s *selectStmt) Int8() (int8, error) {
	return s.conn.reader.Int8()
}

// Int16 read Int16 value
func (s *selectStmt) Int16() (int16, error) {
	return s.conn.reader.Int16()
}

// Int32 read Int32 value
func (s *selectStmt) Int32() (int32, error) {
	return s.conn.reader.Int32()
}

// Decimal32 read Decimal32 value
func (s *selectStmt) Decimal32(scale int) (float64, error) {
	val, err := s.conn.reader.Int32()
	return float64(val) / factors10[scale], err
}

// Decimal64 read Decimal64 value
func (s *selectStmt) Decimal64(scale int) (float64, error) {
	val, err := s.conn.reader.Int64()
	return float64(val) / factors10[scale], err
}

// Int64 read Int64 value
func (s *selectStmt) Int64() (int64, error) {
	return s.conn.reader.Int64()
}

// Uint8 read Uint8 value
func (s *selectStmt) Uint8() (uint8, error) {
	return s.conn.reader.Uint8()
}

// Uint16 read Uint16 value
func (s *selectStmt) Uint16() (uint16, error) {
	return s.conn.reader.Uint16()
}

// Uint32 read Uint32 value
func (s *selectStmt) Uint32() (uint32, error) {
	return s.conn.reader.Uint32()
}

// Uint64 read Uint64 value
func (s *selectStmt) Uint64() (uint64, error) {
	return s.conn.reader.Uint64()
}

// Float32 read Float32 value
func (s *selectStmt) Float32() (float32, error) {
	return s.conn.reader.Float32()
}

// Float64 read Float64 value
func (s *selectStmt) Float64() (float64, error) {
	return s.conn.reader.Float64()
}

// String read String value
func (s *selectStmt) String() (string, error) {
	return s.conn.reader.String()
}

// ByteArray read String value as []byte
func (s *selectStmt) ByteArray() ([]byte, error) {
	return s.conn.reader.ByteArray()
}

// FixedString read FixedString value
func (s *selectStmt) FixedString(strlen int) ([]byte, error) {
	return s.conn.reader.FixedString(strlen)
}

// Date read Date value
func (s *selectStmt) Date() (time.Time, error) {
	val, err := s.conn.reader.Int16()
	return time.Unix(int64(val)*24*3600, 0), err
}

// DateTime read DateTime value
func (s *selectStmt) DateTime() (time.Time, error) {
	val, err := s.conn.reader.Uint32()
	return time.Unix(int64(val), 0), err
}

// UUID read UUID value
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

// IPv4 read IPv4 value
func (s *selectStmt) IPv4() (net.IP, error) {
	var (
		val []byte
		err error
	)
	val, err = s.conn.reader.FixedString(4)
	return net.IPv4(val[3], val[2], val[1], val[0]).To4(), err
}

// IPv6 read IPv6 value
func (s *selectStmt) IPv6() (net.IP, error) {
	var (
		val []byte
		err error
	)

	val, err = s.conn.reader.FixedString(16)
	return net.IP(val), err
}
