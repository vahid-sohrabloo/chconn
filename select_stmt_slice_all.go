package chconn

import (
	"net"
	"time"
)

// Int8All reads all Int8 values from a block
func (s *selectStmt) Int8All(value *[]int8) error {
	return s.Int8S(s.block.NumRows, value)
}

// Int16All reads all Int16 values from a block
func (s *selectStmt) Int16All(value *[]int16) error {
	return s.Int16S(s.block.NumRows, value)
}

// Int32All reads all Int32 values from a block
func (s *selectStmt) Int32All(value *[]int32) error {
	return s.Int32S(s.block.NumRows, value)
}

// Decimal32All reads all Decimal32 values from a block
func (s *selectStmt) Decimal32All(value *[]float64, scale int) error {
	return s.Decimal32S(s.block.NumRows, value, scale)
}

// Decimal64All reads all Decimal64 values from a block
func (s *selectStmt) Decimal64All(value *[]float64, scale int) error {
	return s.Decimal64S(s.block.NumRows, value, scale)
}

// Int64All reads all Int64 values from a block
func (s *selectStmt) Int64All(value *[]int64) error {
	return s.Int64S(s.block.NumRows, value)
}

// Uint8All reads all Uint8 values from a block
func (s *selectStmt) Uint8All(value *[]uint8) error {
	return s.Uint8S(s.block.NumRows, value)
}

// Uint16All reads all Uint16 values from a block
func (s *selectStmt) Uint16All(value *[]uint16) error {
	return s.Uint16S(s.block.NumRows, value)
}

// Uint32All reads all Uint32 values from a block
func (s *selectStmt) Uint32All(value *[]uint32) error {
	return s.Uint32S(s.block.NumRows, value)
}

// Uint64All reads all Uint64 values from a block
func (s *selectStmt) Uint64All(value *[]uint64) error {
	return s.Uint64S(s.block.NumRows, value)
}

// Float32All reads all Float32 values from a block
func (s *selectStmt) Float32All(value *[]float32) error {
	return s.Float32S(s.block.NumRows, value)
}

// Float64All reads all Float64 values from a block
func (s *selectStmt) Float64All(value *[]float64) error {
	return s.Float64S(s.block.NumRows, value)
}

// StringAll reads all String values from a block
func (s *selectStmt) StringAll(value *[]string) error {
	return s.StringS(s.block.NumRows, value)
}

// ByteArrayAll reads all ByteArray values from a block
func (s *selectStmt) ByteArrayAll(value *[][]byte) error {
	return s.ByteArrayS(s.block.NumRows, value)
}

// FixedStringAll reads all FixedString values from a block
func (s *selectStmt) FixedStringAll(value *[][]byte, strlen int) error {
	return s.FixedStringS(s.block.NumRows, value, strlen)
}

// DateAll reads all Date values from a block
func (s *selectStmt) DateAll(value *[]time.Time) error {
	return s.DateS(s.block.NumRows, value)
}

// DateTimeAll reads all DateTime values from a block
func (s *selectStmt) DateTimeAll(value *[]time.Time) error {
	return s.DateTimeS(s.block.NumRows, value)
}

// DateTime64All reads all DateTime64 values from a block
func (s *selectStmt) DateTime64All(value *[]time.Time, precision int) error {
	return s.DateTime64S(s.block.NumRows, value, precision)
}

// UUIDAll reads all UUID values from a block
func (s *selectStmt) UUIDAll(value *[][16]byte) error {
	return s.UUIDS(s.block.NumRows, value)
}

// IPv4All reads all IPv4 values from a block
func (s *selectStmt) IPv4All(value *[]net.IP) error {
	return s.IPv4S(s.block.NumRows, value)
}

// IPv6All reads all IPv6 values from a block
func (s *selectStmt) IPv6All(value *[]net.IP) error {
	return s.IPv6S(s.block.NumRows, value)
}
