package chconn

import (
	"net"
	"time"
)

// Int8PAll read all Int8 null values from a block
func (s *selectStmt) Int8PAll(values *[]*int8) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Int8PS(s.block.NumRows, nulls, values)
}

// Int16PAll read all Int16 null values from a block
func (s *selectStmt) Int16PAll(values *[]*int16) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Int16PS(s.block.NumRows, nulls, values)
}

// Int32PAll read all Int32 null values from a block
func (s *selectStmt) Int32PAll(values *[]*int32) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Int32PS(s.block.NumRows, nulls, values)
}

// Decimal32PAll read all Decimal32 null values from a block
func (s *selectStmt) Decimal32PAll(values *[]*float64, scale int) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Decimal32PS(s.block.NumRows, nulls, values, scale)
}

// Decimal64PAll read all Decimal64 null values from a block
func (s *selectStmt) Decimal64PAll(values *[]*float64, scale int) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Decimal64PS(s.block.NumRows, nulls, values, scale)
}

// Int64PAll read all Int64 null values from a block
func (s *selectStmt) Int64PAll(values *[]*int64) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Int64PS(s.block.NumRows, nulls, values)
}

// Uint8PAll read all Uint8 null values from a block
func (s *selectStmt) Uint8PAll(values *[]*uint8) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Uint8PS(s.block.NumRows, nulls, values)
}

// Uint16PAll read all Uint16 null values from a block
func (s *selectStmt) Uint16PAll(values *[]*uint16) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Uint16PS(s.block.NumRows, nulls, values)
}

// Uint32PAll read all Uint32 null values from a block
func (s *selectStmt) Uint32PAll(values *[]*uint32) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Uint32PS(s.block.NumRows, nulls, values)
}

// Uint64PAll read all Uint64 null values from a block
func (s *selectStmt) Uint64PAll(values *[]*uint64) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Uint64PS(s.block.NumRows, nulls, values)
}

// Float32PAll read all Float32 null values from a block
func (s *selectStmt) Float32PAll(values *[]*float32) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Float32PS(s.block.NumRows, nulls, values)
}

// Float64PAll read all Float64 null values from a block
func (s *selectStmt) Float64PAll(values *[]*float64) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.Float64PS(s.block.NumRows, nulls, values)
}

// StringPAll read all String null values from a block
func (s *selectStmt) StringPAll(values *[]*string) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.StringPS(s.block.NumRows, nulls, values)
}

// ByteArrayPAll read all ByteArray null values from a block
func (s *selectStmt) ByteArrayPAll(values *[][]byte) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.ByteArrayPS(s.block.NumRows, nulls, values)
}

// FixedStringPAll read all FixedString null values from a block
func (s *selectStmt) FixedStringPAll(values *[][]byte, strlen int) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.FixedStringPS(s.block.NumRows, nulls, values, strlen)
}

// DatePAll read all Date null values from a block
func (s *selectStmt) DatePAll(values *[]*time.Time) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.DatePS(s.block.NumRows, nulls, values)
}

// DateTimePAll read all DateTime null values from a block
func (s *selectStmt) DateTimePAll(values *[]*time.Time) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.DateTimePS(s.block.NumRows, nulls, values)
}

// DateTime64PAll read all DateTime64 null values from a block
func (s *selectStmt) DateTime64PAll(values *[]*time.Time, precision int) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.DateTime64PS(s.block.NumRows, nulls, values, precision)
}

// UUIDPAll read all UUID null values from a block
func (s *selectStmt) UUIDPAll(values *[]*[16]byte) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.UUIDPS(s.block.NumRows, nulls, values)
}

// IPv4PAll read all IPv4 null values from a block
func (s *selectStmt) IPv4PAll(values *[]*net.IP) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.IPv4PS(s.block.NumRows, nulls, values)
}

// IPv6PAll read all IPv6 null values from a block
func (s *selectStmt) IPv6PAll(values *[]*net.IP) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.IPv6PS(s.block.NumRows, nulls, values)
}
