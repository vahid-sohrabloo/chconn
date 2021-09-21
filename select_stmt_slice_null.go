package chconn

import (
	"net"
	"time"
)

// Int8PS read num of Int8 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Int8PS(num uint64, nulls []uint8, values *[]*int8) error {
	return s.Int8PCallback(num, nulls, func(val *int8) {
		*values = append(*values, val)
	})
}

// Int16PS read num of Int16 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Int16PS(num uint64, nulls []uint8, values *[]*int16) error {
	return s.Int16PCallback(num, nulls, func(val *int16) {
		*values = append(*values, val)
	})
}

// Int32PS read num of Int32 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Int32PS(num uint64, nulls []uint8, values *[]*int32) error {
	return s.Int32PCallback(num, nulls, func(val *int32) {
		*values = append(*values, val)
	})
}

// Decimal32PS read num of Decimal32 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Decimal32PS(num uint64, nulls []uint8, values *[]*float64, scale int) error {
	return s.Decimal32PCallback(num, nulls, func(val *float64) {
		*values = append(*values, val)
	}, scale)
}

// Decimal64PS read num of Decimal64 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Decimal64PS(num uint64, nulls []uint8, values *[]*float64, scale int) error {
	return s.Decimal64PCallback(num, nulls, func(val *float64) {
		*values = append(*values, val)
	}, scale)
}

// Int64PS read num of Int64 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Int64PS(num uint64, nulls []uint8, values *[]*int64) error {
	return s.Int64PCallback(num, nulls, func(val *int64) {
		*values = append(*values, val)
	})
}

// Uint8PS read num of Uint8 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Uint8PS(num uint64, nulls []uint8, values *[]*uint8) error {
	return s.Uint8PCallback(num, nulls, func(val *uint8) {
		*values = append(*values, val)
	})
}

// Uint16PS read num of Uint16 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Uint16PS(num uint64, nulls []uint8, values *[]*uint16) error {
	return s.Uint16PCallback(num, nulls, func(val *uint16) {
		*values = append(*values, val)
	})
}

// Uint32PS read num of Uint32 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Uint32PS(num uint64, nulls []uint8, values *[]*uint32) error {
	return s.Uint32PCallback(num, nulls, func(val *uint32) {
		*values = append(*values, val)
	})
}

// Uint64PS read num of Uint64 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Uint64PS(num uint64, nulls []uint8, values *[]*uint64) error {
	return s.Uint64PCallback(num, nulls, func(val *uint64) {
		*values = append(*values, val)
	})
}

// Float32PS read num of Float32 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Float32PS(num uint64, nulls []uint8, values *[]*float32) error {
	return s.Float32PCallback(num, nulls, func(val *float32) {
		*values = append(*values, val)
	})
}

// Float64PS read num of Float64 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Float64PS(num uint64, nulls []uint8, values *[]*float64) error {
	return s.Float64PCallback(num, nulls, func(val *float64) {
		*values = append(*values, val)
	})
}

// StringPS read num of String null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) StringPS(num uint64, nulls []uint8, values *[]*string) error {
	return s.StringPCallback(num, nulls, func(val *string) {
		*values = append(*values, val)
	})
}

// ByteArrayPS read num of ByteArray null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) ByteArrayPS(num uint64, nulls []uint8, values *[][]byte) error {
	return s.ByteArrayPCallback(num, nulls, func(val []byte) {
		*values = append(*values, val)
	})
}

// FixedStringPS read num of FixedString null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) FixedStringPS(num uint64, nulls []uint8, values *[][]byte, strlen int) error {
	return s.FixedStringPCallback(num, nulls, func(val []byte) {
		*values = append(*values, val)
	}, strlen)
}

// DatePS read num of Date null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) DatePS(num uint64, nulls []uint8, values *[]*time.Time) error {
	return s.DatePCallback(num, nulls, func(val *time.Time) {
		*values = append(*values, val)
	})
}

// DateTimePS read num of DateTime null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) DateTimePS(num uint64, nulls []uint8, values *[]*time.Time) error {
	return s.DateTimePCallback(num, nulls, func(val *time.Time) {
		*values = append(*values, val)
	})
}

// DateTime64PS read num of DateTime64 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) DateTime64PS(num uint64, nulls []uint8, values *[]*time.Time, precision int) error {
	return s.DateTime64PCallback(num, nulls, func(val *time.Time) {
		*values = append(*values, val)
	}, precision)
}

// UUIDPS read num of UUID null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) UUIDPS(num uint64, nulls []uint8, values *[]*[16]byte) error {
	return s.UUIDPCallback(num, nulls, func(val *[16]byte) {
		*values = append(*values, val)
	})
}

// IPv4PS read num of IPv4 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) IPv4PS(num uint64, nulls []uint8, values *[]*net.IP) error {
	return s.IPv4PCallback(num, nulls, func(val *net.IP) {
		*values = append(*values, val)
	})
}

// IPv6PS read num of IPv6 null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) IPv6PS(num uint64, nulls []uint8, values *[]*net.IP) error {
	return s.IPv6PCallback(num, nulls, func(val *net.IP) {
		*values = append(*values, val)
	})
}
