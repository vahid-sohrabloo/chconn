package chconn

import (
	"net"
	"time"
)

// Int8PCallback read num of Int8 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Int8PCallback(num uint64, nulls []uint8, cb func(*int8)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Int8()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// Int16PCallback read num of Int16 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Int16PCallback(num uint64, nulls []uint8, cb func(*int16)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Int16()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// Int32PCallback read num of Int32 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Int32PCallback(num uint64, nulls []uint8, cb func(*int32)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Int32()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// Decimal32PCallback read num of Decimal32 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Decimal32PCallback(num uint64, nulls []uint8, cb func(*float64), scale int) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Decimal32(scale)
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// Decimal64PCallback read num of Decimal64 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Decimal64PCallback(num uint64, nulls []uint8, cb func(*float64), scale int) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Decimal64(scale)
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// Int64PCallback read num of Int64 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Int64PCallback(num uint64, nulls []uint8, cb func(*int64)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Int64()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// Uint8PCallback read num of Uint8 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Uint8PCallback(num uint64, nulls []uint8, cb func(*uint8)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Uint8()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// Uint16PCallback read num of Uint16 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Uint16PCallback(num uint64, nulls []uint8, cb func(*uint16)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Uint16()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// Uint32PCallback read num of Uint32 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Uint32PCallback(num uint64, nulls []uint8, cb func(*uint32)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Uint32()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// Uint64PCallback read num of Uint64 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Uint64PCallback(num uint64, nulls []uint8, cb func(*uint64)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Uint64()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// Float32PCallback read num of Float32 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Float32PCallback(num uint64, nulls []uint8, cb func(*float32)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Float32()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// Float64PCallback read num of Float64 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) Float64PCallback(num uint64, nulls []uint8, cb func(*float64)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Float64()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// StringPCallback read num of String null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) StringPCallback(num uint64, nulls []uint8, cb func(*string)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.String()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// ByteArrayPCallback read num of ByteArray null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) ByteArrayPCallback(num uint64, nulls []uint8, cb func([]byte)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.ByteArray()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// FixedStringPCallback read num of FixedString null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) FixedStringPCallback(num uint64, nulls []uint8, cb func([]byte), strlen int) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.FixedString(strlen)
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// DatePCallback read num of Date null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) DatePCallback(num uint64, nulls []uint8, cb func(*time.Time)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.Date()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// DateTimePCallback read num of DateTime null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) DateTimePCallback(num uint64, nulls []uint8, cb func(*time.Time)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.DateTime()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// DateTime64PCallback read num of DateTime64 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) DateTime64PCallback(num uint64, nulls []uint8, cb func(*time.Time), precision int) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.DateTime64(precision)
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// UUIDPCallback read num of UUID null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) UUIDPCallback(num uint64, nulls []uint8, cb func(*[16]byte)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.UUID()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// IPv4PCallback read num of IPv4 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) IPv4PCallback(num uint64, nulls []uint8, cb func(*net.IP)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.IPv4()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

// IPv6PCallback read num of IPv6 null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) IPv6PCallback(num uint64, nulls []uint8, cb func(*net.IP)) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.IPv6()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}
