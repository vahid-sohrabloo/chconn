package chconn

import (
	"net"
	"time"
)

// Int8S read num of Int8 values
func (s *selectStmt) Int8S(num uint64, value *[]int8) error {
	var (
		val int8
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Int8()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Int16S read num of Int16 values
func (s *selectStmt) Int16S(num uint64, value *[]int16) error {
	var (
		val int16
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Int16()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Int32S read num of Int32 values
func (s *selectStmt) Int32S(num uint64, value *[]int32) error {
	var (
		val int32
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Int32()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Decimal32S read num of Decimal32 values
func (s *selectStmt) Decimal32S(num uint64, value *[]float64, scale int) error {
	var (
		val float64
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Decimal32(scale)
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Decimal64S read num of Decimal64 values
func (s *selectStmt) Decimal64S(num uint64, value *[]float64, scale int) error {
	var (
		val float64
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Decimal64(scale)
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Int64S read num of Int64 values
func (s *selectStmt) Int64S(num uint64, value *[]int64) error {
	var (
		val int64
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Int64()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Uint8S read num of Uint8 values
func (s *selectStmt) Uint8S(num uint64, value *[]uint8) error {
	var (
		val uint8
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Uint8()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Uint16S read num of Uint16 values
func (s *selectStmt) Uint16S(num uint64, value *[]uint16) error {
	var (
		val uint16
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Uint16()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Uint32S read num of Uint32 values
func (s *selectStmt) Uint32S(num uint64, value *[]uint32) error {
	var (
		val uint32
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Uint32()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Uint64S read num of Uint64 values
func (s *selectStmt) Uint64S(num uint64, value *[]uint64) error {
	var (
		val uint64
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Uint64()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Float32S read num of Float32 values
func (s *selectStmt) Float32S(num uint64, value *[]float32) error {
	var (
		val float32
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Float32()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Float64S read num of Float64 values
func (s *selectStmt) Float64S(num uint64, value *[]float64) error {
	var (
		val float64
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Float64()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// StringS read num of String values
func (s *selectStmt) StringS(num uint64, value *[]string) error {
	var (
		val string
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.String()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// ByteArrayS read num of ByteArray values
func (s *selectStmt) ByteArrayS(num uint64, value *[][]byte) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.ByteArray()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// FixedStringS read num of FixedString values
func (s *selectStmt) FixedStringS(num uint64, value *[][]byte, strlen int) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.FixedString(strlen)
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// DateS read num of Date values
func (s *selectStmt) DateS(num uint64, value *[]time.Time) error {
	var (
		val time.Time
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.Date()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// DateTimeS read num of DateTime values
func (s *selectStmt) DateTimeS(num uint64, value *[]time.Time) error {
	var (
		val time.Time
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.DateTime()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// DateTime64S read num of DateTime64 values
func (s *selectStmt) DateTime64S(num uint64, value *[]time.Time, precision int) error {
	var (
		val time.Time
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.DateTime64(precision)
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// UUIDS read num of UUID values
func (s *selectStmt) UUIDS(num uint64, value *[][16]byte) error {
	var (
		val [16]byte
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.UUID()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// IPv4S read num of IPv4 values
func (s *selectStmt) IPv4S(num uint64, value *[]net.IP) error {
	var (
		val net.IP
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.IPv4()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// IPv6S read num of IPv6 values
func (s *selectStmt) IPv6S(num uint64, value *[]net.IP) error {
	var (
		val net.IP
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.IPv6()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}
