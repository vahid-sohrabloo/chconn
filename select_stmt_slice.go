package chconn

import (
	"net"
	"time"
)

// Int8S read num of Int8 values from a block
func (s *selectStmt) Int8S(num uint64, value *[]int8) error {
	var (
		val int8
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Int8()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Int16S read num of Int16 values from a block
func (s *selectStmt) Int16S(num uint64, value *[]int16) error {
	var (
		val int16
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Int16()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Int32S read num of Int32 values from a block
func (s *selectStmt) Int32S(num uint64, value *[]int32) error {
	var (
		val int32
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Int32()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Decimal32S read num of Decimal32 values from a block
func (s *selectStmt) Decimal32S(num uint64, value *[]float64, scale int) error {
	var (
		val int32
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Int32()
		if err != nil {
			return err
		}
		*value = append(*value, float64(val)/factors10[scale])
	}
	return nil
}

// Decimal64S read num of Decimal64 values from a block
func (s *selectStmt) Decimal64S(num uint64, value *[]float64, scale int) error {
	var (
		val int64
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Int64()
		if err != nil {
			return err
		}
		*value = append(*value, float64(val)/factors10[scale])
	}
	return nil
}

// Int64S read num of Int64 values from a block
func (s *selectStmt) Int64S(num uint64, value *[]int64) error {
	var (
		val int64
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Int64()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Uint8S read num of Uint8 values from a block
func (s *selectStmt) Uint8S(num uint64, value *[]uint8) error {
	var (
		val uint8
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Uint8()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Uint16S read num of Uint16 values from a block
func (s *selectStmt) Uint16S(num uint64, value *[]uint16) error {
	var (
		val uint16
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Uint16()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Uint32S read num of Uint32 values from a block
func (s *selectStmt) Uint32S(num uint64, value *[]uint32) error {
	var (
		val uint32
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Uint32()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Uint64S read num of Uint64 values from a block
func (s *selectStmt) Uint64S(num uint64, value *[]uint64) error {
	var (
		val uint64
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Uint64()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Float32S read num of Float32 values from a block
func (s *selectStmt) Float32S(num uint64, value *[]float32) error {
	var (
		val float32
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Float32()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// Float64S read num of Float64 values from a block
func (s *selectStmt) Float64S(num uint64, value *[]float64) error {
	var (
		val float64
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Float64()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// StringS read num of String values from a block
func (s *selectStmt) StringS(num uint64, value *[]string) error {
	var (
		val string
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.String()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// ByteArrayS read num of String values as []byte from a block
func (s *selectStmt) ByteArrayS(num uint64, value *[][]byte) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.ByteArray()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// FixedStringS read num of FixedString values from a block
func (s *selectStmt) FixedStringS(num uint64, value *[][]byte, strlen int) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.FixedString(strlen)
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

// DateS read num of Date values from a block
func (s *selectStmt) DateS(num uint64, value *[]time.Time) error {
	var (
		val int16
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Int16()
		if err != nil {
			return err
		}
		*value = append(*value, time.Unix(int64(val)*24*3600, 0))
	}
	return nil
}

// DateTimeS read num of DateTime values from a block
func (s *selectStmt) DateTimeS(num uint64, value *[]time.Time) error {
	var (
		val uint32
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Uint32()
		if err != nil {
			return err
		}
		*value = append(*value, time.Unix(int64(val), 0))
	}
	return nil
}

// UUIDS read num of UUID values from a block
func (s *selectStmt) UUIDS(num uint64, value *[][16]byte) error {
	var (
		val [16]byte
		err error
	)
	uuidData := make([]byte, 16)
	for i := uint64(0); i < num; i++ {
		_, err = s.conn.reader.Read(uuidData)
		if err != nil {
			return err
		}
		copy(val[:], swapUUID(uuidData))
		*value = append(*value, val)
	}
	return nil
}

// IPv4S read num of IPv4 values from a block
func (s *selectStmt) IPv4S(num uint64, value *[]net.IP) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.FixedString(4)
		if err != nil {
			return err
		}
		*value = append(*value, net.IPv4(val[3], val[2], val[1], val[0]).To4())
	}
	return nil
}

// IPv6S read num of IPv6 values from a block
func (s *selectStmt) IPv6S(num uint64, value *[]net.IP) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.FixedString(16)
		if err != nil {
			return err
		}
		*value = append(*value, net.IP(val))
	}
	return nil
}

// LenS Read num of len of Array
func (s *selectStmt) LenS(num uint64, value *[]int) (lastOffset uint64, err error) {
	var (
		val int
	)
	for i := uint64(0); i < num; i++ {
		val, lastOffset, err = s.conn.reader.Len()
		if err != nil {
			return 0, err
		}
		*value = append(*value, val)
	}
	s.conn.reader.ResetOffset()
	return lastOffset, nil
}

// Int8All read all Int8 values from a block
func (s *selectStmt) Int8All(value *[]int8) error {
	return s.Int8S(s.block.NumRows, value)
}

// Int16All read all Int16 values from a block
func (s *selectStmt) Int16All(value *[]int16) error {
	return s.Int16S(s.block.NumRows, value)
}

// Int32All read all Int32 values from a block
func (s *selectStmt) Int32All(value *[]int32) error {
	return s.Int32S(s.block.NumRows, value)
}

// Decimal32All read all Decimal32 values from a block

func (s *selectStmt) Decimal32All(value *[]float64, scale int) error {
	return s.Decimal32S(s.block.NumRows, value, scale)
}

// Decimal64All read all Decimal64 values from a block
func (s *selectStmt) Decimal64All(value *[]float64, scale int) error {
	return s.Decimal64S(s.block.NumRows, value, scale)
}

// Int64All read all Int64 values from a block
func (s *selectStmt) Int64All(value *[]int64) error {
	return s.Int64S(s.block.NumRows, value)
}

// Uint8All read all Uint8 values from a block
func (s *selectStmt) Uint8All(value *[]uint8) error {
	return s.Uint8S(s.block.NumRows, value)
}

// Uint16All read all Uint16 values from a block
func (s *selectStmt) Uint16All(value *[]uint16) error {
	return s.Uint16S(s.block.NumRows, value)
}

// Uint32All read all Uint32 values from a block
func (s *selectStmt) Uint32All(value *[]uint32) error {
	return s.Uint32S(s.block.NumRows, value)
}

// Uint64All read all Uint64 values from a block
func (s *selectStmt) Uint64All(value *[]uint64) error {
	return s.Uint64S(s.block.NumRows, value)
}

// Float32All read all Float32 values from a block
func (s *selectStmt) Float32All(value *[]float32) error {
	return s.Float32S(s.block.NumRows, value)
}

// Float64All read all Float64 values from a block
func (s *selectStmt) Float64All(value *[]float64) error {
	return s.Float64S(s.block.NumRows, value)
}

// StringAll read all String values from a block
func (s *selectStmt) StringAll(value *[]string) error {
	return s.StringS(s.block.NumRows, value)
}

// ByteArrayAll read all ByteArray values from a block
func (s *selectStmt) ByteArrayAll(value *[][]byte) error {
	return s.ByteArrayS(s.block.NumRows, value)
}

// FixedStringAll read all FixedString values from a block
func (s *selectStmt) FixedStringAll(value *[][]byte, strlen int) error {
	return s.FixedStringS(s.block.NumRows, value, strlen)
}

// DateAll read all Date values from a block
func (s *selectStmt) DateAll(value *[]time.Time) error {
	return s.DateS(s.block.NumRows, value)
}

// DateTimeAll read all DateTime values from a block
func (s *selectStmt) DateTimeAll(value *[]time.Time) error {
	return s.DateTimeS(s.block.NumRows, value)
}

// UUIDAll read all UUID values from a block
func (s *selectStmt) UUIDAll(value *[][16]byte) error {
	return s.UUIDS(s.block.NumRows, value)
}

// IPv4All read all IPv4 values from a block
func (s *selectStmt) IPv4All(value *[]net.IP) error {
	return s.IPv4S(s.block.NumRows, value)
}

// IPv6All read all IPv6 values from a block
func (s *selectStmt) IPv6All(value *[]net.IP) error {
	return s.IPv6S(s.block.NumRows, value)
}

// LenAll read all Array Len values from a block
func (s *selectStmt) LenAll(value *[]int) (uint64, error) {
	return s.LenS(s.block.NumRows, value)
}

//nolint:dupl // fix it later
// LowCardinalityString read LowCardinality String values from a block
func (s *selectStmt) LowCardinalityString(values *[]string) error {
	serializationType, err := s.conn.reader.Uint64()
	if err != nil {
		return err
	}
	intType := serializationType & 0xf

	dictionarySize, err := s.conn.reader.Uint64()
	if err != nil {
		return err
	}

	dictionary := make([]string, 0, dictionarySize)
	err = s.StringS(dictionarySize, &dictionary)
	if err != nil {
		return err
	}

	indicesSize, err := s.conn.reader.Uint64()
	if err != nil {
		return err
	}

	switch intType {
	case 0:
		var val uint8
		for i := uint64(0); i < indicesSize; i++ {
			val, err = s.conn.reader.Uint8()
			if err != nil {
				return err
			}
			*values = append(*values, dictionary[int(val)])
		}
	case 1:
		var val uint16
		for i := uint64(0); i < indicesSize; i++ {
			val, err = s.conn.reader.Uint16()
			if err != nil {
				return err
			}
			*values = append(*values, dictionary[int(val)])
		}
	case 2:
		var val uint32
		for i := uint64(0); i < indicesSize; i++ {
			val, err = s.conn.reader.Uint32()
			if err != nil {
				return err
			}
			*values = append(*values, dictionary[int(val)])
		}
	case 3:
		var val uint64
		for i := uint64(0); i < indicesSize; i++ {
			val, err = s.conn.reader.Uint64()
			if err != nil {
				return err
			}
			*values = append(*values, dictionary[int(val)])
		}
	}
	return nil
}

//nolint:dupl // fix it later
// LowCardinalityFixedString read LowCardinality Fixed String values from a block
func (s *selectStmt) LowCardinalityFixedString(values *[][]byte, strlne int) error {
	serializationType, err := s.conn.reader.Uint64()
	if err != nil {
		return err
	}
	intType := serializationType & 0xf

	dictionarySize, err := s.conn.reader.Uint64()
	if err != nil {
		return err
	}

	dictionary := make([][]byte, 0, dictionarySize)
	err = s.FixedStringS(dictionarySize, &dictionary, strlne)
	if err != nil {
		return err
	}

	indicesSize, err := s.conn.reader.Uint64()
	if err != nil {
		return err
	}

	switch intType {
	case 0:
		var val uint8
		for i := uint64(0); i < indicesSize; i++ {
			val, err = s.conn.reader.Uint8()
			if err != nil {
				return err
			}
			*values = append(*values, dictionary[int(val)])
		}
	case 1:
		var val uint16
		for i := uint64(0); i < indicesSize; i++ {
			val, err = s.conn.reader.Uint16()
			if err != nil {
				return err
			}
			*values = append(*values, dictionary[int(val)])
		}
	case 2:
		var val uint32
		for i := uint64(0); i < indicesSize; i++ {
			val, err = s.conn.reader.Uint32()
			if err != nil {
				return err
			}
			*values = append(*values, dictionary[int(val)])
		}
	case 3:
		var val uint64
		for i := uint64(0); i < indicesSize; i++ {
			val, err = s.conn.reader.Uint64()
			if err != nil {
				return err
			}
			*values = append(*values, dictionary[int(val)])
		}
	}
	return nil
}
