package chconn

import (
	"time"
	"net"
)

func (s *SelectStmt) Int8S(num uint64, value *[]int8) error {
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

func (s *SelectStmt) Int16S(num uint64, value *[]int16) error {
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

func (s *SelectStmt) Int32S(num uint64, value *[]int32) error {
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

func (s *SelectStmt) Decimal32S(num uint64, value *[]float64, scale int) error {
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

func (s *SelectStmt) Decimal64S(num uint64, value *[]float64, scale int) error {
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

func (s *SelectStmt) Int64S(num uint64, value *[]int64) error {
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

func (s *SelectStmt) Uint8S(num uint64, value *[]uint8) error {
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

func (s *SelectStmt) Uint16S(num uint64, value *[]uint16) error {
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

func (s *SelectStmt) Uint32S(num uint64, value *[]uint32) error {
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

func (s *SelectStmt) Uint64S(num uint64, value *[]uint64) error {
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

func (s *SelectStmt) Float32S(num uint64, value *[]float32) error {
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

func (s *SelectStmt) Float64S(num uint64, value *[]float64) error {
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

func (s *SelectStmt) StringS(num uint64, value *[]string) error {
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

func (s *SelectStmt) ByteArrayS(num uint64, value *[][]byte) error {
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

func (s *SelectStmt) FixedStringS(num uint64, len int, value *[][]byte) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.FixedString(len)
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

func (s *SelectStmt) DateS(num uint64, value *[]time.Time) error {
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

func (s *SelectStmt) DateInServerTimezoneS(num uint64, value *[]time.Time) error {
	var (
		val int16
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Int16()
		if err != nil {
			return err
		}
		*value = append(*value, time.Unix(int64(val)*24*3600, 0).In(s.conn.ServerInfo.Timezone))
	}
	return nil
}

func (s *SelectStmt) DateTimeS(num uint64, value *[]time.Time) error {
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

func (s *SelectStmt) DateTimesInServerTimezoneS(num uint64, value *[]time.Time) error {
	var (
		val uint32
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.Uint32()
		if err != nil {
			return err
		}
		*value = append(*value, time.Unix(int64(val), 0).In(s.conn.ServerInfo.Timezone))
	}
	return nil
}

func (s *SelectStmt) UUIDS(num uint64, value *[][16]byte) error {
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

func (s *SelectStmt) IPv4S(num uint64, value *[]net.IP) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.conn.reader.FixedString(4)
		if err != nil {
			return err
		}
		*value = append(*value, net.IPv4(val[3], val[2], val[1], val[0]))
	}
	return nil
}

func (s *SelectStmt) IPv6S(num uint64, value *[]net.IP) error {
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

func (s *SelectStmt) LenS(num uint64, value *[]int) (lastOffset uint64, err error) {
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

func (s *SelectStmt) Int8All(value *[]int8) error {
	return s.Int8S(s.Block.NumRows, value)
}
func (s *SelectStmt) Int16All(value *[]int16) error {
	return s.Int16S(s.Block.NumRows, value)
}
func (s *SelectStmt) Int32All(value *[]int32) error {
	return s.Int32S(s.Block.NumRows, value)
}

func (s *SelectStmt) Decimal32All(value *[]float64, scale int) error {
	return s.Decimal32S(s.Block.NumRows, value, scale)
}

func (s *SelectStmt) Decimal64All(value *[]float64, scale int) error {
	return s.Decimal64S(s.Block.NumRows, value, scale)
}

func (s *SelectStmt) Int64All(value *[]int64) error {
	return s.Int64S(s.Block.NumRows, value)
}
func (s *SelectStmt) Uint8All(value *[]uint8) error {
	return s.Uint8S(s.Block.NumRows, value)
}
func (s *SelectStmt) Uint16All(value *[]uint16) error {
	return s.Uint16S(s.Block.NumRows, value)
}
func (s *SelectStmt) Uint32All(value *[]uint32) error {
	return s.Uint32S(s.Block.NumRows, value)
}
func (s *SelectStmt) Uint64All(value *[]uint64) error {
	return s.Uint64S(s.Block.NumRows, value)
}
func (s *SelectStmt) Float32All(value *[]float32) error {
	return s.Float32S(s.Block.NumRows, value)
}
func (s *SelectStmt) Float64All(value *[]float64) error {
	return s.Float64S(s.Block.NumRows, value)
}
func (s *SelectStmt) StringAll(value *[]string) error {
	return s.StringS(s.Block.NumRows, value)
}
func (s *SelectStmt) ByteArrayAll(value *[][]byte) error {
	return s.ByteArrayS(s.Block.NumRows, value)
}
func (s *SelectStmt) FixedStringAll(len int, value *[][]byte) error {
	return s.FixedStringS(s.Block.NumRows, len, value)
}
func (s *SelectStmt) DateAll(value *[]time.Time) error {
	return s.DateS(s.Block.NumRows, value)
}
func (s *SelectStmt) DateInServerTimezoneAll(value *[]time.Time) error {
	return s.DateInServerTimezoneS(s.Block.NumRows, value)
}
func (s *SelectStmt) DateTimeAll(value *[]time.Time) error {
	return s.DateTimeS(s.Block.NumRows, value)
}
func (s *SelectStmt) DateTimeInServerTimezoneAll(value *[]time.Time) error {
	return s.DateTimesInServerTimezoneS(s.Block.NumRows, value)
}
func (s *SelectStmt) UUIDAll(value *[][16]byte) error {
	return s.UUIDS(s.Block.NumRows, value)
}
func (s *SelectStmt) IPv4All(value *[]net.IP) error {
	return s.IPv4S(s.Block.NumRows, value)
}
func (s *SelectStmt) IPv6All(value *[]net.IP) error {
	return s.IPv6S(s.Block.NumRows, value)
}
func (s *SelectStmt) LenAll(value *[]int) (uint64, error) {
	return s.LenS(s.Block.NumRows, value)
}