package chconn

import (
	"net"
	"time"
)

func (s *selectStmt) Int8PS(num uint64, nulls []uint8, value *[]*int8) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Int8()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Int16PS(num uint64, nulls []uint8, value *[]*int16) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Int16()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Int32PS(num uint64, nulls []uint8, value *[]*int32) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Int32()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Decimal32PS(num uint64, nulls []uint8, value *[]*float64, scale int) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Int32()
		if err != nil {
			return err
		}
		floatVal := float64(val) / factors10[scale]
		if nulls[i] == 0 {
			*value = append(*value, &floatVal)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Decimal64PS(num uint64, nulls []uint8, value *[]*float64, scale int) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Int64()
		if err != nil {
			return err
		}
		floatVal := float64(val) / factors10[scale]
		if nulls[i] == 0 {
			*value = append(*value, &floatVal)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Int64PS(num uint64, nulls []uint8, value *[]*int64) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Int64()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Uint8PS(num uint64, nulls []uint8, value *[]*uint8) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Uint8()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Uint16PS(num uint64, nulls []uint8, value *[]*uint16) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Uint16()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Uint32PS(num uint64, nulls []uint8, value *[]*uint32) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Uint32()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Uint64PS(num uint64, nulls []uint8, value *[]*uint64) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Uint64()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Float32PS(num uint64, nulls []uint8, value *[]*float32) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Float32()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Float64PS(num uint64, nulls []uint8, value *[]*float64) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Float64()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) StringPS(num uint64, nulls []uint8, value *[]*string) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.String()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) ByteArrayPS(num uint64, nulls []uint8, value *[][]byte) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.ByteArray()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) FixedStringPS(num uint64, nulls []uint8, value *[][]byte, strlen int) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.FixedString(strlen)
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			*value = append(*value, val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) DatePS(num uint64, nulls []uint8, value *[]*time.Time) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Int16()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			timeVal := time.Unix(int64(val)*24*3600, 0)
			*value = append(*value, &timeVal)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) DateTimePS(num uint64, nulls []uint8, value *[]*time.Time) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Uint32()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			timeVal := time.Unix(int64(val), 0)
			*value = append(*value, &timeVal)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) UUIDPS(num uint64, nulls []uint8, value *[]*[16]byte) error {
	var err error
	uuidData := make([]byte, 16)
	for i := uint64(0); i < num; i++ {
		_, err = s.conn.reader.Read(uuidData)
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			var val [16]byte
			copy(val[:], swapUUID(uuidData))
			*value = append(*value, &val)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) IPv4PS(num uint64, nulls []uint8, value *[]*net.IP) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.FixedString(4)
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			ip := net.IPv4(val[3], val[2], val[1], val[0]).To4()
			*value = append(*value, &ip)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) IPv6PS(num uint64, nulls []uint8, value *[]*net.IP) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.FixedString(16)
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			ip := net.IP(val)
			*value = append(*value, &ip)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *selectStmt) Int8PAll(value *[]*int8) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Int8PS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) Int16PAll(value *[]*int16) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Int16PS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) Int32PAll(value *[]*int32) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Int32PS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) Decimal32PAll(value *[]*float64, scale int) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Decimal32PS(s.block.NumRows, nulls, value, scale)
}
func (s *selectStmt) Decimal64PAll(value *[]*float64, scale int) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Decimal64PS(s.block.NumRows, nulls, value, scale)
}
func (s *selectStmt) Int64PAll(value *[]*int64) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Int64PS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) Uint8PAll(value *[]*uint8) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Uint8PS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) Uint16PAll(value *[]*uint16) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Uint16PS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) Uint32PAll(value *[]*uint32) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Uint32PS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) Uint64PAll(value *[]*uint64) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Uint64PS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) Float32PAll(value *[]*float32) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Float32PS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) Float64PAll(value *[]*float64) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.Float64PS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) StringPAll(value *[]*string) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.StringPS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) ByteArrayPAll(value *[][]byte) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.ByteArrayPS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) FixedStringPAll(value *[][]byte, strlen int) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.FixedStringPS(s.block.NumRows, nulls, value, strlen)
}
func (s *selectStmt) DatePAll(value *[]*time.Time) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.DatePS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) DateTimePAll(value *[]*time.Time) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.DateTimePS(s.block.NumRows, nulls, value)
}

func (s *selectStmt) UUIDPAll(value *[]*[16]byte) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.UUIDPS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) IPv4PAll(value *[]*net.IP) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.IPv4PS(s.block.NumRows, nulls, value)
}
func (s *selectStmt) IPv6PAll(value *[]*net.IP) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt PALL: read nulls", err}
	}
	return s.IPv6PS(s.block.NumRows, nulls, value)
}

func (s *selectStmt) GetNullS(num uint64) ([]uint8, error) {
	if int(num) > cap(s.nulls) {
		s.nulls = make([]uint8, 0, num)
	}
	s.nulls = s.nulls[:0]
	err := s.Uint8S(num, &s.nulls)
	if err != nil {
		return nil, err
	}
	return s.nulls[:num], nil
}

func (s *selectStmt) GetNullSAll() ([]uint8, error) {
	return s.GetNullS(s.block.NumRows)
}
