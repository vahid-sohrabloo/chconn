package chconn

import (
	"net"
	"time"
)

func (s *SelectStmt) Int8PS(num uint64, nulls []uint8, value *[]*int8) error {

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

func (s *SelectStmt) Int16PS(num uint64, nulls []uint8, value *[]*int16) error {

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

func (s *SelectStmt) Int32PS(num uint64, nulls []uint8, value *[]*int32) error {

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

func (s *SelectStmt) Decimal32PS(num uint64, nulls []uint8, value *[]*float64, scale int) error {

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

func (s *SelectStmt) Decimal64PS(num uint64, nulls []uint8, value *[]*float64, scale int) error {

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

func (s *SelectStmt) Int64PS(num uint64, nulls []uint8, value *[]*int64) error {

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

func (s *SelectStmt) Uint8PS(num uint64, nulls []uint8, value *[]*uint8) error {

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

func (s *SelectStmt) Uint16PS(num uint64, nulls []uint8, value *[]*uint16) error {

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

func (s *SelectStmt) Uint32PS(num uint64, nulls []uint8, value *[]*uint32) error {

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

func (s *SelectStmt) Uint64PS(num uint64, nulls []uint8, value *[]*uint64) error {

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

func (s *SelectStmt) Float32PS(num uint64, nulls []uint8, value *[]*float32) error {

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

func (s *SelectStmt) Float64PS(num uint64, nulls []uint8, value *[]*float64) error {

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

func (s *SelectStmt) StringPS(num uint64, nulls []uint8, value *[]*string) error {

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

func (s *SelectStmt) ByteArrayPS(num uint64, nulls []uint8, value *[][]byte) error {

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

func (s *SelectStmt) FixedStringPS(num uint64, nulls []uint8, len int, value *[][]byte) error {

	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.FixedString(len)
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

func (s *SelectStmt) DatePS(num uint64, nulls []uint8, value *[]*time.Time) error {

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

		*value = append(*value)
	}
	return nil
}

func (s *SelectStmt) DateInServerTimezonePS(num uint64, nulls []uint8, value *[]*time.Time) error {

	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Int16()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			timeVal := time.Unix(int64(val)*24*3600, 0).In(s.conn.ServerInfo.Timezone)
			*value = append(*value, &timeVal)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *SelectStmt) DateTimePS(num uint64, nulls []uint8, value *[]*time.Time) error {

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

func (s *SelectStmt) DateTimesInServerTimezonePS(num uint64, nulls []uint8, value *[]*time.Time) error {

	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.Uint32()
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			timeVal := time.Unix(int64(val), 0).In(s.conn.ServerInfo.Timezone)
			*value = append(*value, &timeVal)
		} else {
			*value = append(*value, nil)
		}
	}
	return nil
}

func (s *SelectStmt) UUIDPS(num uint64, nulls []uint8, value *[]*[16]byte) error {
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

func (s *SelectStmt) IPv4PS(num uint64, nulls []uint8, value *[]*net.IP) error {

	for i := uint64(0); i < num; i++ {
		val, err := s.conn.reader.FixedString(4)
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			ip := net.IPv4(val[3], val[2], val[1], val[0])
			*value = append(*value, &ip)
		} else {
			*value = append(*value, nil)
		}
		*value = append(*value)
	}
	return nil
}

func (s *SelectStmt) IPv6PS(num uint64, nulls []uint8, value *[]*net.IP) error {

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

func (s *SelectStmt) Int8PAll(value *[]*int8) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Int8PS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) Int16PAll(value *[]*int16) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Int16PS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) Int32PAll(value *[]*int32) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Int32PS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) Decimal32PAll(value *[]*float64, scale int) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Decimal32PS(s.Block.NumRows, nulls, value, scale)
}
func (s *SelectStmt) Decimal64PAll(value *[]*float64, scale int) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Decimal64PS(s.Block.NumRows, nulls, value, scale)
}
func (s *SelectStmt) Int64PAll(value *[]*int64) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Int64PS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) Uint8PAll(value *[]*uint8) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Uint8PS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) Uint16PAll(value *[]*uint16) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Uint16PS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) Uint32PAll(value *[]*uint32) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Uint32PS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) Uint64PAll(value *[]*uint64) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Uint64PS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) Float32PAll(value *[]*float32) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Float32PS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) Float64PAll(value *[]*float64) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.Float64PS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) StringPAll(value *[]*string) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.StringPS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) ByteArrayPAll(value *[][]byte) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.ByteArrayPS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) FixedStringPAll(len int, value *[][]byte) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.FixedStringPS(s.Block.NumRows, nulls, len, value)
}
func (s *SelectStmt) DatePAll(value *[]*time.Time) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.DatePS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) DateInServerTimezonePAll(value *[]*time.Time) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.DateInServerTimezonePS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) DateTimePAll(value *[]*time.Time) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.DateTimePS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) DateTimesInServerTimezonePAll(value *[]*time.Time) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.DateTimesInServerTimezonePS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) UUIDPAll(value *[]*[16]byte) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.UUIDPS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) IPv4PAll(value *[]*net.IP) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.IPv4PS(s.Block.NumRows, nulls, value)
}
func (s *SelectStmt) IPv6PAll(value *[]*net.IP) error {
	nulls, err := s.GetNullS(s.Block.NumRows)
	if err != nil {
		return err
	}
	return s.IPv6PS(s.Block.NumRows, nulls, value)
}

func (s *SelectStmt) GetNullS(num uint64) ([]uint8, error) {
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

func (s *SelectStmt) GetNullSAll() ([]uint8, error) {
	return s.GetNullS(s.Block.NumRows)
}
