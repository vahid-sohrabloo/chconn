package chconn

import (
	"net"
	"time"
)

type SelectStmt struct {
	Block       *Block
	conn        *Conn
	query       string
	queryID     string
	stage       QueryProcessingStage
	settings    *Setting
	clientInfo  *ClientInfo
	LastErr     error
	ProfileInfo *Profile
	Progress    *Progress
}

func (s *SelectStmt) Next() bool {
	res, err := s.conn.ReciveAndProccessData()
	if err != nil {
		// todo wrap this error
		s.LastErr = err
		return false
	}

	if block, ok := res.(*Block); ok {
		if block.NumRows == 0 {
			err = block.readColumns(s.conn)
			if err != nil {
				s.LastErr = err
				return false
			}
			return s.Next()
		}
		s.Block = block
		return true
	}

	if profile, ok := res.(*Profile); ok {
		s.ProfileInfo = profile
		return s.Next()
	}
	if progress, ok := res.(*Progress); ok {
		s.Progress = progress
		return s.Next()
	}
	if _, ok := res.(ServerInfo); ok {
		return s.Next()
	}
	if res == nil {
		return false
	}

	return false
}

func (s *SelectStmt) Close() {
	s.conn.unlock()
}

func (s *SelectStmt) NextColumn() (*Column, error) {
	return s.Block.NextColumn(s.conn)
}

func (s *SelectStmt) Int8(value *[]int8) error {
	var (
		val int8
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Int8()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil

}

func (s *SelectStmt) Int16(value *[]int16) error {
	var (
		val int16
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Int16()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil

}

func (s *SelectStmt) Int32(value *[]int32) error {
	var (
		val int32
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Int32()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil

}

func (s *SelectStmt) Int64(value *[]int64) error {
	var (
		val int64
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Int64()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil

}

func (s *SelectStmt) Uint8(value *[]uint8) error {
	var (
		val uint8
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Uint8()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

func (s *SelectStmt) Uint16(value *[]uint16) error {
	var (
		val uint16
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Uint16()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

func (s *SelectStmt) Uint32(value *[]uint32) error {
	var (
		val uint32
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Uint32()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

func (s *SelectStmt) Uint64(value *[]uint64) error {
	var (
		val uint64
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Uint64()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

func (s *SelectStmt) Float32(value *[]float32) error {
	var (
		val float32
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Float32()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

func (s *SelectStmt) Float64(value *[]float64) error {
	var (
		val float64
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Float64()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

func (s *SelectStmt) String(value *[]string) error {
	var (
		val string
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.String()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

func (s *SelectStmt) ByteArray(value *[][]byte) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.ByteArray()
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

func (s *SelectStmt) FixedString(len int, value *[][]byte) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.FixedString(len)
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

func (s *SelectStmt) Date(value *[]time.Time) error {
	var (
		val int16
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Int16()
		if err != nil {
			return err
		}
		*value = append(*value, time.Unix(int64(val)*24*3600, 0))
	}
	return nil
}

func (s *SelectStmt) DateInServerTimezone(value *[]time.Time) error {
	var (
		val int16
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Int16()
		if err != nil {
			return err
		}
		*value = append(*value, time.Unix(int64(val)*24*3600, 0).In(s.conn.ServerInfo.Timezone))
	}
	return nil
}

func (s *SelectStmt) DateTime(len int, value *[]time.Time) error {
	var (
		val int16
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Int16()
		if err != nil {
			return err
		}
		*value = append(*value, time.Unix(int64(val)*24*3600, 0))
	}
	return nil
}

func (s *SelectStmt) DateTimeInServerTimezone(len int, value *[]time.Time) error {
	var (
		val uint32
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.Uint32()
		if err != nil {
			return err
		}
		*value = append(*value, time.Unix(int64(val), 0).In(s.conn.ServerInfo.Timezone))
	}
	return nil
}

func (s *SelectStmt) UUID(len int, value *[][16]byte) error {
	var (
		val [16]byte
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		_, err = s.conn.reader.Read(val[:])
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}

func (s *SelectStmt) IPv4(len int, value *[]net.IP) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.FixedString(4)
		if err != nil {
			return err
		}
		*value = append(*value, net.IPv4(val[3], val[2], val[1], val[0]))
	}
	return nil
}

func (s *SelectStmt) IPv6(len int, value *[]net.IP) error {
	var (
		val []byte
		err error
	)
	for i := uint64(0); i < s.Block.NumRows; i++ {
		val, err = s.conn.reader.FixedString(16)
		if err != nil {
			return err
		}
		*value = append(*value, net.IP(val))
	}
	return nil
}
