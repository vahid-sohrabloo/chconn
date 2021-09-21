// Code generated. DO NOT EDIT
package chconn

import (
	"fmt"
	"net"
	"time"
)

const (
	ChTestSelectRowEnum8Hello int8 = 1
	ChTestSelectRowEnum8World int8 = 2
)
const (
	ChTestSelectRowEnum16Hello int16 = 1
	ChTestSelectRowEnum16World int16 = 2
)

func GetSelectChTestSelectRowQuery(tableName string) string {
	return fmt.Sprintf(`Select int8,
int16,
int32,
int64,
uint8,
uint16,
uint32,
uint64,
float32,
float64,
string,
string2,
fString,
array,
date,
datetime,
datetime64,
decimal32,
decimal64,
uuid,
tuple,
ipv4,
ipv6,
enum8,
enum16 FROM %s`, tableName)
}

func GetInsertChTestSelectRowQuery(tableName string) string {
	return fmt.Sprintf(`INSERT INTO %s (int8,
int16,
int32,
int64,
uint8,
uint16,
uint32,
uint64,
float32,
float64,
string,
string2,
fString,
array,
date,
datetime,
datetime64,
decimal32,
decimal64,
uuid,
tuple,
ipv4,
ipv6,
enum8,
enum16) VALUES `, tableName)
}

type ChTestSelectRow struct {
	Int8       int8
	Int16      int16
	Int32      int32
	Int64      int64
	Uint8      uint8
	Uint16     uint16
	Uint32     uint32
	Uint64     uint64
	Float32    float32
	Float64    float64
	String     string
	String2    string
	FString    []byte
	Array      []uint8
	Date       time.Time
	Datetime   time.Time
	Datetime64 time.Time
	Decimal32  float64
	Decimal64  float64
	Uuid       [16]byte
	Tuple      struct {
		Field1 uint8
		Field2 string
	}
	Ipv4   net.IP
	Ipv6   net.IP
	Enum8  int8
	Enum16 int16
}

// write method
func (t *ChTestSelectRow) Write(writer *InsertWriter) error {
	var err error
	writer.AddRow(1)
	// column int8
	writer.Int8(0, t.Int8)

	// column int16
	writer.Int16(1, t.Int16)

	// column int32
	writer.Int32(2, t.Int32)

	// column int64
	writer.Int64(3, t.Int64)

	// column uint8
	writer.Uint8(4, t.Uint8)

	// column uint16
	writer.Uint16(5, t.Uint16)

	// column uint32
	writer.Uint32(6, t.Uint32)

	// column uint64
	writer.Uint64(7, t.Uint64)

	// column float32
	writer.Float32(8, t.Float32)

	// column float64
	writer.Float64(9, t.Float64)

	// column string
	writer.String(10, t.String)

	// column string2
	writer.String(11, t.String2)

	// column fString
	if len(t.FString) != 2 {
		return fmt.Errorf("len of t.FString should be 2 not %d", len(t.FString))
	}
	writer.FixedString(12, t.FString)

	// column array
	writer.AddLen(13, uint64(len(t.Array)))

	for _, f := range t.Array {
		writer.Uint8(14, f)

	}

	// column date
	writer.Date(15, t.Date)

	// column datetime
	writer.DateTime(16, t.Datetime)

	// column datetime64
	writer.DateTime64(17, 9, t.Datetime64)

	// column decimal32
	writer.Decimal32(18, t.Decimal32, 4)

	// column decimal64
	writer.Decimal64(19, t.Decimal64, 4)

	// column uuid
	writer.UUID(20, t.Uuid)

	// column tuple
	writer.Uint8(21, t.Tuple.Field1)
	writer.String(22, t.Tuple.Field2)

	// column ipv4
	err = writer.IPv4(23, t.Ipv4)
	if err != nil {
		return err
	}

	// column ipv6
	err = writer.IPv6(24, t.Ipv6)
	if err != nil {
		return err
	}

	// column enum8
	writer.Int8(25, t.Enum8)

	// column enum16
	writer.Int16(26, t.Enum16)

	return nil
}

// read method
func ReadChTestSelectRow(stmt SelectStmt) ([]*ChTestSelectRow, error) {
	rows := make([]*ChTestSelectRow, 0)
	var err error
	var rowOffset int
	for stmt.Next() {
		rowsInBlock := int(stmt.RowsInBlock())
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows = append(rows, NewChTestSelectRow())
		}

		// column int8 (Int8)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Int8, err = stmt.Int8()
			if err != nil {
				return nil, err
			}
		}

		// column int16 (Int16)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Int16, err = stmt.Int16()
			if err != nil {
				return nil, err
			}
		}

		// column int32 (Int32)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Int32, err = stmt.Int32()
			if err != nil {
				return nil, err
			}
		}

		// column int64 (Int64)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Int64, err = stmt.Int64()
			if err != nil {
				return nil, err
			}
		}

		// column uint8 (UInt8)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Uint8, err = stmt.Uint8()
			if err != nil {
				return nil, err
			}
		}

		// column uint16 (UInt16)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Uint16, err = stmt.Uint16()
			if err != nil {
				return nil, err
			}
		}

		// column uint32 (UInt32)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Uint32, err = stmt.Uint32()
			if err != nil {
				return nil, err
			}
		}

		// column uint64 (UInt64)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Uint64, err = stmt.Uint64()
			if err != nil {
				return nil, err
			}
		}

		// column float32 (Float32)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Float32, err = stmt.Float32()
			if err != nil {
				return nil, err
			}
		}

		// column float64 (Float64)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Float64, err = stmt.Float64()
			if err != nil {
				return nil, err
			}
		}

		// column string (String)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].String, err = stmt.String()
			if err != nil {
				return nil, err
			}
		}

		// column string2 (String)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].String2, err = stmt.String()
			if err != nil {
				return nil, err
			}
		}

		// column fString (FixedString(2))
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].FString, err = stmt.FixedString(2)
			if err != nil {
				return nil, err
			}
		}

		// column array (Array(UInt8))
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		// get array lens
		lenArray := make([]int, 0, rowsInBlock)
		lastOffset, err := stmt.LenS(uint64(rowsInBlock), &lenArray)
		if err != nil {
			return nil, err
		}
		_ = lastOffset
		for i0, l := range lenArray {
			rows[rowOffset+i0].Array = make([]uint8, l)
			for i1 := 0; i1 < l; i1++ {
				rows[rowOffset+i0].Array[i1], err = stmt.Uint8()
				if err != nil {
					return nil, err
				}
			}

		}
		// column date (Date)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Date, err = stmt.Date()
			if err != nil {
				return nil, err
			}
		}

		// column datetime (DateTime('Iran'))
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Datetime, err = stmt.DateTime()
			if err != nil {
				return nil, err
			}
		}

		// column datetime64 (DateTime64(9, 'Iran'))
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Datetime64, err = stmt.DateTime64(9)
			if err != nil {
				return nil, err
			}
		}

		// column decimal32 (Decimal(9, 4))
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Decimal32, err = stmt.Decimal32(4)
			if err != nil {
				return nil, err
			}
		}

		// column decimal64 (Decimal(18, 4))
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Decimal64, err = stmt.Decimal64(4)
			if err != nil {
				return nil, err
			}
		}

		// column uuid (UUID)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Uuid, err = stmt.UUID()
			if err != nil {
				return nil, err
			}
		}

		// column tuple (Tuple(UInt8, String))
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Tuple.Field1, err = stmt.Uint8()
			if err != nil {
				return nil, err
			}
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Tuple.Field2, err = stmt.String()
			if err != nil {
				return nil, err
			}
		}

		// column ipv4 (IPv4)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Ipv4, err = stmt.IPv4()
			if err != nil {
				return nil, err
			}
		}

		// column ipv6 (IPv6)
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Ipv6, err = stmt.IPv6()
			if err != nil {
				return nil, err
			}
		}

		// column enum8 (Enum8('hello' = 1, 'world' = 2))
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Enum8, err = stmt.Int8()
			if err != nil {
				return nil, err
			}
		}

		// column enum16 (Enum16('hello' = 1, 'world' = 2))
		_, err = stmt.NextColumn()
		if err != nil {
			return nil, err
		}
		for i0 := 0; i0 < rowsInBlock; i0++ {
			rows[rowOffset+i0].Enum16, err = stmt.Int16()
			if err != nil {
				return nil, err
			}
		}

		rowOffset += rowsInBlock
	}
	return rows, nil
}

func NewChTestSelectRow() *ChTestSelectRow {
	return &ChTestSelectRow{}
}

func NewChTestSelectRowWriter() *InsertWriter {
	return NewInsertWriter(27)
}
