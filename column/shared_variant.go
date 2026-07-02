package column

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"strconv"
	"time"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
)

// SharedVariant is a column that is used for dynamic columns
type SharedVariant struct {
	StringBase[string]
}

// NewSharedVariant creates a new [SharedVariant] column for use inside [Dynamic] columns.
func NewSharedVariant() *SharedVariant {
	return &SharedVariant{}
}

func (c *SharedVariant) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	// implement this
	return nil
}

func (c *SharedVariant) chconnType() string {
	return "column.SharedVariant"
}

func (c *SharedVariant) structType() string {
	return helper.SharedVariantStr
}

func (c *SharedVariant) Scan(row int, dest any) error {
	switch d := dest.(type) {
	case *any:
		*d = c.Row(row)
		return nil
	case sql.Scanner:
		return d.Scan(c.Row(row))
	}

	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "*" + reflect.TypeOf(c.Row(row)).String(),
	}
}

func (c *SharedVariant) ScanValue(row int, value reflect.Value) error {
	if value.Kind() != reflect.Ptr {
		return fmt.Errorf("scan dest should be a pointer")
	}

	val := value.Elem()
	rowData := c.Row(row)
	rowVal := reflect.ValueOf(rowData)
	if val.Type().AssignableTo(rowVal.Type()) {
		val.Set(rowVal)
		return nil
	}
	if val.Kind() == reflect.Interface {
		if value.NumMethod() == 0 {
			val.Set(reflect.ValueOf(rowData))
			return nil
		}
	}
	return fmt.Errorf("cannot scan text into %s", val.Type().String())
}

func (c *SharedVariant) Row(row int) any {
	return c.RowAny(row)
}

func (c *SharedVariant) RowAny(row int) any {
	data := c.RowBytes(row)
	if len(data) == 0 {
		return nil
	}
	bType, data := c.readSharedTypes(data)
	ret, _ := c.readValue(bType, data)
	return ret
}

type binaryBaseType struct {
	bType      helper.BinaryTypeIndex
	childTypes []binaryBaseType
}

func (c *SharedVariant) readSharedTypes(data []byte) (bType binaryBaseType, retData []byte) {
	bType = binaryBaseType{
		bType:      helper.BinaryTypeIndex(data[0]),
		childTypes: make([]binaryBaseType, 0, 64),
	}
	data = data[1:]
	if bType.bType == helper.BinaryTypeIndexArray {
		bType.childTypes = make([]binaryBaseType, 1)
		bType.childTypes[0], data = c.readSharedTypes(data)
	}
	return bType, data
}

//nolint:gocyclo,gocritic
func (c *SharedVariant) readValue(btype binaryBaseType, data []byte) (any, []byte) {
	switch btype.bType {
	case helper.BinaryTypeIndexNothing:
		return nil, data
	case helper.BinaryTypeIndexUInt8:
		return data[0], data[1:]
	case helper.BinaryTypeIndexUInt16:
		return binary.LittleEndian.Uint16(data), data[2:]
	case helper.BinaryTypeIndexUInt32:
		return binary.LittleEndian.Uint32(data), data[4:]
	case helper.BinaryTypeIndexUInt64:
		return binary.LittleEndian.Uint64(data), data[8:]
	case helper.BinaryTypeIndexInt8:
		return int8(data[1]), data[1:]
	case helper.BinaryTypeIndexInt16:
		return int16(binary.LittleEndian.Uint16(data)), data[2:]
	case helper.BinaryTypeIndexInt32:
		return int32(binary.LittleEndian.Uint32(data)), data[4:]
	case helper.BinaryTypeIndexInt64:
		return int64(binary.LittleEndian.Uint64(data)), data[8:]
	case helper.BinaryTypeIndexFloat32:
		bits := binary.LittleEndian.Uint32(data)
		return math.Float32frombits(bits), data[4:]
	case helper.BinaryTypeIndexFloat64:
		bits := binary.LittleEndian.Uint64(data)
		return math.Float64frombits(bits), data[8:]
	case helper.BinaryTypeIndexBFloat16:
		bits := binary.LittleEndian.Uint16(data)
		return math.Float32frombits(uint32(bits) << 16), data[2:]
	case helper.BinaryTypeIndexTime:
		return int32(binary.LittleEndian.Uint32(data)), data[4:]
	case helper.BinaryTypeIndexTime64:
		return int64(binary.LittleEndian.Uint64(data)), data[8:]
	case helper.BinaryTypeIndexString:
		return string(data), data[len(data):]
	case helper.BinaryTypeIndexBool:
		return data[0] != 0, data[1:]
	case helper.BinaryTypeIndexUUID:
		return data[0:16], data[16:]
	case helper.BinaryTypeIndexIPv4:
		return net.IP(data[0:4]), data[4:]
	case helper.BinaryTypeIndexIPv6:
		return net.IP(data[6:16]), data[16:]
	case helper.BinaryTypeIndexDate:
		// todo timezone
		// todo this is not valid date conversion
		return time.Unix(int64(binary.LittleEndian.Uint16(data)), 0).UTC(), data[2:]
	case helper.BinaryTypeIndexDateTimeUTC:
		// todo timezone
		return time.Unix(int64(binary.LittleEndian.Uint32(data)), 0).UTC(), data[4:]
	case helper.BinaryTypeIndexDecimal32, helper.BinaryTypeIndexDecimal64, helper.BinaryTypeIndexDecimal128, helper.BinaryTypeIndexDecimal256:
		// todo
		return data[0:], data[1+len(data[0:]):]
	case helper.BinaryTypeIndexArray:
		lenArray, nRead := binary.Uvarint(data)
		data = data[nRead:]
		ret := make([]any, lenArray)
		for i := range ret {
			ret[i], data = c.readValue(btype.childTypes[0], data)
		}
		return ret, data
	default:
		panic(fmt.Sprintf("unknown type %d", data[0]))
	}
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *SharedVariant) WriteTo(w io.Writer) (int64, error) {
	// make sure string is working for this type
	return c.StringBase.WriteTo(w)
}

func (c *SharedVariant) FullType() string {
	if len(c.columnHeader.Name) == 0 {
		return "SharedVariant"
	}
	return string(c.columnHeader.Name) + " SharedVariant"
}

func (c *SharedVariant) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	data := c.RowBytes(row)
	if len(data) == 0 {
		return append(b, "null"...)
	}
	bType, data := c.readSharedTypes(data)
	b, _ = c.valueToJSON(bType, data, ignoreDoubleQuotes, b)
	return b
}

//nolint:gocyclo,gocritic,funlen
func (c *SharedVariant) valueToJSON(btype binaryBaseType, data []byte, ignoreDoubleQuotes bool, b []byte) ([]byte, []byte) {
	switch btype.bType {
	case helper.BinaryTypeIndexNothing:
		return append(b, "null"...), data
	case helper.BinaryTypeIndexUInt8:
		return strconv.AppendUint(b, uint64(data[0]), 10), data[1:]
	case helper.BinaryTypeIndexUInt16:
		return strconv.AppendUint(b, uint64(binary.LittleEndian.Uint16(data)), 10), data[2:]
	case helper.BinaryTypeIndexUInt32:
		return strconv.AppendUint(b, uint64(binary.LittleEndian.Uint32(data)), 10), data[4:]
	case helper.BinaryTypeIndexUInt64:
		return strconv.AppendUint(b, binary.LittleEndian.Uint64(data), 10), data[8:]
	case helper.BinaryTypeIndexInt8:
		return strconv.AppendInt(b, int64(int8(data[0])), 10), data[1:]
	case helper.BinaryTypeIndexInt16:
		return strconv.AppendInt(b, int64(int16(binary.LittleEndian.Uint16(data))), 10), data[2:]
	case helper.BinaryTypeIndexInt32:
		return strconv.AppendInt(b, int64(int32(binary.LittleEndian.Uint32(data))), 10), data[4:]
	case helper.BinaryTypeIndexInt64:
		return strconv.AppendInt(b, int64(binary.LittleEndian.Uint64(data)), 10), data[8:]
	case helper.BinaryTypeIndexFloat32:
		bits := binary.LittleEndian.Uint32(data)
		return strconv.AppendFloat(b, float64(math.Float32frombits(bits)), 'f', -1, 32), data[4:]
	case helper.BinaryTypeIndexFloat64:
		bits := binary.LittleEndian.Uint64(data)
		return strconv.AppendFloat(b, math.Float64frombits(bits), 'f', -1, 64), data[8:]
	case helper.BinaryTypeIndexBFloat16:
		bits := binary.LittleEndian.Uint16(data)
		f := math.Float32frombits(uint32(bits) << 16)
		return strconv.AppendFloat(b, float64(f), 'f', -1, 32), data[2:]
	case helper.BinaryTypeIndexTime:
		return strconv.AppendInt(b, int64(int32(binary.LittleEndian.Uint32(data))), 10), data[4:]
	case helper.BinaryTypeIndexTime64:
		return strconv.AppendInt(b, int64(binary.LittleEndian.Uint64(data)), 10), data[8:]
	case helper.BinaryTypeIndexString:
		return helper.AppendJSONSting(b, ignoreDoubleQuotes, data), data[len(data):]
	case helper.BinaryTypeIndexBool:
		if data[0] != 0 {
			b = append(b, "true"...)
		} else {
			b = append(b, "false"...)
		}
		return b, data[1:]
	case helper.BinaryTypeIndexArray:
		lenArray, nRead := binary.Uvarint(data)
		data = data[nRead:]
		b = append(b, '[')
		for i := range lenArray {
			if i > 0 {
				b = append(b, ',')
			}
			b, data = c.valueToJSON(btype.childTypes[0], data, ignoreDoubleQuotes, b)
		}
		b = append(b, ']')
		return b, data
	case helper.BinaryTypeIndexUUID:
		// UUID is 16 bytes; output as string for now
		return helper.AppendJSONSting(b, ignoreDoubleQuotes, data[0:16]), data[16:]
	case helper.BinaryTypeIndexIPv4:
		ip := net.IP(data[0:4])
		return helper.AppendJSONSting(b, ignoreDoubleQuotes, []byte(ip.String())), data[4:]
	case helper.BinaryTypeIndexIPv6:
		ip := net.IP(data[6:16])
		return helper.AppendJSONSting(b, ignoreDoubleQuotes, []byte(ip.String())), data[16:]
	case helper.BinaryTypeIndexDate:
		t := time.Unix(int64(binary.LittleEndian.Uint16(data))*86400, 0).UTC()
		return helper.AppendJSONSting(b, ignoreDoubleQuotes, []byte(t.Format("2006-01-02"))), data[2:]
	case helper.BinaryTypeIndexDateTimeUTC:
		t := time.Unix(int64(binary.LittleEndian.Uint32(data)), 0).UTC()
		return helper.AppendJSONSting(b, ignoreDoubleQuotes, []byte(t.Format("2006-01-02 15:04:05"))), data[4:]
	default:
		return append(b, "null"...), data
	}
}
