package column

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/vahid-sohrabloo/chconn/v2/internal/helper"
)

var chColumnByteSize = map[string]int{
	"Int8":       1,
	"Int16":      2,
	"Int32":      4,
	"Int64":      8,
	"Int128":     16,
	"Int256":     32,
	"UInt8":      1,
	"UInt16":     2,
	"UInt32":     4,
	"UInt64":     8,
	"UInt128":    16,
	"UInt256":    32,
	"Float32":    4,
	"Float64":    8,
	"Date":       2,
	"Date32":     4,
	"DateTime":   4,
	"DateTime64": 8,
	"UUID":       16,
	"IPv4":       4,
	"IPv6":       16,
}

var byteChColumnType = map[int]string{
	1:  "Int8|UInt8|Enum8",
	2:  "Int16|UInt16|Enum16|Date",
	4:  "Int32|UInt32|Float32|Decimal32|Date32|DateTime|IPv4",
	8:  "Int64|UInt64|Float64|Decimal64|DateTime64",
	16: "Int128|UInt128|Decimal128|IPv6|UUID",
	32: "Int256|UInt256|Decimal256",
}

func (c *Base[T]) Validate() error {
	chType := helper.FilterSimpleAggregate(c.chType)
	if byteSize, ok := chColumnByteSize[string(chType)]; ok {
		if byteSize != c.size {
			return &ErrInvalidType{
				column: c,
			}
		}
		return nil
	}

	if ok, err := c.checkEnum8(chType); ok {
		return err
	}

	if ok, err := c.checkEnum16(chType); ok {
		return err
	}

	if ok, err := c.checkDateTime(chType); ok {
		return err
	}

	if ok, err := c.checkDateTime(chType); ok {
		return err
	}

	if ok, err := c.checkDateTime64(chType); ok {
		return err
	}
	if ok, err := c.checkFixedString(chType); ok {
		return err
	}
	if ok, err := c.checkDecimal(chType); ok {
		return err
	}

	return &ErrInvalidType{
		column: c,
	}
}

func (c *Base[T]) checkEnum8(chType []byte) (bool, error) {
	if helper.IsEnum8(chType) {
		if c.size != Uint8Size {
			return true, &ErrInvalidType{
				column: c,
			}
		}
		return true, nil
	}
	return false, nil
}

func (c *Base[T]) checkEnum16(chType []byte) (bool, error) {
	if helper.IsEnum16(chType) {
		if c.size != Uint16Size {
			return true, &ErrInvalidType{
				column: c,
			}
		}
		return true, nil
	}
	return false, nil
}

func (c *Base[T]) checkDateTime(chType []byte) (bool, error) {
	if helper.IsDateTimeWithParam(chType) {
		if c.size != 4 {
			return true, &ErrInvalidType{
				column: c,
			}
		}
		c.params = []interface{}{
			// precision
			0,
			// timezone
			chType[helper.DateTimeStrLen : len(chType)-1],
		}
		return true, nil
	}
	return false, nil
}

func (c *Base[T]) checkDateTime64(chType []byte) (bool, error) {
	if helper.IsDateTime64(chType) {
		if c.size != 8 {
			return true, &ErrInvalidType{
				column: c,
			}
		}
		parts := bytes.Split(chType[helper.DecimalStrLen:len(chType)-1], []byte(", "))
		c.params = []interface{}{
			parts[0],
			[]byte{},
		}
		if len(parts) > 1 {
			c.params[1] = parts[1]
		}
		return true, nil
	}
	return false, nil
}

func (c *Base[T]) checkFixedString(chType []byte) (bool, error) {
	if helper.IsFixedString(chType) {
		size, err := strconv.Atoi(string(chType[helper.FixedStringStrLen : len(chType)-1]))
		if err != nil {
			return true, fmt.Errorf("invalid size: %s", err)
		}
		if c.size != size {
			return true, &ErrInvalidType{
				column: c,
			}
		}
		return true, nil
	}
	return false, nil
}

func (c *Base[T]) checkDecimal(chType []byte) (bool, error) {
	if helper.IsDecimal(chType) {
		parts := bytes.Split(chType[helper.DecimalStrLen:len(chType)-1], []byte(", "))
		if len(parts) != 2 {
			return true, fmt.Errorf("invalid decimal type (should have precision and scale): %s", c.chType)
		}

		precision, err := strconv.Atoi(string(parts[0]))
		if err != nil {
			return true, fmt.Errorf("invalid precision: %s", err)
		}
		scale, err := strconv.Atoi(string(parts[1]))
		if err != nil {
			return true, fmt.Errorf("invalid scale: %s", err)
		}
		c.params = []interface{}{precision, scale}
		var size int
		switch {
		case precision >= 1 && precision <= 9:
			size = 4
		case precision >= 10 && precision <= 18:
			size = 8
		case precision >= 19 && precision <= 38:
			size = 16
		case precision >= 39 && precision <= 76:
			size = 32
		default:
			return true, fmt.Errorf("invalid precision: %d. it should be between 1 and 76", precision)
		}
		if c.size != size {
			return true, &ErrInvalidType{
				column: c,
			}
		}
		return true, nil
	}
	return false, nil
}

func (c *Base[T]) ColumnType() string {
	if ok, _ := c.checkFixedString(c.chType); !ok {
		if str, ok := byteChColumnType[c.size]; ok {
			return str
		}
	}
	return fmt.Sprintf("T(%d bytes size)", c.size)
}
