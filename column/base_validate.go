package column

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
)

var chColumnByteSize = map[string]int{
	"Bool":       1,
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

var goTypeToChType = map[string]string{
	"bool":             "Bool",
	"int8":             "Int8",
	"int16":            "Int16",
	"int32":            "Int32",
	"int64":            "Int64",
	"types.Int128":     "Int128",
	"types.Int256":     "Int256",
	"uint8":            "UInt8",
	"uint16":           "UInt16",
	"uint32":           "UInt32",
	"uint64":           "UInt64",
	"types.Uint128":    "UInt128",
	"types.Uint256":    "UInt256",
	"float32":          "Float32",
	"float64":          "Float64",
	"types.Date":       "Date",
	"types.Date32":     "Date32",
	"types.DateTime":   "DateTime",
	"types.DateTime64": "DateTime64",
	"types.UUID":       "UUID",
	"types.IPv4":       "IPv4",
	"types.IPv6":       "IPv6",
}

var byteChstructType = map[int]string{
	1:  "Int8|UInt8|Enum8",
	2:  "Int16|UInt16|Enum16|Date",
	4:  "Int32|UInt32|Float32|Decimal32|Date32|DateTime|IPv4",
	8:  "Int64|UInt64|Float64|Decimal64|DateTime64",
	16: "Int128|UInt128|Decimal128|IPv6|UUID",
	32: "Int256|UInt256|Decimal256",
}

func (c *Base[T]) SetStrict(strict bool) *Base[T] {
	c.strict = strict
	return c
}

func (c *Base[T]) Validate(forInsert bool) error {
	chType := helper.FilterSimpleAggregate(c.chType)

	if byteSize, ok := chColumnByteSize[string(chType)]; ok {
		if c.strict {
			if goTypeToChType[c.kind.String()] != string(chType) && goTypeToChType[c.rtype.String()] != string(chType) {
				return &ErrInvalidType{
					chType:     string(c.chType),
					goToChType: c.structType(),
					chconnType: c.chconnType(),
				}
			}
		} else if byteSize != c.size {
			return &ErrInvalidType{
				chType:     string(c.chType),
				goToChType: c.structType(),
				chconnType: c.chconnType(),
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
		chType:     string(c.chType),
		goToChType: c.structType(),
		chconnType: c.chconnType(),
	}
}

func (c *Base[T]) checkEnum8(chType []byte) (bool, error) {
	if helper.IsEnum8(chType) {
		if c.strict {
			if c.kind.String() != "int8" {
				return true, &ErrInvalidType{
					chType:     string(c.chType),
					goToChType: c.structType(),
					chconnType: c.chconnType(),
				}
			}
			return true, nil
		}
		if c.size != Int8Size {
			return true, &ErrInvalidType{
				chType:     string(c.chType),
				goToChType: c.structType(),
				chconnType: c.chconnType(),
			}
		}
		return true, nil
	}
	return false, nil
}

func (c *Base[T]) checkEnum16(chType []byte) (bool, error) {
	if helper.IsEnum16(chType) {
		if c.strict {
			if c.kind.String() != "int16" {
				return true, &ErrInvalidType{
					chType:     string(c.chType),
					goToChType: c.structType(),
					chconnType: c.chconnType(),
				}
			}
			return true, nil
		}
		if c.size != Int16Size {
			return true, &ErrInvalidType{
				chType:     string(c.chType),
				goToChType: c.structType(),
				chconnType: c.chconnType(),
			}
		}
		return true, nil
	}
	return false, nil
}

func (c *Base[T]) checkDateTime(chType []byte) (bool, error) {
	if helper.IsDateTimeWithParam(chType) {
		if c.strict {
			if c.kind.String() != "uint32" {
				return true, &ErrInvalidType{
					chType:     string(c.chType),
					goToChType: "UInt32|DateTime",
					chconnType: c.chconnType(),
				}
			}
		} else if c.size != DateTimeSize {
			return true, &ErrInvalidType{
				chType:     string(c.chType),
				goToChType: c.structType(),
				chconnType: c.chconnType(),
			}
		}
		c.params = []any{
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
		if c.strict {
			if c.kind.String() != "int64" {
				return true, &ErrInvalidType{
					chType:     string(c.chType),
					goToChType: "Int64|DateTime64",
					chconnType: c.chconnType(),
				}
			}
		} else if c.size != DateTime64Size {
			return true, &ErrInvalidType{
				chType:     string(c.chType),
				goToChType: c.structType(),
				chconnType: c.chconnType(),
			}
		}
		parts := bytes.Split(chType[helper.DecimalStrLen:len(chType)-1], []byte(", "))
		c.params = []any{
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
		// todo check strict mode
		if c.size != size {
			return true, &ErrInvalidType{
				chType:     string(c.chType),
				goToChType: c.structType(),
				chconnType: c.chconnType(),
			}
		}
		return true, nil
	}
	return false, nil
}

func (c *Base[T]) checkDecimal(chType []byte) (bool, error) {
	// todo handle strict mode
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
		c.params = []any{precision, scale}
		var size int
		switch {
		case precision >= 1 && precision <= 9:
			c.isDecimal = decimal32Type
			size = 4
		case precision >= 10 && precision <= 18:
			c.isDecimal = decimal64Type
			size = 8
		case precision >= 19 && precision <= 38:
			c.isDecimal = decimal128Type
			size = 16
		case precision >= 39 && precision <= 76:
			c.isDecimal = decimal256Type
			size = 32
		default:
			return true, fmt.Errorf("invalid precision: %d. it should be between 1 and 76", precision)
		}
		if c.size != size {
			return true, &ErrInvalidType{
				chType:     string(c.chType),
				goToChType: c.structType(),
				chconnType: c.chconnType(),
			}
		}
		return true, nil
	}
	return false, nil
}

func (c *Base[T]) chconnType() string {
	return "column.Base[" + c.rtype.String() + "]"
}

func (c *Base[T]) structType() string {
	if c.strict {
		structType := goTypeToChType[c.kind.String()]
		if structType == "" {
			structType = goTypeToChType[c.rtype.String()]
		}
		return structType
	}
	if !helper.IsFixedString(c.chType) {
		if str, ok := byteChstructType[c.size]; ok {
			return str
		}
	}
	return fmt.Sprintf("T(%d bytes size)", c.size)
}
