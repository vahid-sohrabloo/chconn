package generator

import (
	"strconv"
	"strings"

	"github.com/dave/jennifer/jen"
)

//nolint:gocyclo,funlen
func getFieldByType(chType string) *jen.Statement {
	switch chType {
	case "Int8":
		return jen.Int8()
	case "Int16":
		return jen.Int16()
	case "Int32":
		return jen.Int32()
	case "Int64":
		return jen.Int64()
	case "UInt8":
		return jen.Uint8()
	case "UInt16":
		return jen.Uint16()
	case "UInt32":
		return jen.Uint32()
	case "UInt64":
		return jen.Uint64()
	case "Float64":
		return jen.Float64()
	case "Float32":
		return jen.Float32()
	case "String":
		return jen.String()
	case "DateTime":
		return jen.Qual("time", "Time")
	case "Date":
		return jen.Qual("time", "Time")
	case "IPv4":
		return jen.Qual("net", "IP")
	case "IPv6":
		return jen.Qual("net", "IP")
	case "UUID":
		return jen.Index(jen.Lit(16)).Byte()
	default:
		if strings.HasPrefix(chType, "DateTime(") {
			return jen.Qual("time", "Time")
		}
		if strings.HasPrefix(chType, "DateTime64(") {
			return jen.Qual("time", "Time")
		}
		// todo support Decimal Decimal128 and Decimal256
		if strings.HasPrefix(chType, "Decimal(") {
			return jen.Float64()
		}
		if strings.HasPrefix(chType, "LowCardinality(") {
			return getFieldByType(chType[15 : len(chType)-1])
		}

		if strings.HasPrefix(chType, "Enum8(") {
			return jen.Int8()
		}
		if strings.HasPrefix(chType, "Enum16(") {
			return jen.Int16()
		}
		if strings.HasPrefix(chType, "Nullable(") {
			return jen.Op("*").Add(getFieldByType(chType[9 : len(chType)-1]))
		}
		if strings.HasPrefix(chType, "SimpleAggregateFunction(") {
			return getFieldByType(getNestedType(chType[24:]))
		}
		if strings.HasPrefix(chType, "FixedString(") {
			return jen.Index().Byte()
		}
		if strings.HasPrefix(chType, "Array(") {
			field := getFieldByType(chType[6 : len(chType)-1])
			if field == nil {
				return nil
			}
			return jen.Index().Add(field)
		}
		if strings.HasPrefix(chType, "Tuple(") {
			var openFunc int
			var fields []jen.Code
			cur := 6
			// for between `Tuple(` and `)`
			idx := 1
			for i, char := range chType[6 : len(chType)-1] {
				if char == ',' {
					if openFunc == 0 {
						fields = append(
							fields,
							jen.Id("Field"+strconv.Itoa(idx)).Add(getFieldByType(chType[cur:i+6])),
						)
						idx++
						cur = i + 6
					}
					continue
				}
				if char == '(' {
					openFunc++
					continue
				}
				if char == ')' {
					openFunc--
					continue
				}
			}
			fields = append(
				fields,
				jen.Id("Field"+strconv.Itoa(idx)).Add(getFieldByType(chType[cur+2:len(chType)-1])),
			)

			return jen.Struct(fields...)
		}
		if strings.HasPrefix(chType, "Array(") {
			field := getFieldByType(chType[6 : len(chType)-1])
			if field == nil {
				return nil
			}
			return jen.Index().Add(field)
		}
	}
	panic("NOT support " + chType)
}

func getNestedType(chType string) string {
	for i, v := range chType {
		if v == ',' {
			return chType[i+2 : len(chType)-1]
		}
	}
	panic("Cannot found  netsted type of " + chType)
}

func getStandardName(name string) string {
	if name == "f" {
		return "f"
	}
	return snakeCaseToCamelCase(strings.ReplaceAll(name, ".", "_"))
}

func snakeCaseToCamelCase(inputUnderScoreStr string) (camelCase string) {
	isToUpper := false

	for k, v := range inputUnderScoreStr {
		if k == 0 {
			camelCase = strings.ToUpper(string(inputUnderScoreStr[0]))
		} else {
			if isToUpper {
				camelCase += strings.ToUpper(string(v))
				isToUpper = false
			} else {
				if v == '_' {
					isToUpper = true
				} else {
					camelCase += string(v)
				}
			}
		}
	}
	return
}
