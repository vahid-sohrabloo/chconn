package helper

import (
	"bytes"
	"fmt"
	"strconv"
)

func IsEnum8(chType []byte) bool {
	return len(chType) > Enum8StrLen && (string(chType[:Enum8StrLen]) == Enum8Str)
}

func ExtractEnum(data []byte) (intToStringMap map[int16]string, stringToIntMap map[string]int16, err error) {
	enums := bytes.Split(data, []byte(", "))
	intToStringMap = make(map[int16]string)
	stringToIntMap = make(map[string]int16)
	for _, enum := range enums {
		parts := bytes.SplitN(enum, []byte(" = "), 2)
		if len(parts) != 2 {
			return nil, nil, fmt.Errorf("invalid enum: %s", enum)
		}

		id, err := strconv.ParseInt(string(parts[1]), 10, 8)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid enum id: %s", parts[1])
		}

		val := string(parts[0][1 : len(parts[0])-1])
		intToStringMap[int16(id)] = val
		stringToIntMap[val] = int16(id)
	}
	return intToStringMap, stringToIntMap, nil
}

func IsEnum16(chType []byte) bool {
	return len(chType) > Enum16StrLen && (string(chType[:Enum16StrLen]) == Enum16Str)
}

func IsDateTimeWithParam(chType []byte) bool {
	return len(chType) > DateTimeStrLen && (string(chType[:DateTimeStrLen]) == DateTimeStr)
}

func IsDateTime64(chType []byte) bool {
	return len(chType) > DateTime64StrLen && (string(chType[:DateTime64StrLen]) == DateTime64Str)
}

func IsFixedString(chType []byte) bool {
	return len(chType) > FixedStringStrLen && (string(chType[:FixedStringStrLen]) == FixedStringStr)
}

func IsDecimal(chType []byte) bool {
	return len(chType) > DecimalStrLen && (string(chType[:DecimalStrLen]) == DecimalStr)
}

func IsRing(chType []byte) bool {
	return string(chType) == RingStr
}

func IsMultiPolygon(chType []byte) bool {
	return string(chType) == MultiPolygonStr
}

func IsNested(chType []byte) bool {
	return len(chType) > LenNestedStr && string(chType[:LenNestedStr]) == NestedStr
}

func NestedToArrayType(chType []byte) []byte {
	if IsNested(chType) {
		newChType := make([]byte, 0, len(chType)-LenNestedStr+LenArrayStr+LenTupleStr+1)
		newChType = append(newChType, "Array(Tuple("...)
		newChType = append(newChType, chType[LenNestedStr:]...)
		newChType = append(newChType, ')')
		return newChType
	}
	return chType
}

func IsArray(chType []byte) bool {
	return len(chType) > LenArrayStr && string(chType[:LenArrayStr]) == ArrayStr
}

func IsPolygon(chType []byte) bool {
	return string(chType) == PolygonStr
}

func IsString(chType []byte) bool {
	return string(chType) == StringStr
}

func IsLowCardinality(chType []byte) bool {
	return len(chType) > LenLowCardinalityStr && string(chType[:LenLowCardinalityStr]) == LowCardinalityStr
}

func IsNullableLowCardinality(chType []byte) bool {
	return len(chType) > LenLowCardinalityNullableStr &&
		string(chType[:LenLowCardinalityNullableStr]) == LowCardinalityNullableStr
}

func IsMap(chType []byte) bool {
	return len(chType) > LenMapStr && string(chType[:LenMapStr]) == MapStr
}

func IsNullable(chType []byte) bool {
	return len(chType) > LenNullableStr && string(chType[:LenNullableStr]) == NullableStr
}

func IsPoint(chType []byte) bool {
	return string(chType) == PointStr
}

func IsTuple(chType []byte) bool {
	return len(chType) > LenTupleStr && string(chType[:LenTupleStr]) == TupleStr
}

type ColumnData struct {
	ChType, Name []byte
}

func TypesInParentheses(b []byte) ([]ColumnData, error) {
	var columns []ColumnData
	var openFunc int
	var hasBacktick bool
	cur := 0
	for i, char := range b {
		if char == '`' {
			if !hasBacktick {
				hasBacktick = true
				continue
			}
			if b[i-1] != '\\' {
				hasBacktick = false
			}
			continue
		}
		if hasBacktick {
			continue
		}
		if char == ',' {
			if openFunc == 0 {
				colData, err := SplitNameType(b[cur:i])
				if err != nil {
					return nil, err
				}
				columns = append(columns, colData)
				//  add 2 to skip the ', '
				cur = i + 2
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
	colData, err := SplitNameType(b[cur:])
	if err != nil {
		return nil, err
	}
	return append(columns, colData), nil
}

func SplitNameType(b []byte) (ColumnData, error) {
	// for example: `date f` Array(String)
	if b[0] == '`' {
		b = b[1:]
		for i, char := range b {
			if char == '`' && b[i-1] != '\\' {
				return ColumnData{
					Name:   b[1 : i+1],
					ChType: b[i+2:],
				}, nil
			}
		}
		return ColumnData{}, fmt.Errorf("cannot find closing backtick in %s", b)
	}
	for i, char := range b {
		if char == '(' {
			break
		}
		if char == ' ' {
			return ColumnData{
				Name:   b[1 : i+1],
				ChType: b[i+1:],
			}, nil
		}
	}
	return ColumnData{
		ChType: b,
	}, nil
}

func FilterSimpleAggregate(chType []byte) []byte {
	if len(chType) <= SimpleAggregateStrLen || (string(chType[:SimpleAggregateStrLen]) != SimpleAggregateStr) {
		return chType
	}
	chType = chType[SimpleAggregateStrLen:]
	for i, v := range chType {
		if v == ',' {
			return chType[i+2 : len(chType)-1]
		}
	}
	panic("Cannot found nested type of " + string(chType))
}
