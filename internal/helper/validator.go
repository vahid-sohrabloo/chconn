package helper

func IsEnum8(chType []byte) bool {
	return len(chType) > Enum8StrLen && (string(chType[:Enum8StrLen]) == Enum8Str)
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

func TypesInParentheses(b []byte) [][]byte {
	var columnsTuple [][]byte
	var openFunc int
	cur := 0
	for i, char := range b {
		if char == ',' {
			if openFunc == 0 {
				columnsTuple = append(columnsTuple, b[cur:i])
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
	return append(columnsTuple, b[cur:])
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
