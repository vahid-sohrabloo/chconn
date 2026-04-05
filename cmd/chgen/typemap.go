package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// goTypeInfo holds the resolved Go type string and enum metadata.
type goTypeInfo struct {
	goType     string
	isEnum     bool
	enumValues map[string]int
}

// enumValueRe matches 'name' = value pairs inside Enum8/Enum16 definitions.
var enumValueRe = regexp.MustCompile(`'([^']+)'\s*=\s*(-?\d+)`)

// chTypeToGo converts a ClickHouse type string to Go type information.
// If timeAsUint is true, date/time types are mapped to their underlying
// integer type instead of time.Time.
func chTypeToGo(chType string, timeAsUint bool) (goTypeInfo, error) {
	chType = strings.TrimSpace(chType)

	// --- Wrappers that delegate recursively ---

	if strings.HasPrefix(chType, "Nullable(") && strings.HasSuffix(chType, ")") {
		inner := chType[len("Nullable(") : len(chType)-1]
		info, err := chTypeToGo(inner, timeAsUint)
		if err != nil {
			return goTypeInfo{}, err
		}
		info.goType = "*" + info.goType
		return info, nil
	}

	if strings.HasPrefix(chType, "LowCardinality(") && strings.HasSuffix(chType, ")") {
		inner := chType[len("LowCardinality(") : len(chType)-1]
		return chTypeToGo(inner, timeAsUint)
	}

	if strings.HasPrefix(chType, "Array(") && strings.HasSuffix(chType, ")") {
		inner := chType[len("Array(") : len(chType)-1]
		info, err := chTypeToGo(inner, timeAsUint)
		if err != nil {
			return goTypeInfo{}, err
		}
		info.goType = "[]" + info.goType
		return info, nil
	}

	if strings.HasPrefix(chType, "SimpleAggregateFunction(") && strings.HasSuffix(chType, ")") {
		args := chType[len("SimpleAggregateFunction(") : len(chType)-1]
		comma := findTopLevelComma(args)
		if comma < 0 {
			return goTypeInfo{}, fmt.Errorf("invalid SimpleAggregateFunction: %q", chType)
		}
		inner := strings.TrimSpace(args[comma+1:])
		return chTypeToGo(inner, timeAsUint)
	}

	if strings.HasPrefix(chType, "Map(") && strings.HasSuffix(chType, ")") {
		args := chType[len("Map(") : len(chType)-1]
		comma := findTopLevelComma(args)
		if comma < 0 {
			return goTypeInfo{}, fmt.Errorf("invalid Map type: %q", chType)
		}
		keyStr := strings.TrimSpace(args[:comma])
		valStr := strings.TrimSpace(args[comma+1:])
		keyInfo, err := chTypeToGo(keyStr, timeAsUint)
		if err != nil {
			return goTypeInfo{}, fmt.Errorf("Map key: %w", err)
		}
		valInfo, err := chTypeToGo(valStr, timeAsUint)
		if err != nil {
			return goTypeInfo{}, fmt.Errorf("Map value: %w", err)
		}
		return goTypeInfo{goType: fmt.Sprintf("map[%s]%s", keyInfo.goType, valInfo.goType)}, nil
	}

	// --- Decimal(P, S) — precision determines the underlying type ---

	if strings.HasPrefix(chType, "Decimal(") && strings.HasSuffix(chType, ")") {
		args := chType[len("Decimal(") : len(chType)-1]
		precStr, _, ok := strings.Cut(args, ",")
		if !ok {
			return goTypeInfo{}, fmt.Errorf("invalid Decimal type: %q", chType)
		}
		precStr = strings.TrimSpace(precStr)
		prec, err := strconv.Atoi(precStr)
		if err != nil {
			return goTypeInfo{}, fmt.Errorf("invalid Decimal precision: %q", chType)
		}
		switch {
		case prec <= 9:
			return goTypeInfo{goType: "types.Decimal32"}, nil
		case prec <= 18:
			return goTypeInfo{goType: "types.Decimal64"}, nil
		case prec <= 38:
			return goTypeInfo{goType: "types.Decimal128"}, nil
		case prec <= 76:
			return goTypeInfo{goType: "types.Decimal256"}, nil
		default:
			return goTypeInfo{}, fmt.Errorf("Decimal precision %d exceeds max 76", prec)
		}
	}

	// --- Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S) ---

	if strings.HasPrefix(chType, "Decimal32(") {
		return goTypeInfo{goType: "types.Decimal32"}, nil
	}
	if strings.HasPrefix(chType, "Decimal64(") {
		return goTypeInfo{goType: "types.Decimal64"}, nil
	}
	if strings.HasPrefix(chType, "Decimal128(") {
		return goTypeInfo{goType: "types.Decimal128"}, nil
	}
	if strings.HasPrefix(chType, "Decimal256(") {
		return goTypeInfo{goType: "types.Decimal256"}, nil
	}

	// --- Tuple(...) — map to any; requires manual struct definition ---

	if strings.HasPrefix(chType, "Tuple(") {
		return goTypeInfo{goType: "any"}, nil
	}

	// --- Nested(...) — equivalent to Array(Tuple(...)); map to any ---

	if strings.HasPrefix(chType, "Nested(") {
		return goTypeInfo{goType: "any"}, nil
	}

	// --- FixedString(N) ---

	if strings.HasPrefix(chType, "FixedString(") && strings.HasSuffix(chType, ")") {
		nStr := chType[len("FixedString(") : len(chType)-1]
		n, err := strconv.Atoi(strings.TrimSpace(nStr))
		if err != nil || n <= 0 {
			return goTypeInfo{}, fmt.Errorf("invalid FixedString length: %q", chType)
		}
		return goTypeInfo{goType: fmt.Sprintf("[%d]byte", n)}, nil
	}

	// --- Enum8 / Enum16 ---

	if strings.HasPrefix(chType, "Enum8(") && strings.HasSuffix(chType, ")") {
		return parseEnum(chType[len("Enum8("):len(chType)-1], "int8")
	}
	if strings.HasPrefix(chType, "Enum16(") && strings.HasSuffix(chType, ")") {
		return parseEnum(chType[len("Enum16("):len(chType)-1], "int16")
	}

	// --- DateTime with optional timezone: DateTime('UTC') or DateTime64(N) ---

	if strings.HasPrefix(chType, "DateTime64(") && strings.HasSuffix(chType, ")") {
		if timeAsUint {
			return goTypeInfo{goType: "int64"}, nil
		}
		return goTypeInfo{goType: "time.Time"}, nil
	}

	if strings.HasPrefix(chType, "DateTime(") && strings.HasSuffix(chType, ")") {
		// DateTime('timezone') — treat same as DateTime
		if timeAsUint {
			return goTypeInfo{goType: "uint32"}, nil
		}
		return goTypeInfo{goType: "time.Time"}, nil
	}

	// --- Geo types ---

	switch chType {
	case "Point":
		return goTypeInfo{goType: "types.Point"}, nil
	case "Ring":
		return goTypeInfo{goType: "[]types.Point"}, nil
	case "Polygon":
		return goTypeInfo{goType: "[][]types.Point"}, nil
	case "MultiPolygon":
		return goTypeInfo{goType: "[][][]types.Point"}, nil
	}

	// --- Primitives and plain date/time ---

	switch chType {
	case "Int8":
		return goTypeInfo{goType: "int8"}, nil
	case "Int16":
		return goTypeInfo{goType: "int16"}, nil
	case "Int32":
		return goTypeInfo{goType: "int32"}, nil
	case "Int64":
		return goTypeInfo{goType: "int64"}, nil
	case "UInt8":
		return goTypeInfo{goType: "uint8"}, nil
	case "UInt16":
		return goTypeInfo{goType: "uint16"}, nil
	case "UInt32":
		return goTypeInfo{goType: "uint32"}, nil
	case "UInt64":
		return goTypeInfo{goType: "uint64"}, nil
	case "Float32":
		return goTypeInfo{goType: "float32"}, nil
	case "Float64":
		return goTypeInfo{goType: "float64"}, nil
	case "String":
		return goTypeInfo{goType: "string"}, nil
	case "Bool":
		return goTypeInfo{goType: "bool"}, nil
	case "UUID":
		return goTypeInfo{goType: "types.UUID"}, nil
	case "IPv4":
		return goTypeInfo{goType: "types.IPv4"}, nil
	case "IPv6":
		return goTypeInfo{goType: "types.IPv6"}, nil
	case "Int128":
		return goTypeInfo{goType: "types.Int128"}, nil
	case "Int256":
		return goTypeInfo{goType: "types.Int256"}, nil
	case "UInt128":
		return goTypeInfo{goType: "types.Uint128"}, nil
	case "UInt256":
		return goTypeInfo{goType: "types.Uint256"}, nil
	case "Decimal32":
		return goTypeInfo{goType: "types.Decimal32"}, nil
	case "Decimal64":
		return goTypeInfo{goType: "types.Decimal64"}, nil
	case "Decimal128":
		return goTypeInfo{goType: "types.Decimal128"}, nil
	case "Decimal256":
		return goTypeInfo{goType: "types.Decimal256"}, nil
	case "JSON":
		return goTypeInfo{goType: "json.RawMessage"}, nil
	case "Date":
		if timeAsUint {
			return goTypeInfo{goType: "uint16"}, nil
		}
		return goTypeInfo{goType: "time.Time"}, nil
	case "Date32":
		if timeAsUint {
			return goTypeInfo{goType: "int32"}, nil
		}
		return goTypeInfo{goType: "time.Time"}, nil
	case "DateTime":
		if timeAsUint {
			return goTypeInfo{goType: "uint32"}, nil
		}
		return goTypeInfo{goType: "time.Time"}, nil
	}

	return goTypeInfo{}, fmt.Errorf("unsupported ClickHouse type: %q", chType)
}

// parseEnum extracts enum name→value pairs and returns a goTypeInfo.
func parseEnum(body, baseType string) (goTypeInfo, error) {
	matches := enumValueRe.FindAllStringSubmatch(body, -1)
	if len(matches) == 0 {
		return goTypeInfo{}, fmt.Errorf("no enum values found in %q", body)
	}
	vals := make(map[string]int, len(matches))
	for _, m := range matches {
		v, err := strconv.Atoi(m[2])
		if err != nil {
			return goTypeInfo{}, fmt.Errorf("invalid enum value %q: %w", m[2], err)
		}
		vals[m[1]] = v
	}
	return goTypeInfo{goType: baseType, isEnum: true, enumValues: vals}, nil
}

// findTopLevelComma returns the index of the first comma in s that is not
// nested inside parentheses. Returns -1 if no such comma exists.
func findTopLevelComma(s string) int {
	depth := 0
	for i, ch := range s {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}
