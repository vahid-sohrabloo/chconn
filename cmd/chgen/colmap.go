package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// colInfo describes the column type and constructor for a given Go field + chtype.
type colInfo struct {
	fieldType        string // e.g. "*column.Base[uint64]"
	constructor      string // e.g. "column.New[uint64]()"
	isNullable       bool
	needsStrictFalse bool
	appendMethod     string // "Append" or "AppendP"
	rowMethod        string // "Row" or "RowP"
}

// fixedStringRe matches [N]byte Go types.
var fixedStringRe = regexp.MustCompile(`^\[(\d+)\]byte$`)

// colMapping maps a Go field type + ClickHouse type tag to the correct
// chconn column constructor info.
func colMapping(goType string, chType string) (colInfo, error) {
	goType = strings.TrimSpace(goType)
	chType = strings.TrimSpace(chType)

	// Strip SimpleAggregateFunction(func, X) wrapper first.
	if strings.HasPrefix(chType, "SimpleAggregateFunction(") && strings.HasSuffix(chType, ")") {
		args := chType[len("SimpleAggregateFunction(") : len(chType)-1]
		comma := findTopLevelComma(args)
		if comma < 0 {
			return colInfo{}, fmt.Errorf("invalid SimpleAggregateFunction: %q", chType)
		}
		chType = strings.TrimSpace(args[comma+1:])
	}

	// Handle Nullable wrapper: goType must be *T.
	if strings.HasPrefix(chType, "Nullable(") && strings.HasSuffix(chType, ")") {
		if !strings.HasPrefix(goType, "*") {
			return colInfo{}, fmt.Errorf("incompatible: Go type %q is not a pointer for Nullable chtype %q", goType, chType)
		}
		innerGoType := goType[1:]
		innerChType := chType[len("Nullable(") : len(chType)-1]
		inner, err := colMapping(innerGoType, innerChType)
		if err != nil {
			return colInfo{}, err
		}
		// Build nullable variant from inner
		ci := colInfo{
			isNullable:   true,
			appendMethod: "AppendP",
			rowMethod:    "RowP",
		}
		// Determine the nullable field type and constructor based on inner
		if inner.isNullable {
			return colInfo{}, fmt.Errorf("nested Nullable not supported")
		}
		// Use .Nullable() method on the inner constructor
		ci.constructor = inner.constructor + ".Nullable()"
		// Determine fieldType based on inner
		switch {
		case strings.HasPrefix(inner.fieldType, "*column.String"):
			ci.fieldType = "*column.StringNullable"
		case strings.HasPrefix(inner.fieldType, "*column.Date["):
			// e.g. *column.Date[types.DateTime] -> *column.DateNullable[types.DateTime]
			dateParam := inner.fieldType[len("*column.Date[") : len(inner.fieldType)-1]
			ci.fieldType = fmt.Sprintf("*column.DateNullable[%s]", dateParam)
		case strings.HasPrefix(inner.fieldType, "*column.Base["):
			// e.g. *column.Base[uint64] -> *column.BaseNullable[uint64]
			baseParam := inner.fieldType[len("*column.Base[") : len(inner.fieldType)-1]
			ci.fieldType = fmt.Sprintf("*column.BaseNullable[%s]", baseParam)
		default:
			return colInfo{}, fmt.Errorf("cannot create Nullable wrapper for field type %q", inner.fieldType)
		}
		ci.needsStrictFalse = inner.needsStrictFalse
		return ci, nil
	}

	// Handle Array wrapper: goType must be []T.
	if strings.HasPrefix(chType, "Array(") && strings.HasSuffix(chType, ")") {
		if !strings.HasPrefix(goType, "[]") {
			return colInfo{}, fmt.Errorf("incompatible: Go type %q is not a slice for Array chtype %q", goType, chType)
		}
		innerGoType := goType[2:]
		innerChType := chType[len("Array(") : len(chType)-1]
		inner, err := colMapping(innerGoType, innerChType)
		if err != nil {
			return colInfo{}, err
		}
		return colInfo{
			fieldType:    fmt.Sprintf("*column.Array[%s]", innerGoType),
			constructor:  inner.constructor + ".Array()",
			appendMethod: "Append",
			rowMethod:    "Row",
		}, nil
	}

	// Handle Map wrapper: goType must be map[K]V.
	if strings.HasPrefix(chType, "Map(") && strings.HasSuffix(chType, ")") {
		if !strings.HasPrefix(goType, "map[") {
			return colInfo{}, fmt.Errorf("incompatible: Go type %q is not a map for Map chtype %q", goType, chType)
		}
		// Parse map[K]V from goType
		keyGoType, valGoType, err := parseMapGoType(goType)
		if err != nil {
			return colInfo{}, fmt.Errorf("cannot parse map Go type %q: %w", goType, err)
		}
		// Parse Map(ChK, ChV) from chType
		args := chType[len("Map(") : len(chType)-1]
		comma := findTopLevelComma(args)
		if comma < 0 {
			return colInfo{}, fmt.Errorf("invalid Map chtype: %q", chType)
		}
		keyChType := strings.TrimSpace(args[:comma])
		valChType := strings.TrimSpace(args[comma+1:])

		keyInfo, err := colMapping(keyGoType, keyChType)
		if err != nil {
			return colInfo{}, fmt.Errorf("Map key: %w", err)
		}
		valInfo, err := colMapping(valGoType, valChType)
		if err != nil {
			return colInfo{}, fmt.Errorf("Map value: %w", err)
		}
		return colInfo{
			fieldType:    fmt.Sprintf("*column.Map[%s, %s]", keyGoType, valGoType),
			constructor:  fmt.Sprintf("column.NewMap[%s, %s](%s, %s)", keyGoType, valGoType, innerConstructor(keyInfo.constructor), innerConstructor(valInfo.constructor)),
			appendMethod: "Append",
			rowMethod:    "Row",
		}, nil
	}

	// Track LowCardinality flag.
	isLC := false
	if strings.HasPrefix(chType, "LowCardinality(") && strings.HasSuffix(chType, ")") {
		isLC = true
		chType = chType[len("LowCardinality(") : len(chType)-1]
	}

	// Now handle the base types.
	ci, err := baseColMapping(goType, chType)
	if err != nil {
		return colInfo{}, err
	}

	// Apply LowCardinality wrapper if needed.
	if isLC {
		ci.fieldType = fmt.Sprintf("*column.LowCardinality[%s]", goType)
		ci.constructor = ci.constructor + ".LowCardinality()"
	}

	return ci, nil
}

// innerConstructor strips the leading "column." from a constructor expression
// when it will be used as a NewMap argument (the function already has the package prefix).
// Actually for NewMap we pass the full constructor result, so we just return it.
func innerConstructor(c string) string {
	return c
}

// parseMapGoType parses "map[K]V" and returns K and V as strings.
func parseMapGoType(goType string) (string, string, error) {
	if !strings.HasPrefix(goType, "map[") {
		return "", "", fmt.Errorf("not a map type")
	}
	rest := goType[4:] // after "map["
	// Find the closing bracket for the key
	depth := 1
	for i, ch := range rest {
		switch ch {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				key := rest[:i]
				val := rest[i+1:]
				return key, val, nil
			}
		}
	}
	return "", "", fmt.Errorf("malformed map type: %q", goType)
}

// baseColMapping handles scalar types (no Nullable/Array/Map wrappers).
func baseColMapping(goType string, chType string) (colInfo, error) {
	// Special case: string
	if goType == "string" {
		if chType == "String" {
			return colInfo{
				fieldType:    "*column.String",
				constructor:  "column.NewString()",
				appendMethod: "Append",
				rowMethod:    "Row",
			}, nil
		}
		return colInfo{}, fmt.Errorf("incompatible: Go type %q cannot map to chtype %q", goType, chType)
	}

	// Special case: time.Time + date/time chtypes
	if goType == "time.Time" {
		switch {
		case chType == "DateTime":
			return colInfo{
				fieldType:    "*column.Date[types.DateTime]",
				constructor:  "column.NewDate[types.DateTime]()",
				appendMethod: "Append",
				rowMethod:    "Row",
			}, nil
		case chType == "Date":
			return colInfo{
				fieldType:    "*column.Date[types.Date]",
				constructor:  "column.NewDate[types.Date]()",
				appendMethod: "Append",
				rowMethod:    "Row",
			}, nil
		case chType == "Date32":
			return colInfo{
				fieldType:    "*column.Date[types.Date32]",
				constructor:  "column.NewDate[types.Date32]()",
				appendMethod: "Append",
				rowMethod:    "Row",
			}, nil
		case strings.HasPrefix(chType, "DateTime64("):
			return colInfo{
				fieldType:    "*column.Date[types.DateTime64]",
				constructor:  "column.NewDate[types.DateTime64]()",
				appendMethod: "Append",
				rowMethod:    "Row",
			}, nil
		case strings.HasPrefix(chType, "DateTime("):
			// DateTime with timezone
			return colInfo{
				fieldType:    "*column.Date[types.DateTime]",
				constructor:  "column.NewDate[types.DateTime]()",
				appendMethod: "Append",
				rowMethod:    "Row",
			}, nil
		}
		return colInfo{}, fmt.Errorf("incompatible: Go type %q cannot map to chtype %q", goType, chType)
	}

	// Special case: uint32 + DateTime (raw timestamp mode, needsStrictFalse)
	if goType == "uint32" && (chType == "DateTime" || strings.HasPrefix(chType, "DateTime(")) {
		return colInfo{
			fieldType:        "*column.Base[uint32]",
			constructor:      "column.New[uint32]()",
			needsStrictFalse: true,
			appendMethod:     "Append",
			rowMethod:        "Row",
		}, nil
	}

	// Special case: uint16 + Date (raw date mode, needsStrictFalse)
	if goType == "uint16" && chType == "Date" {
		return colInfo{
			fieldType:        "*column.Base[uint16]",
			constructor:      "column.New[uint16]()",
			needsStrictFalse: true,
			appendMethod:     "Append",
			rowMethod:        "Row",
		}, nil
	}

	// Tuple/Nested: mapped to any — not directly constructible by chgen.
	// Users must define the column manually.
	if goType == "any" {
		return colInfo{}, fmt.Errorf("Go type %q (from Tuple/Nested) requires manual column definition", goType)
	}

	// JSON type: json.RawMessage maps to *column.JSON
	if goType == "json.RawMessage" {
		if chType != "JSON" && !strings.HasPrefix(chType, "JSON(") {
			return colInfo{}, fmt.Errorf("incompatible: Go type %q cannot map to chtype %q", goType, chType)
		}
		return colInfo{
			fieldType:    "*column.JSON",
			constructor:  "column.NewJSON()",
			appendMethod: "Append",
			rowMethod:    "Row",
		}, nil
	}

	// FixedString: [N]byte
	if m := fixedStringRe.FindStringSubmatch(goType); m != nil {
		n, _ := strconv.Atoi(m[1])
		// Verify chtype matches
		expectedChType := fmt.Sprintf("FixedString(%d)", n)
		if chType != expectedChType {
			return colInfo{}, fmt.Errorf("incompatible: Go type %q does not match chtype %q (expected %s)", goType, chType, expectedChType)
		}
		return colInfo{
			fieldType:    fmt.Sprintf("*column.Base[%s]", goType),
			constructor:  fmt.Sprintf("column.New[%s]()", goType),
			appendMethod: "Append",
			rowMethod:    "Row",
		}, nil
	}

	// Enum types: Enum8(...) or Enum16(...)
	if strings.HasPrefix(chType, "Enum8(") || strings.HasPrefix(chType, "Enum16(") {
		// Any custom Go type maps to Base[GoType]
		return colInfo{
			fieldType:    fmt.Sprintf("*column.Base[%s]", goType),
			constructor:  fmt.Sprintf("column.New[%s]()", goType),
			appendMethod: "Append",
			rowMethod:    "Row",
		}, nil
	}

	// Decimal types with parameters: Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S)
	if strings.HasPrefix(chType, "Decimal32(") || strings.HasPrefix(chType, "Decimal64(") ||
		strings.HasPrefix(chType, "Decimal128(") || strings.HasPrefix(chType, "Decimal256(") {
		expected, ok := chTypeToGoScalar(chType)
		if !ok {
			return colInfo{}, fmt.Errorf("unsupported chtype %q", chType)
		}
		if goType != expected {
			return colInfo{}, fmt.Errorf("incompatible: Go type %q cannot map to chtype %q (expected %s)", goType, chType, expected)
		}
		return colInfo{
			fieldType:    fmt.Sprintf("*column.Base[%s]", goType),
			constructor:  fmt.Sprintf("column.New[%s]()", goType),
			appendMethod: "Append",
			rowMethod:    "Row",
		}, nil
	}

	// Decimal(P, S) — precision determines the Go type
	if strings.HasPrefix(chType, "Decimal(") && strings.HasSuffix(chType, ")") {
		expected, ok := decimalPSToGoType(chType)
		if !ok {
			return colInfo{}, fmt.Errorf("unsupported chtype %q", chType)
		}
		if goType != expected {
			return colInfo{}, fmt.Errorf("incompatible: Go type %q cannot map to chtype %q (expected %s)", goType, chType, expected)
		}
		return colInfo{
			fieldType:    fmt.Sprintf("*column.Base[%s]", goType),
			constructor:  fmt.Sprintf("column.New[%s]()", goType),
			appendMethod: "Append",
			rowMethod:    "Row",
		}, nil
	}

	// Generic primitives: verify the Go type matches the expected type for the chtype.
	expected, ok := chTypeToGoScalar(chType)
	if !ok {
		return colInfo{}, fmt.Errorf("unsupported chtype %q", chType)
	}
	if goType != expected {
		return colInfo{}, fmt.Errorf("incompatible: Go type %q cannot map to chtype %q (expected %s)", goType, chType, expected)
	}
	return colInfo{
		fieldType:    fmt.Sprintf("*column.Base[%s]", goType),
		constructor:  fmt.Sprintf("column.New[%s]()", goType),
		appendMethod: "Append",
		rowMethod:    "Row",
	}, nil
}

// chTypeToGoScalar returns the canonical Go type for a scalar ClickHouse type,
// and a boolean indicating whether the chtype is recognised.
func chTypeToGoScalar(chType string) (string, bool) {
	switch chType {
	case "Int8":
		return "int8", true
	case "Int16":
		return "int16", true
	case "Int32":
		return "int32", true
	case "Int64":
		return "int64", true
	case "UInt8":
		return "uint8", true
	case "UInt16":
		return "uint16", true
	case "UInt32":
		return "uint32", true
	case "UInt64":
		return "uint64", true
	case "Float32":
		return "float32", true
	case "Float64":
		return "float64", true
	case "Bool":
		return "bool", true
	case "UUID":
		return "types.UUID", true
	case "IPv4":
		return "types.IPv4", true
	case "IPv6":
		return "types.IPv6", true
	case "Int128":
		return "types.Int128", true
	case "Int256":
		return "types.Int256", true
	case "UInt128":
		return "types.Uint128", true
	case "UInt256":
		return "types.Uint256", true
	case "Decimal128":
		return "types.Decimal128", true
	case "Decimal256":
		return "types.Decimal256", true
	}
	// Decimal with scale parameter
	if strings.HasPrefix(chType, "Decimal32(") {
		return "types.Decimal32", true
	}
	if strings.HasPrefix(chType, "Decimal64(") {
		return "types.Decimal64", true
	}
	if strings.HasPrefix(chType, "Decimal128(") {
		return "types.Decimal128", true
	}
	if strings.HasPrefix(chType, "Decimal256(") {
		return "types.Decimal256", true
	}
	return "", false
}

// decimalPSToGoType maps a Decimal(P, S) chtype to the corresponding Go type
// based on the precision value.
func decimalPSToGoType(chType string) (string, bool) {
	if !strings.HasPrefix(chType, "Decimal(") || !strings.HasSuffix(chType, ")") {
		return "", false
	}
	args := chType[len("Decimal(") : len(chType)-1]
	precStr, _, ok := strings.Cut(args, ",")
	if !ok {
		return "", false
	}
	precStr = strings.TrimSpace(precStr)
	prec, err := strconv.Atoi(precStr)
	if err != nil {
		return "", false
	}
	switch {
	case prec <= 9:
		return "types.Decimal32", true
	case prec <= 18:
		return "types.Decimal64", true
	case prec <= 38:
		return "types.Decimal128", true
	case prec <= 76:
		return "types.Decimal256", true
	default:
		return "", false
	}
}
