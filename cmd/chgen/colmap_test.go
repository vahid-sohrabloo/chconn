package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColMapping_Primitives covers uint64/UInt64, int32/Int32, float32/Float32, bool/UInt8, int8/Int8.
func TestColMapping_Primitives(t *testing.T) {
	cases := []struct {
		goType    string
		chType    string
		wantField string
		wantCtor  string
	}{
		{"uint64", "UInt64", "*column.Base[uint64]", "column.New[uint64]()"},
		{"int32", "Int32", "*column.Base[int32]", "column.New[int32]()"},
		{"float32", "Float32", "*column.Base[float32]", "column.New[float32]()"},
		{"bool", "UInt8", "", ""}, // bool maps to Bool, not UInt8 — expect error
		{"int8", "Int8", "*column.Base[int8]", "column.New[int8]()"},
		{"uint8", "UInt8", "*column.Base[uint8]", "column.New[uint8]()"},
		{"bool", "Bool", "*column.Base[bool]", "column.New[bool]()"},
	}

	for _, tc := range cases {
		t.Run(tc.goType+"/"+tc.chType, func(t *testing.T) {
			info, err := colMapping(tc.goType, tc.chType)
			if tc.wantField == "" {
				// expect an error (incompatible)
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantField, info.fieldType)
			assert.Equal(t, tc.wantCtor, info.constructor)
			assert.False(t, info.isNullable)
			assert.False(t, info.needsStrictFalse)
			assert.Equal(t, "Append", info.appendMethod)
			assert.Equal(t, "Row", info.rowMethod)
		})
	}
}

// TestColMapping_String covers string/String and string/LowCardinality(String).
func TestColMapping_String(t *testing.T) {
	t.Run("string/String", func(t *testing.T) {
		info, err := colMapping("string", "String")
		require.NoError(t, err)
		assert.Equal(t, "*column.String", info.fieldType)
		assert.Equal(t, "column.NewString()", info.constructor)
		assert.Equal(t, "Append", info.appendMethod)
		assert.Equal(t, "Row", info.rowMethod)
	})

	t.Run("string/LowCardinality(String)", func(t *testing.T) {
		info, err := colMapping("string", "LowCardinality(String)")
		require.NoError(t, err)
		assert.Equal(t, "*column.LowCardinality[string]", info.fieldType)
		assert.Equal(t, "column.NewString().LowCardinality()", info.constructor)
		assert.Equal(t, "Append", info.appendMethod)
		assert.Equal(t, "Row", info.rowMethod)
	})
}

// TestColMapping_DateTime covers time.Time/DateTime and uint32/DateTime.
func TestColMapping_DateTime(t *testing.T) {
	t.Run("time.Time/DateTime", func(t *testing.T) {
		info, err := colMapping("time.Time", "DateTime")
		require.NoError(t, err)
		assert.Equal(t, "*column.Date[types.DateTime]", info.fieldType)
		assert.Equal(t, "column.NewDate[types.DateTime]()", info.constructor)
		assert.False(t, info.needsStrictFalse)
	})

	t.Run("time.Time/Date", func(t *testing.T) {
		info, err := colMapping("time.Time", "Date")
		require.NoError(t, err)
		assert.Equal(t, "*column.Date[types.Date]", info.fieldType)
		assert.Equal(t, "column.NewDate[types.Date]()", info.constructor)
	})

	t.Run("time.Time/Date32", func(t *testing.T) {
		info, err := colMapping("time.Time", "Date32")
		require.NoError(t, err)
		assert.Equal(t, "*column.Date[types.Date32]", info.fieldType)
		assert.Equal(t, "column.NewDate[types.Date32]()", info.constructor)
	})

	t.Run("time.Time/DateTime64(3)", func(t *testing.T) {
		info, err := colMapping("time.Time", "DateTime64(3)")
		require.NoError(t, err)
		assert.Equal(t, "*column.Date[types.DateTime64]", info.fieldType)
		assert.Equal(t, "column.NewDate[types.DateTime64]()", info.constructor)
	})

	t.Run("uint32/DateTime", func(t *testing.T) {
		info, err := colMapping("uint32", "DateTime")
		require.NoError(t, err)
		assert.Equal(t, "*column.Base[uint32]", info.fieldType)
		assert.Equal(t, "column.New[uint32]()", info.constructor)
		assert.True(t, info.needsStrictFalse)
	})

	t.Run("uint16/Date", func(t *testing.T) {
		info, err := colMapping("uint16", "Date")
		require.NoError(t, err)
		assert.Equal(t, "*column.Base[uint16]", info.fieldType)
		assert.Equal(t, "column.New[uint16]()", info.constructor)
		assert.True(t, info.needsStrictFalse)
	})
}

// TestColMapping_Nullable covers *int64/Nullable(Int64) and SimpleAggregateFunction unwrapping.
func TestColMapping_Nullable(t *testing.T) {
	t.Run("*int64/Nullable(Int64)", func(t *testing.T) {
		info, err := colMapping("*int64", "Nullable(Int64)")
		require.NoError(t, err)
		assert.Equal(t, "*column.BaseNullable[int64]", info.fieldType)
		assert.Equal(t, "column.New[int64]().Nullable()", info.constructor)
		assert.True(t, info.isNullable)
		assert.Equal(t, "AppendP", info.appendMethod)
		assert.Equal(t, "RowP", info.rowMethod)
	})

	t.Run("*uint64/SimpleAggregateFunction(sum,Nullable(Int64))", func(t *testing.T) {
		// Note: SAF strips to Nullable(Int64), but goType is *uint64
		// The inner type after stripping SAF is Nullable(Int64) → inner is Int64 → expects int64
		// but goType is *uint64 → incompatible after stripping *
		// Let's use *int64 to match
		info, err := colMapping("*int64", "SimpleAggregateFunction(sum, Nullable(Int64))")
		require.NoError(t, err)
		assert.Equal(t, "*column.BaseNullable[int64]", info.fieldType)
		assert.Equal(t, "column.New[int64]().Nullable()", info.constructor)
		assert.True(t, info.isNullable)
		assert.Equal(t, "AppendP", info.appendMethod)
		assert.Equal(t, "RowP", info.rowMethod)
	})

	t.Run("*uint64/SimpleAggregateFunction(sum,Nullable(UInt64))", func(t *testing.T) {
		info, err := colMapping("*uint64", "SimpleAggregateFunction(sum, Nullable(UInt64))")
		require.NoError(t, err)
		assert.Equal(t, "*column.BaseNullable[uint64]", info.fieldType)
		assert.Equal(t, "column.New[uint64]().Nullable()", info.constructor)
		assert.True(t, info.isNullable)
	})
}

// TestColMapping_Array covers []uint16/Array(UInt16) and []string/Array(LowCardinality(String)).
func TestColMapping_Array(t *testing.T) {
	t.Run("[]uint16/Array(UInt16)", func(t *testing.T) {
		info, err := colMapping("[]uint16", "Array(UInt16)")
		require.NoError(t, err)
		assert.Equal(t, "*column.Array[uint16]", info.fieldType)
		assert.Equal(t, "column.New[uint16]().Array()", info.constructor)
		assert.Equal(t, "Append", info.appendMethod)
		assert.Equal(t, "Row", info.rowMethod)
	})

	t.Run("[]string/Array(LowCardinality(String))", func(t *testing.T) {
		info, err := colMapping("[]string", "Array(LowCardinality(String))")
		require.NoError(t, err)
		assert.Equal(t, "*column.Array[string]", info.fieldType)
		assert.Equal(t, "column.NewString().LowCardinality().Array()", info.constructor)
	})
}

// TestColMapping_Map covers map[string]string/Map(LowCardinality(String), LowCardinality(String)).
func TestColMapping_Map(t *testing.T) {
	t.Run("map[string]string/Map(LowCardinality(String),LowCardinality(String))", func(t *testing.T) {
		info, err := colMapping("map[string]string", "Map(LowCardinality(String), LowCardinality(String))")
		require.NoError(t, err)
		assert.Equal(t, "*column.Map[string, string]", info.fieldType)
		assert.Contains(t, info.constructor, "column.NewMap[string, string]")
		assert.Contains(t, info.constructor, "column.NewString().LowCardinality()")
	})

	t.Run("map[string]uint64/Map(String,UInt64)", func(t *testing.T) {
		info, err := colMapping("map[string]uint64", "Map(String, UInt64)")
		require.NoError(t, err)
		assert.Equal(t, "*column.Map[string, uint64]", info.fieldType)
		assert.Equal(t, "column.NewMap[string, uint64](column.NewString(), column.New[uint64]())", info.constructor)
	})
}

// TestColMapping_FixedString covers [2]byte/FixedString(2) and [2]byte/LowCardinality(FixedString(2)).
func TestColMapping_FixedString(t *testing.T) {
	t.Run("[2]byte/FixedString(2)", func(t *testing.T) {
		info, err := colMapping("[2]byte", "FixedString(2)")
		require.NoError(t, err)
		assert.Equal(t, "*column.Base[[2]byte]", info.fieldType)
		assert.Equal(t, "column.New[[2]byte]()", info.constructor)
		assert.Equal(t, "Append", info.appendMethod)
		assert.Equal(t, "Row", info.rowMethod)
	})

	t.Run("[2]byte/LowCardinality(FixedString(2))", func(t *testing.T) {
		info, err := colMapping("[2]byte", "LowCardinality(FixedString(2))")
		require.NoError(t, err)
		assert.Equal(t, "*column.LowCardinality[[2]byte]", info.fieldType)
		assert.Equal(t, "column.New[[2]byte]().LowCardinality()", info.constructor)
	})
}

// TestColMapping_Enum covers EventSource/Enum8('prebid' = 1) → Base[EventSource].
func TestColMapping_Enum(t *testing.T) {
	t.Run("EventSource/Enum8", func(t *testing.T) {
		info, err := colMapping("EventSource", "Enum8('prebid' = 1, 'dynamicAllocation' = 2)")
		require.NoError(t, err)
		assert.Equal(t, "*column.Base[EventSource]", info.fieldType)
		assert.Equal(t, "column.New[EventSource]()", info.constructor)
		assert.Equal(t, "Append", info.appendMethod)
		assert.Equal(t, "Row", info.rowMethod)
	})

	t.Run("MyEnum16/Enum16", func(t *testing.T) {
		info, err := colMapping("MyEnum16", "Enum16('a' = 1, 'b' = 2)")
		require.NoError(t, err)
		assert.Equal(t, "*column.Base[MyEnum16]", info.fieldType)
		assert.Equal(t, "column.New[MyEnum16]()", info.constructor)
	})
}

// TestColMapping_Invalid covers incompatible type combinations.
func TestColMapping_Invalid(t *testing.T) {
	t.Run("string/DateTime", func(t *testing.T) {
		_, err := colMapping("string", "DateTime")
		require.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "incompatible"), "expected 'incompatible' in error: %v", err)
	})

	t.Run("int64/String", func(t *testing.T) {
		_, err := colMapping("int64", "String")
		require.Error(t, err)
	})

	t.Run("string/UInt64", func(t *testing.T) {
		_, err := colMapping("string", "UInt64")
		require.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "incompatible"), "expected 'incompatible' in error: %v", err)
	})

	t.Run("int64/Nullable(Int64) missing pointer", func(t *testing.T) {
		_, err := colMapping("int64", "Nullable(Int64)")
		require.Error(t, err)
	})
}

// TestColMapping_Decimal covers Decimal32/64/128/256 with their chtype variants.
func TestColMapping_Decimal(t *testing.T) {
	cases := []struct {
		goType    string
		chType    string
		wantField string
		wantCtor  string
	}{
		{"types.Decimal32", "Decimal32(3)", "*column.Base[types.Decimal32]", "column.New[types.Decimal32]()"},
		{"types.Decimal64", "Decimal64(6)", "*column.Base[types.Decimal64]", "column.New[types.Decimal64]()"},
		{"types.Decimal128", "Decimal128(18)", "*column.Base[types.Decimal128]", "column.New[types.Decimal128]()"},
		{"types.Decimal256", "Decimal256(38)", "*column.Base[types.Decimal256]", "column.New[types.Decimal256]()"},
		// Decimal(P,S) forms — goType determined by precision
		{"types.Decimal128", "Decimal(38, 10)", "*column.Base[types.Decimal128]", "column.New[types.Decimal128]()"},
		// Bare forms (no scale parameter)
		{"types.Decimal128", "Decimal128", "*column.Base[types.Decimal128]", "column.New[types.Decimal128]()"},
		{"types.Decimal256", "Decimal256", "*column.Base[types.Decimal256]", "column.New[types.Decimal256]()"},
	}

	for _, tc := range cases {
		t.Run(tc.goType+"/"+tc.chType, func(t *testing.T) {
			info, err := colMapping(tc.goType, tc.chType)
			require.NoError(t, err)
			assert.Equal(t, tc.wantField, info.fieldType)
			assert.Equal(t, tc.wantCtor, info.constructor)
			assert.Equal(t, "Append", info.appendMethod)
			assert.Equal(t, "Row", info.rowMethod)
		})
	}
}

// TestColMapping_JSON covers json.RawMessage/JSON mapping.
func TestColMapping_JSON(t *testing.T) {
	t.Run("json.RawMessage/JSON", func(t *testing.T) {
		info, err := colMapping("json.RawMessage", "JSON")
		require.NoError(t, err)
		assert.Equal(t, "*column.JSON", info.fieldType)
		assert.Equal(t, "column.NewJSON()", info.constructor)
		assert.Equal(t, "Append", info.appendMethod)
		assert.Equal(t, "Row", info.rowMethod)
	})

	t.Run("json.RawMessage/JSON(a String)", func(t *testing.T) {
		info, err := colMapping("json.RawMessage", "JSON(a String, max_dynamic_paths=16)")
		require.NoError(t, err)
		assert.Equal(t, "*column.JSON", info.fieldType)
		assert.Equal(t, "column.NewJSON()", info.constructor)
	})

	t.Run("json.RawMessage/String incompatible", func(t *testing.T) {
		_, err := colMapping("json.RawMessage", "String")
		require.Error(t, err)
	})
}

// TestColMapping_Point covers Point, Ring, Polygon, MultiPolygon geo types.
func TestColMapping_Point(t *testing.T) {
	t.Run("types.Point/Point", func(t *testing.T) {
		info, err := colMapping("types.Point", "Point")
		require.NoError(t, err)
		assert.Equal(t, "*column.Tuple2[types.Point, float64, float64]", info.fieldType)
		assert.Equal(t, "column.NewPoint()", info.constructor)
		assert.Equal(t, "Append", info.appendMethod)
		assert.Equal(t, "Row", info.rowMethod)
	})

	t.Run("[]types.Point/Ring", func(t *testing.T) {
		info, err := colMapping("[]types.Point", "Ring")
		require.NoError(t, err)
		assert.Equal(t, "*column.Array[types.Point]", info.fieldType)
		assert.Equal(t, "column.NewPoint().Array()", info.constructor)
	})

	t.Run("[][]types.Point/Polygon", func(t *testing.T) {
		info, err := colMapping("[][]types.Point", "Polygon")
		require.NoError(t, err)
		assert.Equal(t, "*column.Array2[types.Point]", info.fieldType)
		assert.Equal(t, "column.NewPoint().Array().Array()", info.constructor)
	})

	t.Run("[][][]types.Point/MultiPolygon", func(t *testing.T) {
		info, err := colMapping("[][][]types.Point", "MultiPolygon")
		require.NoError(t, err)
		assert.Equal(t, "*column.Array3[types.Point]", info.fieldType)
		assert.Equal(t, "column.NewPoint().Array().Array().Array()", info.constructor)
	})
}

// TestColMapping_NullableDateTime covers *time.Time/Nullable(DateTime) etc.
func TestColMapping_NullableDateTime(t *testing.T) {
	t.Run("*time.Time/Nullable(DateTime)", func(t *testing.T) {
		info, err := colMapping("*time.Time", "Nullable(DateTime)")
		require.NoError(t, err)
		assert.Equal(t, "*column.DateNullable[types.DateTime]", info.fieldType)
		assert.Equal(t, "column.NewDate[types.DateTime]().Nullable()", info.constructor)
		assert.True(t, info.isNullable)
		assert.Equal(t, "AppendP", info.appendMethod)
		assert.Equal(t, "RowP", info.rowMethod)
	})

	t.Run("*time.Time/Nullable(Date)", func(t *testing.T) {
		info, err := colMapping("*time.Time", "Nullable(Date)")
		require.NoError(t, err)
		assert.Equal(t, "*column.DateNullable[types.Date]", info.fieldType)
		assert.Equal(t, "column.NewDate[types.Date]().Nullable()", info.constructor)
	})

	t.Run("*time.Time/Nullable(Date32)", func(t *testing.T) {
		info, err := colMapping("*time.Time", "Nullable(Date32)")
		require.NoError(t, err)
		assert.Equal(t, "*column.DateNullable[types.Date32]", info.fieldType)
		assert.Equal(t, "column.NewDate[types.Date32]().Nullable()", info.constructor)
	})

	t.Run("*time.Time/Nullable(DateTime64(3))", func(t *testing.T) {
		info, err := colMapping("*time.Time", "Nullable(DateTime64(3))")
		require.NoError(t, err)
		assert.Equal(t, "*column.DateNullable[types.DateTime64]", info.fieldType)
		assert.Equal(t, "column.NewDate[types.DateTime64]().Nullable()", info.constructor)
	})
}

// TestColMapping_ArrayNullable covers []*int64/Array(Nullable(Int64)).
func TestColMapping_ArrayNullable(t *testing.T) {
	t.Run("[]*int64/Array(Nullable(Int64))", func(t *testing.T) {
		info, err := colMapping("[]*int64", "Array(Nullable(Int64))")
		require.NoError(t, err)
		assert.Equal(t, "*column.ArrayNullable[int64]", info.fieldType)
		assert.Equal(t, "column.New[int64]().Nullable().Array()", info.constructor)
		assert.Equal(t, "AppendP", info.appendMethod)
		assert.Equal(t, "RowP", info.rowMethod)
	})

	t.Run("[]*string/Array(Nullable(String))", func(t *testing.T) {
		info, err := colMapping("[]*string", "Array(Nullable(String))")
		require.NoError(t, err)
		assert.Equal(t, "*column.ArrayNullable[string]", info.fieldType)
		assert.Equal(t, "column.NewString().Nullable().Array()", info.constructor)
		assert.Equal(t, "AppendP", info.appendMethod)
		assert.Equal(t, "RowP", info.rowMethod)
	})
}

// TestColMapping_NestedArray covers [][]string/Array(Array(String)).
func TestColMapping_NestedArray(t *testing.T) {
	t.Run("[][]string/Array(Array(String))", func(t *testing.T) {
		info, err := colMapping("[][]string", "Array(Array(String))")
		require.NoError(t, err)
		assert.Equal(t, "*column.Array2[string]", info.fieldType)
		assert.Equal(t, "column.NewString().Array().Array()", info.constructor)
		assert.Equal(t, "Append", info.appendMethod)
		assert.Equal(t, "Row", info.rowMethod)
	})

	t.Run("[][]int64/Array(Array(Int64))", func(t *testing.T) {
		info, err := colMapping("[][]int64", "Array(Array(Int64))")
		require.NoError(t, err)
		assert.Equal(t, "*column.Array2[int64]", info.fieldType)
		assert.Equal(t, "column.New[int64]().Array().Array()", info.constructor)
	})

	t.Run("[][][]int64/Array(Array(Array(Int64)))", func(t *testing.T) {
		info, err := colMapping("[][][]int64", "Array(Array(Array(Int64)))")
		require.NoError(t, err)
		assert.Equal(t, "*column.Array3[int64]", info.fieldType)
		assert.Equal(t, "column.New[int64]().Array().Array().Array()", info.constructor)
	})
}

// TestColMapping_LCNullable covers *string/LowCardinality(Nullable(String)).
func TestColMapping_LCNullable(t *testing.T) {
	t.Run("*string/LowCardinality(Nullable(String))", func(t *testing.T) {
		info, err := colMapping("*string", "LowCardinality(Nullable(String))")
		require.NoError(t, err)
		assert.Equal(t, "*column.LowCardinalityNullable[string]", info.fieldType)
		assert.Equal(t, "column.NewString().LowCardinality().Nullable()", info.constructor)
		assert.True(t, info.isNullable)
		assert.Equal(t, "AppendP", info.appendMethod)
		assert.Equal(t, "RowP", info.rowMethod)
	})
}

// TestColMapping_MapNullable covers map[string]*int64/Map(String, Nullable(Int64)).
func TestColMapping_MapNullable(t *testing.T) {
	t.Run("map[string]*int64/Map(String,Nullable(Int64))", func(t *testing.T) {
		info, err := colMapping("map[string]*int64", "Map(String, Nullable(Int64))")
		require.NoError(t, err)
		assert.Equal(t, "*column.MapNullable[string, int64]", info.fieldType)
		assert.Equal(t, "column.NewMapNullable[string, int64](column.NewString(), column.New[int64]().Nullable())", info.constructor)
		assert.Equal(t, "AppendP", info.appendMethod)
		assert.Equal(t, "RowP", info.rowMethod)
	})
}

// TestColMapping_Tuple verifies that Tuple and Nested chtypes are recognized.
func TestColMapping_Tuple(t *testing.T) {
	t.Run("struct/Tuple", func(t *testing.T) {
		info, err := colMapping("Address", "Tuple(city String, zip_code Int32)")
		require.NoError(t, err)
		assert.True(t, info.isTuple)
		assert.Equal(t, "*column.Tuple", info.fieldType)
		assert.Equal(t, "Address", info.goType)
	})

	t.Run("slice/Nested", func(t *testing.T) {
		info, err := colMapping("[]Phone", "Nested(number String, type Int8)")
		require.NoError(t, err)
		assert.True(t, info.isNested)
		assert.Equal(t, "*column.ArrayBase", info.fieldType)
		assert.Equal(t, "Phone", info.goType)
	})

	t.Run("non-slice/Nested error", func(t *testing.T) {
		_, err := colMapping("Phone", "Nested(number String, type Int8)")
		require.Error(t, err)
	})
}

// TestColMapping_BFloat16 verifies that types.BFloat16/BFloat16 maps to Base[types.BFloat16].
func TestColMapping_BFloat16(t *testing.T) {
	result, err := colMapping("types.BFloat16", "BFloat16")
	require.NoError(t, err)
	assert.Equal(t, "*column.Base[types.BFloat16]", result.fieldType)
	assert.Equal(t, "column.New[types.BFloat16]()", result.constructor)
}

// TestColMapping_ChTime verifies that types.ChTime/Time maps to Base[types.ChTime].
func TestColMapping_ChTime(t *testing.T) {
	result, err := colMapping("types.ChTime", "Time")
	require.NoError(t, err)
	assert.Equal(t, "*column.Base[types.ChTime]", result.fieldType)
	assert.Equal(t, "column.New[types.ChTime]()", result.constructor)
}

func TestColMapping_ChTime64(t *testing.T) {
	result, err := colMapping("types.ChTime64", "Time64(3)")
	require.NoError(t, err)
	assert.Equal(t, "*column.Base[types.ChTime64]", result.fieldType)
	assert.Equal(t, "column.New[types.ChTime64]()", result.constructor)
}
