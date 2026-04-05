package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChTypeToGo_Primitives covers all primitive scalar types.
func TestChTypeToGo_Primitives(t *testing.T) {
	cases := []struct {
		chType string
		want   string
	}{
		{"Int8", "int8"},
		{"Int16", "int16"},
		{"Int32", "int32"},
		{"Int64", "int64"},
		{"UInt8", "uint8"},
		{"UInt16", "uint16"},
		{"UInt32", "uint32"},
		{"UInt64", "uint64"},
		{"Float32", "float32"},
		{"Float64", "float64"},
		{"String", "string"},
		{"Bool", "bool"},
		{"UUID", "types.UUID"},
		{"IPv4", "types.IPv4"},
		{"IPv6", "types.IPv6"},
		{"Int128", "types.Int128"},
		{"Int256", "types.Int256"},
		{"UInt128", "types.Uint128"},
		{"UInt256", "types.Uint256"},
	}

	for _, tc := range cases {
		t.Run(tc.chType, func(t *testing.T) {
			info, err := chTypeToGo(tc.chType, false)
			require.NoError(t, err)
			assert.Equal(t, tc.want, info.goType)
			assert.False(t, info.isEnum)
		})
	}
}

// TestChTypeToGo_DateTime covers date and time types with and without timeAsUint.
func TestChTypeToGo_DateTime(t *testing.T) {
	cases := []struct {
		chType     string
		timeAsUint bool
		want       string
	}{
		// timeAsUint = false — all resolve to time.Time
		{"Date", false, "time.Time"},
		{"Date32", false, "time.Time"},
		{"DateTime", false, "time.Time"},
		{"DateTime64(3)", false, "time.Time"},
		{"DateTime('UTC')", false, "time.Time"},
		{"DateTime('America/New_York')", false, "time.Time"},

		// timeAsUint = true — resolve to underlying integer type
		{"Date", true, "uint16"},
		{"Date32", true, "int32"},
		{"DateTime", true, "uint32"},
		{"DateTime64(3)", true, "int64"},
		{"DateTime('UTC')", true, "uint32"},
	}

	for _, tc := range cases {
		tc := tc
		name := tc.chType
		if tc.timeAsUint {
			name += "/timeAsUint"
		}
		t.Run(name, func(t *testing.T) {
			info, err := chTypeToGo(tc.chType, tc.timeAsUint)
			require.NoError(t, err)
			assert.Equal(t, tc.want, info.goType)
		})
	}
}

// TestChTypeToGo_Wrappers covers Nullable, LowCardinality, and Array wrappers.
func TestChTypeToGo_Wrappers(t *testing.T) {
	t.Run("Nullable(Int64)", func(t *testing.T) {
		info, err := chTypeToGo("Nullable(Int64)", false)
		require.NoError(t, err)
		assert.Equal(t, "*int64", info.goType)
	})

	t.Run("Nullable(String)", func(t *testing.T) {
		info, err := chTypeToGo("Nullable(String)", false)
		require.NoError(t, err)
		assert.Equal(t, "*string", info.goType)
	})

	t.Run("LowCardinality(String)", func(t *testing.T) {
		info, err := chTypeToGo("LowCardinality(String)", false)
		require.NoError(t, err)
		assert.Equal(t, "string", info.goType)
	})

	t.Run("Array(UInt16)", func(t *testing.T) {
		info, err := chTypeToGo("Array(UInt16)", false)
		require.NoError(t, err)
		assert.Equal(t, "[]uint16", info.goType)
	})

	t.Run("Array(Array(String))", func(t *testing.T) {
		info, err := chTypeToGo("Array(Array(String))", false)
		require.NoError(t, err)
		assert.Equal(t, "[][]string", info.goType)
	})

	t.Run("Nullable(DateTime)", func(t *testing.T) {
		info, err := chTypeToGo("Nullable(DateTime)", false)
		require.NoError(t, err)
		assert.Equal(t, "*time.Time", info.goType)
	})
}

// TestChTypeToGo_FixedString covers FixedString(N) → [N]byte.
func TestChTypeToGo_FixedString(t *testing.T) {
	cases := []struct {
		chType string
		want   string
	}{
		{"FixedString(2)", "[2]byte"},
		{"FixedString(16)", "[16]byte"},
		{"FixedString(32)", "[32]byte"},
	}

	for _, tc := range cases {
		t.Run(tc.chType, func(t *testing.T) {
			info, err := chTypeToGo(tc.chType, false)
			require.NoError(t, err)
			assert.Equal(t, tc.want, info.goType)
		})
	}
}

// TestChTypeToGo_SimpleAggregateFunction covers SimpleAggregateFunction unwrapping.
func TestChTypeToGo_SimpleAggregateFunction(t *testing.T) {
	t.Run("sum,Nullable(Int64)", func(t *testing.T) {
		info, err := chTypeToGo("SimpleAggregateFunction(sum, Nullable(Int64))", false)
		require.NoError(t, err)
		assert.Equal(t, "*int64", info.goType)
	})

	t.Run("max,Float32", func(t *testing.T) {
		info, err := chTypeToGo("SimpleAggregateFunction(max, Float32)", false)
		require.NoError(t, err)
		assert.Equal(t, "float32", info.goType)
	})

	t.Run("anyLast,String", func(t *testing.T) {
		info, err := chTypeToGo("SimpleAggregateFunction(anyLast, String)", false)
		require.NoError(t, err)
		assert.Equal(t, "string", info.goType)
	})
}

// TestChTypeToGo_Map covers Map(K, V) including nested LowCardinality wrappers.
func TestChTypeToGo_Map(t *testing.T) {
	t.Run("Map(String,String)", func(t *testing.T) {
		info, err := chTypeToGo("Map(String, String)", false)
		require.NoError(t, err)
		assert.Equal(t, "map[string]string", info.goType)
	})

	t.Run("Map(LowCardinality(String),LowCardinality(String))", func(t *testing.T) {
		info, err := chTypeToGo("Map(LowCardinality(String), LowCardinality(String))", false)
		require.NoError(t, err)
		assert.Equal(t, "map[string]string", info.goType)
	})

	t.Run("Map(String,UInt64)", func(t *testing.T) {
		info, err := chTypeToGo("Map(String, UInt64)", false)
		require.NoError(t, err)
		assert.Equal(t, "map[string]uint64", info.goType)
	})
}

// TestChTypeToGo_Enum covers Enum8 and Enum16 parsing.
func TestChTypeToGo_Enum(t *testing.T) {
	t.Run("Enum8", func(t *testing.T) {
		chType := "Enum8('prebid' = 1, 'dynamicAllocation' = 2)"
		info, err := chTypeToGo(chType, false)
		require.NoError(t, err)
		assert.Equal(t, "int8", info.goType)
		assert.True(t, info.isEnum)
		assert.Equal(t, map[string]int{"prebid": 1, "dynamicAllocation": 2}, info.enumValues)
	})

	t.Run("Enum16", func(t *testing.T) {
		chType := "Enum16('a' = 100, 'b' = 200, 'c' = 300)"
		info, err := chTypeToGo(chType, false)
		require.NoError(t, err)
		assert.Equal(t, "int16", info.goType)
		assert.True(t, info.isEnum)
		assert.Equal(t, map[string]int{"a": 100, "b": 200, "c": 300}, info.enumValues)
	})

	t.Run("Enum8 negative values", func(t *testing.T) {
		chType := "Enum8('neg' = -1, 'zero' = 0, 'pos' = 1)"
		info, err := chTypeToGo(chType, false)
		require.NoError(t, err)
		assert.Equal(t, "int8", info.goType)
		assert.True(t, info.isEnum)
		assert.Equal(t, map[string]int{"neg": -1, "zero": 0, "pos": 1}, info.enumValues)
	})
}

// TestChTypeToGo_Geo covers Point, Ring, Polygon, MultiPolygon types.
func TestChTypeToGo_Geo(t *testing.T) {
	cases := []struct {
		chType string
		want   string
	}{
		{"Point", "types.Point"},
		{"Ring", "[]types.Point"},
		{"Polygon", "[][]types.Point"},
		{"MultiPolygon", "[][][]types.Point"},
	}
	for _, tc := range cases {
		t.Run(tc.chType, func(t *testing.T) {
			info, err := chTypeToGo(tc.chType, false)
			require.NoError(t, err)
			assert.Equal(t, tc.want, info.goType)
		})
	}
}

// TestChTypeToGo_NullableDateTime verifies Nullable(DateTime) → *time.Time.
func TestChTypeToGo_NullableDateTime(t *testing.T) {
	cases := []struct {
		chType string
		want   string
	}{
		{"Nullable(DateTime)", "*time.Time"},
		{"Nullable(Date)", "*time.Time"},
		{"Nullable(Date32)", "*time.Time"},
		{"Nullable(DateTime64(3))", "*time.Time"},
	}
	for _, tc := range cases {
		t.Run(tc.chType, func(t *testing.T) {
			info, err := chTypeToGo(tc.chType, false)
			require.NoError(t, err)
			assert.Equal(t, tc.want, info.goType)
		})
	}
}

// TestChTypeToGo_ArrayNullable verifies Array(Nullable(T)) → []*T.
func TestChTypeToGo_ArrayNullable(t *testing.T) {
	info, err := chTypeToGo("Array(Nullable(Int64))", false)
	require.NoError(t, err)
	assert.Equal(t, "[]*int64", info.goType)
}

// TestChTypeToGo_NestedArray verifies Array(Array(T)) → [][]T.
func TestChTypeToGo_NestedArray(t *testing.T) {
	info, err := chTypeToGo("Array(Array(String))", false)
	require.NoError(t, err)
	assert.Equal(t, "[][]string", info.goType)
}

// TestChTypeToGo_LCNullable verifies LowCardinality(Nullable(String)) → *string.
func TestChTypeToGo_LCNullable(t *testing.T) {
	info, err := chTypeToGo("LowCardinality(Nullable(String))", false)
	require.NoError(t, err)
	assert.Equal(t, "*string", info.goType)
}

// TestChTypeToGo_MapNullable verifies Map(String, Nullable(Int64)) → map[string]*int64.
func TestChTypeToGo_MapNullable(t *testing.T) {
	info, err := chTypeToGo("Map(String, Nullable(Int64))", false)
	require.NoError(t, err)
	assert.Equal(t, "map[string]*int64", info.goType)
}

// TestChTypeToGo_Errors verifies that unknown types produce an error.
func TestChTypeToGo_Errors(t *testing.T) {
	_, err := chTypeToGo("UnknownType", false)
	assert.Error(t, err)

	_, err = chTypeToGo("FixedString(0)", false)
	assert.Error(t, err)

	_, err = chTypeToGo("FixedString(abc)", false)
	assert.Error(t, err)
}

// TestChTypeToGo_Decimal covers Decimal32/64/128/256 with scale parameter and Decimal(P,S).
func TestChTypeToGo_Decimal(t *testing.T) {
	tests := []struct {
		chType string
		goType string
	}{
		// Short forms with scale
		{"Decimal32(3)", "types.Decimal32"},
		{"Decimal64(6)", "types.Decimal64"},
		{"Decimal128(18)", "types.Decimal128"},
		{"Decimal256(38)", "types.Decimal256"},
		// Bare forms (no scale)
		{"Decimal32", "types.Decimal32"},
		{"Decimal64", "types.Decimal64"},
		{"Decimal128", "types.Decimal128"},
		{"Decimal256", "types.Decimal256"},
		// Decimal(P,S) — precision-based dispatch
		{"Decimal(9, 3)", "types.Decimal32"},
		{"Decimal(18, 6)", "types.Decimal64"},
		{"Decimal(38, 10)", "types.Decimal128"},
		{"Decimal(76, 20)", "types.Decimal256"},
	}
	for _, tt := range tests {
		t.Run(tt.chType, func(t *testing.T) {
			result, err := chTypeToGo(tt.chType, false)
			require.NoError(t, err)
			assert.Equal(t, tt.goType, result.goType)
		})
	}
}

// TestChTypeToGo_JSON verifies that JSON maps to json.RawMessage.
func TestChTypeToGo_JSON(t *testing.T) {
	result, err := chTypeToGo("JSON", false)
	require.NoError(t, err)
	assert.Equal(t, "json.RawMessage", result.goType)
}

// TestChTypeToGo_Tuple verifies that Tuple(...) maps to any.
func TestChTypeToGo_Tuple(t *testing.T) {
	result, err := chTypeToGo("Tuple(String, Int64)", false)
	require.NoError(t, err)
	assert.Equal(t, "any", result.goType)
}

// TestChTypeToGo_Nested verifies that Nested(...) maps to any.
func TestChTypeToGo_Nested(t *testing.T) {
	result, err := chTypeToGo("Nested(name String, value Int64)", false)
	require.NoError(t, err)
	assert.Equal(t, "any", result.goType)
}
