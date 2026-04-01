package testdata

import (
	"time"

	"github.com/vahid-sohrabloo/chconn/v3/types"
)

type AllTypesEvent int8

const (
	AllTypesEventImpression AllTypesEvent = 1
	AllTypesEventPageview   AllTypesEvent = 2
)

type AllTypesSource int16

const (
	AllTypesSourcePrebid AllTypesSource = 1
	AllTypesSourceServer AllTypesSource = 2
)

//go:generate go tool chgen columns --with-iter
type AllTypes struct {
	// Primitives
	ColInt8    int8    `db:"col_int8" chtype:"Int8"`
	ColInt16   int16   `db:"col_int16" chtype:"Int16"`
	ColInt32   int32   `db:"col_int32" chtype:"Int32"`
	ColInt64   int64   `db:"col_int64" chtype:"Int64"`
	ColUint8   uint8   `db:"col_uint8" chtype:"UInt8"`
	ColUint16  uint16  `db:"col_uint16" chtype:"UInt16"`
	ColUint32  uint32  `db:"col_uint32" chtype:"UInt32"`
	ColUint64  uint64  `db:"col_uint64" chtype:"UInt64"`
	ColFloat32 float32 `db:"col_float32" chtype:"Float32"`
	ColFloat64 float64 `db:"col_float64" chtype:"Float64"`
	ColBool    bool    `db:"col_bool" chtype:"Bool"`
	ColString  string  `db:"col_string" chtype:"String"`

	// Date/Time as time.Time (native)
	ColDate       time.Time `db:"col_date" chtype:"Date"`
	ColDate32     time.Time `db:"col_date32" chtype:"Date32"`
	ColDateTime   time.Time `db:"col_datetime" chtype:"DateTime"`
	ColDateTime64 time.Time `db:"col_datetime64" chtype:"DateTime64(3)"`

	// Date/Time as uint (non-native, needs SetStrict(false))
	ColDateUint     uint16 `db:"col_date_uint" chtype:"Date"`
	ColDateTimeUint uint32 `db:"col_datetime_uint" chtype:"DateTime"`

	// UUID, IPv4, IPv6
	ColUUID types.UUID `db:"col_uuid" chtype:"UUID"`
	ColIPv4 types.IPv4 `db:"col_ipv4" chtype:"IPv4"`
	ColIPv6 types.IPv6 `db:"col_ipv6" chtype:"IPv6"`

	// FixedString
	ColFixed2  [2]byte  `db:"col_fixed2" chtype:"FixedString(2)"`
	ColFixed16 [16]byte `db:"col_fixed16" chtype:"FixedString(16)"`

	// LowCardinality
	ColLCString string  `db:"col_lc_string" chtype:"LowCardinality(String)"`
	ColLCFixed  [2]byte `db:"col_lc_fixed" chtype:"LowCardinality(FixedString(2))"`

	// Nullable
	ColNullInt64   *int64   `db:"col_null_int64" chtype:"Nullable(Int64)"`
	ColNullString  *string  `db:"col_null_string" chtype:"Nullable(String)"`
	ColNullFloat32 *float32 `db:"col_null_float32" chtype:"Nullable(Float32)"`

	// SimpleAggregateFunction
	ColSAFMax      float32 `db:"col_saf_max" chtype:"SimpleAggregateFunction(max, Float32)"`
	ColSAFNullable *int64  `db:"col_saf_nullable" chtype:"SimpleAggregateFunction(sum, Nullable(Int64))"`

	// Arrays
	ColArrayUint16 []uint16 `db:"col_array_uint16" chtype:"Array(UInt16)"`
	ColArrayString []string `db:"col_array_string" chtype:"Array(String)"`
	ColArrayLCStr  []string `db:"col_array_lc_str" chtype:"Array(LowCardinality(String))"`
	ColArrayInt64  []int64  `db:"col_array_int64" chtype:"Array(Int64)"`

	// Map
	ColMapStrStr map[string]string `db:"col_map_str_str" chtype:"Map(String, String)"`
	ColMapLCStr  map[string]string `db:"col_map_lc_str" chtype:"Map(LowCardinality(String), LowCardinality(String))"`

	// Enum
	ColEnum8  AllTypesEvent  `db:"col_enum8" chtype:"Enum8('impression' = 1, 'pageview' = 2)"`
	ColEnum16 AllTypesSource `db:"col_enum16" chtype:"Enum16('prebid' = 1, 'server' = 2)"`

	// Big numbers
	ColInt128  types.Int128  `db:"col_int128" chtype:"Int128"`
	ColInt256  types.Int256  `db:"col_int256" chtype:"Int256"`
	ColUint128 types.Uint128 `db:"col_uint128" chtype:"UInt128"`
	ColUint256 types.Uint256 `db:"col_uint256" chtype:"UInt256"`

	// Fields to skip
	IgnoredNoTag    int                 // no tags at all
	IgnoredNoDb     int                 `chtype:"UInt32"`        // chtype but no db
	IgnoredDbDash   int                 `db:"-" chtype:"UInt32"` // db is "-"
	IgnoredNoChtype int                 `db:"something"`         // db but no chtype
	IgnoredPrivate  map[string]struct{} // unexportable type, no tags
}
