package column_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/format"
	"github.com/vahid-sohrabloo/chconn/v3/types"
)

func TestBool(t *testing.T) {
	testColumn(t, false, true, "UInt8", "bool", func(i int) bool {
		return true
	}, func(i int) bool {
		return false
	})
}

func TestUint8(t *testing.T) {
	testColumn(t, false, true, "UInt8", "uint8", func(i int) uint8 {
		return uint8(i)
	}, func(i int) uint8 {
		return uint8(i + 1)
	})
}

func TestUint16(t *testing.T) {
	testColumn(t, false, true, "UInt16", "uint16", func(i int) uint16 {
		return uint16(i)
	}, func(i int) uint16 {
		return uint16(i + 1)
	})
}

func TestUint32(t *testing.T) {
	testColumn(t, false, true, "UInt32", "uint32", func(i int) uint32 {
		return uint32(i)
	}, func(i int) uint32 {
		return uint32(i + 1)
	})
}

func TestUint64(t *testing.T) {
	testColumn(t, false, true, "UInt64", "uint64", func(i int) uint64 {
		return uint64(i)
	}, func(i int) uint64 {
		return uint64(i + 1)
	})
}

func TestUint128(t *testing.T) {
	testColumn(t, false, true, "UInt128", "uint128", func(i int) types.Uint128 {
		return types.Uint128FromBig(big.NewInt(int64(i)))
	}, func(i int) types.Uint128 {
		x := big.NewInt(int64(i))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Uint128FromBig(x)
	})
}

func TestUint256(t *testing.T) {
	testColumn(t, false, true, "UInt256", "uint256", func(i int) types.Uint256 {
		return types.Uint256FromBig(big.NewInt(int64(i)))
	}, func(i int) types.Uint256 {
		x := big.NewInt(int64(i))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Uint256FromBig(x)
	})
}

func TestInt8(t *testing.T) {
	testColumn(t, false, true, "Int8", "int8", func(i int) int8 {
		return int8(i)
	}, func(i int) int8 {
		return int8(i * -1)
	})
}

func TestInt16(t *testing.T) {
	testColumn(t, false, true, "Int16", "int16", func(i int) int16 {
		return int16(i)
	}, func(i int) int16 {
		return int16(i * -1)
	})
}

func TestInt32(t *testing.T) {
	testColumn(t, false, true, "Int32", "int32", func(i int) int32 {
		return int32(i)
	}, func(i int) int32 {
		return int32(i * -1)
	})
}

func TestInt64(t *testing.T) {
	testColumn(t, false, true, "Int64", "int64", func(i int) int64 {
		return int64(i)
	}, func(i int) int64 {
		return int64(i * -1)
	})
}

func TestInt128(t *testing.T) {
	testColumn(t, false, true, "Int128", "int128", func(i int) types.Int128 {
		return types.Int128FromBig(big.NewInt(int64(i * -1)))
	}, func(i int) types.Int128 {
		x := big.NewInt(int64(i) * -1)
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Int128FromBig(x)
	})
}

func TestInt256(t *testing.T) {
	testColumn(t, false, true, "Int256", "int256", func(i int) types.Int256 {
		return types.Int256FromBig(big.NewInt(int64(i)))
	}, func(i int) types.Int256 {
		x := big.NewInt(int64(i) * -1)
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Int256FromBig(x)
	})
}
func TestFixedString(t *testing.T) {
	testColumn(t, false, true, "FixedString(2)", "fixedString", func(i int) [2]byte {
		return [2]byte{byte(i), byte(i + 1)}
	}, func(i int) [2]byte {
		return [2]byte{byte(i + 1), byte(i + 2)}
	})
}

func TestFloat32(t *testing.T) {
	testColumn(t, false, true, "Float32", "float32", func(i int) float32 {
		return float32(i)
	}, func(i int) float32 {
		return float32(i * -1)
	})
}

func TestFloat64(t *testing.T) {
	testColumn(t, false, true, "Float64", "float64", func(i int) float64 {
		return float64(i)
	}, func(i int) float64 {
		return float64(i * -1)
	})
}

func TestDecimal32(t *testing.T) {
	testColumn(t, false, false, "Decimal32(3)", "decimal32", func(i int) types.Decimal32 {
		return types.Decimal32(i)
	}, func(i int) types.Decimal32 {
		return types.Decimal32(i * -1)
	})
}
func TestDecimal64(t *testing.T) {
	testColumn(t, false, false, "Decimal64(3)", "decimal64", func(i int) types.Decimal64 {
		return types.Decimal64(i)
	}, func(i int) types.Decimal64 {
		return types.Decimal64(i * -1)
	})
}

func TestDecimal128(t *testing.T) {
	testColumn(t, false, false, "Decimal128(3)", "decimal128", func(i int) types.Decimal128 {
		return types.Decimal128(types.Int128FromBig(big.NewInt(int64(i))))
	}, func(i int) types.Decimal128 {
		return types.Decimal128(types.Int128FromBig(big.NewInt(int64(i * -1))))
	})
}

func TestDecimal256(t *testing.T) {
	testColumn(t, false, false, "Decimal256(3)", "decimal256", func(i int) types.Decimal256 {
		return types.Decimal256(types.Int256FromBig(big.NewInt(int64(i))))
	}, func(i int) types.Decimal256 {
		return types.Decimal256(types.Int256FromBig(big.NewInt(int64(i * -1))))
	})
}

func TestIPv4(t *testing.T) {
	testColumn(t, false, true, "IPv4", "ipv4", func(i int) types.IPv4 {
		// or directly return types.IPv4
		return types.IPv4FromAddr(netip.AddrFrom4([4]byte{0, 0, 0, byte(i)}))
	}, func(i int) types.IPv4 {
		// or directly return types.IPv4
		return types.IPv4FromAddr(netip.AddrFrom4([4]byte{0, 0, byte(i), 0}))
	})
}

func TestIPv6(t *testing.T) {
	testColumn(t, false, true, "IPv6", "ipv6", func(i int) types.IPv6 {
		// or directly return types.IPv6
		return types.IPv6FromAddr(netip.MustParseAddr("2001:0db8:85a3:0000:0000:8a2e:0370:7334"))
	}, func(i int) types.IPv6 {
		// or directly return types.IPv6
		return types.IPv6FromAddr(netip.AddrFrom16([16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, byte(i + 1)}))
	})
}

func TestUUID(t *testing.T) {
	testColumn(t, false, true, "UUID", "uuid", func(i int) types.UUID {
		return types.UUIDFromBigEndian(uuid.New())
	}, func(i int) types.UUID {
		return types.UUIDFromBigEndian(uuid.New())
	})
}

func TestBoolWithDelete(t *testing.T) {
	testColumn(t, true, true, "UInt8", "bool", func(i int) bool {
		return true
	}, func(i int) bool {
		return false
	})
}

func TestUint8WithDelete(t *testing.T) {
	testColumn(t, true, true, "UInt8", "uint8", func(i int) uint8 {
		return uint8(i)
	}, func(i int) uint8 {
		return uint8(i + 1)
	})
}

func TestUint16WithDelete(t *testing.T) {
	testColumn(t, true, true, "UInt16", "uint16", func(i int) uint16 {
		return uint16(i)
	}, func(i int) uint16 {
		return uint16(i + 1)
	})
}

func TestUint32WithDelete(t *testing.T) {
	testColumn(t, true, true, "UInt32", "uint32", func(i int) uint32 {
		return uint32(i)
	}, func(i int) uint32 {
		return uint32(i + 1)
	})
}

func TestUint64WithDelete(t *testing.T) {
	testColumn(t, true, true, "UInt64", "uint64", func(i int) uint64 {
		return uint64(i)
	}, func(i int) uint64 {
		return uint64(i + 1)
	})
}

func TestUint128WithDelete(t *testing.T) {
	testColumn(t, true, true, "UInt128", "uint128", func(i int) types.Uint128 {
		return types.Uint128FromBig(big.NewInt(int64(i)))
	}, func(i int) types.Uint128 {
		x := big.NewInt(int64(i))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Uint128FromBig(x)
	})
}

func TestUint256WithDelete(t *testing.T) {
	testColumn(t, true, true, "UInt256", "uint256", func(i int) types.Uint256 {
		return types.Uint256FromBig(big.NewInt(int64(i)))
	}, func(i int) types.Uint256 {
		x := big.NewInt(int64(i))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Uint256FromBig(x)
	})
}

func TestInt8WithDelete(t *testing.T) {
	testColumn(t, true, true, "Int8", "int8", func(i int) int8 {
		return int8(i)
	}, func(i int) int8 {
		return int8(i + 1)
	})
}

func TestInt16WithDelete(t *testing.T) {
	testColumn(t, true, true, "Int16", "int16", func(i int) int16 {
		return int16(i)
	}, func(i int) int16 {
		return int16(i + 1)
	})
}

func TestInt32WithDelete(t *testing.T) {
	testColumn(t, true, true, "Int32", "int32", func(i int) int32 {
		return int32(i)
	}, func(i int) int32 {
		return int32(i + 1)
	})
}

func TestInt64WithDelete(t *testing.T) {
	testColumn(t, true, true, "Int64", "int64", func(i int) int64 {
		return int64(i)
	}, func(i int) int64 {
		return int64(i + 1)
	})
}

func TestInt128WithDelete(t *testing.T) {
	testColumn(t, true, true, "Int128", "int128", func(i int) types.Int128 {
		return types.Int128FromBig(big.NewInt(int64(i * -1)))
	}, func(i int) types.Int128 {
		x := big.NewInt(int64(i) * -1)
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Int128FromBig(x)
	})
}

func TestInt256WithDelete(t *testing.T) {
	testColumn(t, true, true, "Int256", "int256", func(i int) types.Int256 {
		return types.Int256FromBig(big.NewInt(int64(i)))
	}, func(i int) types.Int256 {
		x := big.NewInt(int64(i) * -1)
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Int256FromBig(x)
	})
}
func TestFixedStringWithDelete(t *testing.T) {
	testColumn(t, true, true, "FixedString(2)", "fixedString", func(i int) [2]byte {
		return [2]byte{byte(i), byte(i + 1)}
	}, func(i int) [2]byte {
		return [2]byte{byte(i + 1), byte(i + 2)}
	})
}

func TestFloat32WithDelete(t *testing.T) {
	testColumn(t, true, true, "Float32", "float32", func(i int) float32 {
		return float32(i)
	}, func(i int) float32 {
		return float32(i + 1)
	})
}

func TestFloat64WithDelete(t *testing.T) {
	testColumn(t, true, true, "Float64", "float64", func(i int) float64 {
		return float64(i)
	}, func(i int) float64 {
		return float64(i + 1)
	})
}

func TestDecimal32WithDelete(t *testing.T) {
	testColumn(t, true, false, "Decimal32(3)", "decimal32", func(i int) types.Decimal32 {
		return types.Decimal32(i)
	}, func(i int) types.Decimal32 {
		return types.Decimal32(i + 1)
	})
}
func TestDecimal64WithDelete(t *testing.T) {
	testColumn(t, true, false, "Decimal64(3)", "decimal64", func(i int) types.Decimal64 {
		return types.Decimal64(i)
	}, func(i int) types.Decimal64 {
		return types.Decimal64(i + 1)
	})
}

func TestDecimal128WithDelete(t *testing.T) {
	testColumn(t, true, false, "Decimal128(3)", "decimal128", func(i int) types.Decimal128 {
		return types.Decimal128(types.Int128FromBig(big.NewInt(int64(i))))
	}, func(i int) types.Decimal128 {
		return types.Decimal128(types.Int128FromBig(big.NewInt(int64(i + 1))))
	})
}

func TestDecimal256WithDelete(t *testing.T) {
	testColumn(t, true, false, "Decimal256(3)", "decimal256", func(i int) types.Decimal256 {
		return types.Decimal256(types.Int256FromBig(big.NewInt(int64(i))))
	}, func(i int) types.Decimal256 {
		return types.Decimal256(types.Int256FromBig(big.NewInt(int64(i + 1))))
	})
}

func TestIPv4WithDelete(t *testing.T) {
	testColumn(t, true, true, "IPv4", "ipv4", func(i int) types.IPv4 {
		// or directly return types.IPv4
		return types.IPv4FromAddr(netip.AddrFrom4([4]byte{0, 0, 0, byte(i)}))
	}, func(i int) types.IPv4 {
		// or directly return types.IPv4
		return types.IPv4FromAddr(netip.AddrFrom4([4]byte{0, 0, byte(i), 0}))
	})
}

func TestIPv6WithDelete(t *testing.T) {
	testColumn(t, true, true, "IPv6", "ipv6", func(i int) types.IPv6 {
		// or directly return types.IPv6
		return types.IPv6FromAddr(netip.MustParseAddr("2001:0db8:85a3:0000:0000:8a2e:0370:7334"))
	}, func(i int) types.IPv6 {
		// or directly return types.IPv6
		return types.IPv6FromAddr(netip.AddrFrom16([16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, byte(i + 1)}))
	})
}

func TestUUIDWithDelete(t *testing.T) {
	testColumn(t, true, true, "UUID", "uuid", func(i int) types.UUID {
		return types.UUIDFromBigEndian(uuid.New())
	}, func(i int) types.UUID {
		return types.UUIDFromBigEndian(uuid.New())
	})
}

func testColumn[T column.BaseType](
	t *testing.T,
	withDelete, isLC bool,
	chType, tableName string,
	firstVal func(i int) T,
	secondVal func(i int) T,
) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	if withDelete {
		tableName += "_with_delete"
	}

	err = conn.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_%s`, tableName),
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:      "allow_suspicious_low_cardinality_types",
			Value:     "true",
			Important: true,
		},
	}

	var sqlCreate string
	if isLC {
		sqlCreate = fmt.Sprintf(`CREATE TABLE test_%[1]s (
			block_id UInt8,
			%[1]s %[2]s,
			%[1]s_nullable Nullable(%[2]s),
			%[1]s_array Array(%[2]s),
			%[1]s_array_nullable Array(Nullable(%[2]s)),
			%[1]s_array_array Array(Array(%[2]s)),
			%[1]s_array_array_nullable Array(Array(Nullable(%[2]s))),
			%[1]s_array_array_array Array(Array(Array(%[2]s))),
			%[1]s_array_array_array_nullable Array(Array(Array(Nullable(%[2]s)))),
			%[1]s_lc LowCardinality(%[2]s),
			%[1]s_nullable_lc LowCardinality(Nullable(%[2]s)),
			%[1]s_array_lc Array(LowCardinality(%[2]s)),
			%[1]s_array_lc_nullable Array(LowCardinality(Nullable(%[2]s)))
		) Engine=Memory`, tableName, chType)
	} else {
		sqlCreate = fmt.Sprintf(`CREATE TABLE test_%[1]s (
			block_id UInt8,
			%[1]s %[2]s,
			%[1]s_nullable Nullable(%[2]s),
			%[1]s_array Array(%[2]s),
			%[1]s_array_nullable Array(Nullable(%[2]s)),
			%[1]s_array_array Array(Array(%[2]s)),
			%[1]s_array_array_nullable Array(Array(Nullable(%[2]s))),
			%[1]s_array_array_array Array(Array(Array(%[2]s))),
			%[1]s_array_array_array_nullable Array(Array(Array(Nullable(%[2]s))))
		) Engine=Memory`, tableName, chType)
	}
	err = conn.ExecWithOption(context.Background(), sqlCreate, &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)
	blockID := column.New[uint8]()
	col := column.New[T]()
	colNullable := column.New[T]().Nullable()
	colArray := column.New[T]().Array()
	colNullableArray := column.New[T]().Nullable().Array()
	colLC := column.New[T]().LC()
	colArrayArray := column.New[T]().Array().Array()
	colNullableArrayArray := column.New[T]().Nullable().Array().Array()
	colArrayArrayArray := column.New[T]().Array().Array().Array()
	colNullableArrayArrayArray := column.New[T]().Nullable().Array().Array().Array()
	colLCNullable := column.New[T]().LC().Nullable()
	colArrayLC := column.New[T]().LC().Array()
	colArrayLCNullable := column.New[T]().LC().Nullable().Array()
	var colInsert []T
	var colNullableInsert []*T
	var colArrayInsert [][]T
	var colArrayNullableInsert [][]*T
	var colArrayArrayInsert [][][]T
	var colArrayArrayNullableInsert [][][]*T
	var colArrayArrayArrayInsert [][][][]T
	var colArrayArrayArrayNullableInsert [][][][]*T
	var colLCInsert []T
	var colLCNullableInsert []*T
	var colLCArrayInsert [][]T
	var colLCNullableArrayInsert [][]*T

	// SetWriteBufferSize is not necessary. this just to show how to set the write buffer
	col.SetWriteBufferSize(10)
	colNullable.SetWriteBufferSize(10)
	colArray.SetWriteBufferSize(10)
	colNullableArray.SetWriteBufferSize(10)
	colLC.SetWriteBufferSize(10)
	colArrayArray.SetWriteBufferSize(10)
	colNullableArrayArray.SetWriteBufferSize(10)
	colArrayArrayArray.SetWriteBufferSize(10)
	colNullableArrayArrayArray.SetWriteBufferSize(10)
	colLC.SetWriteBufferSize(10)
	colLCNullable.SetWriteBufferSize(10)
	colArrayLC.SetWriteBufferSize(10)
	colArrayLCNullable.SetWriteBufferSize(10)
	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		for i := 0; i < rows; i++ {
			blockID.Append(uint8(insertN))
			val := firstVal(i * (insertN + 1))
			val2 := secondVal(i * (insertN + 1))
			valArray := []T{val, val2}
			valArrayNil := []*T{&val, nil}
			valArrayArray := [][]T{{val, val2}}
			valArrayArrayNil := [][]*T{{&val, nil}}
			valArrayArrayArray := [][][]T{{{val, val2}}}
			valArrayArrayArrayNil := [][][]*T{{{&val, nil}}}

			col.Append(val)
			colInsert = append(colInsert, val)

			// example add nullable
			if i%2 == 0 {
				colNullableInsert = append(colNullableInsert, &val)
				colNullable.Append(val)
				colLCNullableInsert = append(colLCNullableInsert, &val)
				colLCNullable.Append(val)
			} else {
				colNullableInsert = append(colNullableInsert, nil)
				colNullable.AppendNil()
				colLCNullableInsert = append(colLCNullableInsert, nil)
				colLCNullable.AppendNil()
			}
			if i%2 == 0 {
				colArray.Append(valArray)
				colNullableArray.AppendP(valArrayNil)
			} else {
				// test append item
				colArray.AppendLen(len(valArray))
				for _, d := range valArray {
					colArray.AppendItem(d)
				}

				colNullableArray.AppendLen(len(valArrayNil))
				for _, d := range valArrayNil {
					colNullableArray.AppendItemP(d)
				}
			}
			colArrayInsert = append(colArrayInsert, valArray)
			colArrayNullableInsert = append(colArrayNullableInsert, valArrayNil)

			colArrayArray.Append(valArrayArray)
			colArrayArrayInsert = append(colArrayArrayInsert, valArrayArray)

			colNullableArrayArray.AppendP(valArrayArrayNil)
			colArrayArrayNullableInsert = append(colArrayArrayNullableInsert, valArrayArrayNil)

			colArrayArrayArray.Append(valArrayArrayArray)
			colArrayArrayArrayInsert = append(colArrayArrayArrayInsert, valArrayArrayArray)

			colNullableArrayArrayArray.AppendP(valArrayArrayArrayNil)
			colArrayArrayArrayNullableInsert = append(colArrayArrayArrayNullableInsert, valArrayArrayArrayNil)

			colLCInsert = append(colLCInsert, val)
			colLC.Append(val)

			colLCArrayInsert = append(colLCArrayInsert, valArray)
			colArrayLC.Append(valArray)

			colLCNullableArrayInsert = append(colLCNullableArrayInsert, valArrayNil)
			colArrayLCNullable.AppendP(valArrayNil)
		}
		if withDelete && insertN == 0 {
			blockID.Remove(rows / 2)
			col.Remove(rows / 2)
			colNullable.Remove(rows / 2)
			colArray.Remove(rows / 2)
			colNullableArray.Remove(rows / 2)
			colLC.Remove(rows / 2)
			colArrayArray.Remove(rows / 2)
			colNullableArrayArray.Remove(rows / 2)
			colArrayArrayArray.Remove(rows / 2)
			colNullableArrayArrayArray.Remove(rows / 2)
			colLC.Remove(rows / 2)
			colLCNullable.Remove(rows / 2)
			colArrayLC.Remove(rows / 2)
			colArrayLCNullable.Remove(rows / 2)

			colInsert = colInsert[:rows/2]
			colNullableInsert = colNullableInsert[:rows/2]
			colArrayInsert = colArrayInsert[:rows/2]
			colArrayNullableInsert = colArrayNullableInsert[:rows/2]
			colLCInsert = colLCInsert[:rows/2]
			colArrayArrayInsert = colArrayArrayInsert[:rows/2]
			colArrayArrayNullableInsert = colArrayArrayNullableInsert[:rows/2]
			colArrayArrayArrayInsert = colArrayArrayArrayInsert[:rows/2]
			colArrayArrayArrayNullableInsert = colArrayArrayArrayNullableInsert[:rows/2]
			colLCInsert = colLCInsert[:rows/2]
			colLCNullableInsert = colLCNullableInsert[:rows/2]
			colLCArrayInsert = colLCArrayInsert[:rows/2]
			colLCNullableArrayInsert = colLCNullableArrayInsert[:rows/2]
		}
		if isLC {
			err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				block_id,
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable,
				%[1]s_array_array,
				%[1]s_array_array_nullable,
				%[1]s_array_array_array,
				%[1]s_array_array_array_nullable,
				%[1]s_lc,
				%[1]s_nullable_lc,
				%[1]s_array_lc,
				%[1]s_array_lc_nullable
			)
		VALUES`, tableName),
				blockID,
				col,
				colNullable,
				colArray,
				colNullableArray,
				colArrayArray,
				colNullableArrayArray,
				colArrayArrayArray,
				colNullableArrayArrayArray,
				colLC,
				colLCNullable,
				colArrayLC,
				colArrayLCNullable,
			)
		} else {
			err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				block_id,
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable,
				%[1]s_array_array,
				%[1]s_array_array_nullable,
				%[1]s_array_array_array,
				%[1]s_array_array_array_nullable
			)
		VALUES`, tableName),
				blockID,
				col,
				colNullable,
				colArray,
				colNullableArray,
				colArrayArray,
				colNullableArrayArray,
				colArrayArrayArray,
				colNullableArrayArrayArray,
			)
		}

		require.NoError(t, err)
	}

	// For testing Append on InsertStream
	var insertStmt chconn.InsertStmt
	if isLC {
		insertStmt, err = conn.InsertStream(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				block_id,
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable,
				%[1]s_array_array,
				%[1]s_array_array_nullable,
				%[1]s_array_array_array,
				%[1]s_array_array_array_nullable,
				%[1]s_lc,
				%[1]s_nullable_lc,
				%[1]s_array_lc,
				%[1]s_array_lc_nullable
			)
		VALUES`, tableName))

		assert.NoError(t, err)
	} else {
		insertStmt, err = conn.InsertStream(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				block_id,
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable,
				%[1]s_array_array,
				%[1]s_array_array_nullable,
				%[1]s_array_array_array,
				%[1]s_array_array_array_nullable
			)
		VALUES`, tableName))
	}

	require.NoError(t, err)

	rowNum := 10
	for i := 0; i < rowNum; i++ {
		blockID := 2
		val := firstVal(i * 3)
		val2 := secondVal(i * 3)

		colInsert = append(colInsert, val)
		colLCInsert = append(colLCInsert, val)
		colArrayInsert = append(colArrayInsert, []T{val, val2})
		colLCArrayInsert = append(colLCArrayInsert, []T{val, val2})
		colArrayArrayInsert = append(colArrayArrayInsert, [][]T{{val, val2}})
		colArrayArrayArrayInsert = append(colArrayArrayArrayInsert, [][][]T{{{val, val2}}})

		colNullableInsert = append(colNullableInsert, &val2)
		colLCNullableInsert = append(colLCNullableInsert, &val2)
		colArrayNullableInsert = append(colArrayNullableInsert, []*T{&val, &val2})
		colLCNullableArrayInsert = append(colLCNullableArrayInsert, []*T{&val, &val2})
		colArrayArrayNullableInsert = append(colArrayArrayNullableInsert, [][]*T{{&val, &val2}})
		colArrayArrayArrayNullableInsert = append(colArrayArrayArrayNullableInsert, [][][]*T{{{&val, &val2}}})

		if isLC {
			err := insertStmt.Append(
				uint8(blockID), // block_id
				val,
				val2,
				[]T{val, val2},
				[]*T{&val, &val2},
				[][]T{{val, val2}},
				[][]T{{val, val2}},
				[][][]T{{{val, val2}}},
				[][][]*T{{{&val, &val2}}},
				val,
				val2,
				[]T{val, val2},
				[]*T{&val, &val2},
			)

			assert.NoError(t, err)
		} else {
			err := insertStmt.Append(
				uint8(blockID), // block_id
				val,
				&val2,
				[]T{val, val2},
				[]*T{&val, &val2},
				[][]T{{val, val2}},
				[][]*T{{&val, &val2}},
				[][][]T{{{val, val2}}},
				[][][]*T{{{&val, &val2}}},
			)

			assert.NoError(t, err)
		}
	}

	err = insertStmt.Flush(context.Background())
	assert.NoError(t, err)

	// test read all
	colRead := column.New[T]()
	colNullableRead := column.New[T]().Nullable()
	colArrayRead := column.New[T]().Array()
	colNullableArrayRead := column.New[T]().Nullable().Array()
	colArrayArrayRead := column.New[T]().Array().Array()
	colNullableArrayArrayRead := column.New[T]().Nullable().Array().Array()
	colArrayArrayArrayRead := column.New[T]().Array().Array().Array()
	colNullableArrayArrayArrayRead := column.New[T]().Nullable().Array().Array().Array()
	colLCRead := column.New[T]().LC()
	colLCNullableRead := column.New[T]().LC().Nullable()
	colArrayLCRead := column.New[T]().LC().Array()
	colArrayLCNullableRead := column.New[T]().LC().Nullable().Array()

	var selectQuery string
	if isLC {
		selectQuery = fmt.Sprintf(`SELECT
		%[1]s,
		%[1]s_nullable,
		%[1]s_array,
		%[1]s_array_nullable,
		%[1]s_array_array,
		%[1]s_array_array_nullable,
		%[1]s_array_array_array,
		%[1]s_array_array_array_nullable,
		%[1]s_lc,
		%[1]s_nullable_lc,
		%[1]s_array_lc,
		%[1]s_array_lc_nullable
	FROM test_%[1]s order by block_id`, tableName)
	} else {
		selectQuery = fmt.Sprintf(`SELECT
			%[1]s,
			%[1]s_nullable,
			%[1]s_array,
			%[1]s_array_nullable,
			%[1]s_array_array,
			%[1]s_array_array_nullable,
			%[1]s_array_array_array,
			%[1]s_array_array_array_nullable
		FROM test_%[1]s order by block_id`, tableName)
	}
	var selectStmt chconn.SelectStmt
	if isLC {
		selectStmt, err = conn.Select(context.Background(), selectQuery,
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
			colArrayArrayRead,
			colNullableArrayArrayRead,
			colArrayArrayArrayRead,
			colNullableArrayArrayArrayRead,
			colLCRead,
			colLCNullableRead,
			colArrayLCRead,
			colArrayLCNullableRead,
		)
	} else {
		selectStmt, err = conn.Select(context.Background(), selectQuery,
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
			colArrayArrayRead,
			colNullableArrayArrayRead,
			colArrayArrayArrayRead,
			colNullableArrayArrayArrayRead,
		)
	}

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var colData []T
	var colNullableData []*T
	var colArrayData [][]T
	var colArrayNullableData [][]*T
	var colArrayArrayData [][][]T
	var colArrayArrayNullableData [][][]*T
	var colArrayArrayArrayData [][][][]T
	var colArrayArrayArrayNullableData [][][][]*T
	var colLCData []T
	var colLCDataWithKeys []T
	var dictData []T
	var dictKey []int
	var colLCNullableData []*T
	var colLCArrayData [][]T
	var colLCNullableArrayData [][]*T

	for selectStmt.Next() {
		colData = colRead.Read(colData)
		colNullableData = colNullableRead.ReadP(colNullableData)
		colArrayData = colArrayRead.Read(colArrayData)
		colArrayNullableData = colNullableArrayRead.ReadP(colArrayNullableData)
		colArrayArrayData = colArrayArrayRead.Read(colArrayArrayData)
		colArrayArrayNullableData = colNullableArrayArrayRead.ReadP(colArrayArrayNullableData)
		colArrayArrayArrayData = colArrayArrayArrayRead.Read(colArrayArrayArrayData)
		colArrayArrayArrayNullableData = colNullableArrayArrayArrayRead.ReadP(colArrayArrayArrayNullableData)
		if isLC {
			colLCData = colLCRead.Read(colLCData)
			colLCNullableData = colLCNullableRead.ReadP(colLCNullableData)
			colLCArrayData = colArrayLCRead.Read(colLCArrayData)
			colLCNullableArrayData = colArrayLCNullableRead.ReadP(colLCNullableArrayData)
			dictData = colLCRead.Dicts()
			dictKey = colLCRead.Keys()
			// get data from dict and keys
			for _, val := range dictKey {
				colLCDataWithKeys = append(colLCDataWithKeys, dictData[val])
			}
		}
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNullableInsert, colNullableData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
	assert.Equal(t, colArrayArrayInsert, colArrayArrayData)
	assert.Equal(t, colArrayArrayNullableInsert, colArrayArrayNullableData)
	assert.Equal(t, colArrayArrayArrayInsert, colArrayArrayArrayData)
	assert.Equal(t, colArrayArrayArrayNullableInsert, colArrayArrayArrayNullableData)
	if isLC {
		assert.Equal(t, colLCInsert, colLCData)
		assert.Equal(t, colLCInsert, colLCDataWithKeys)
		assert.Equal(t, colLCNullableInsert, colLCNullableData)
		assert.Equal(t, colLCArrayInsert, colLCArrayData)
		assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)
	}

	// test row
	colRead = column.New[T]()
	colNullableRead = column.New[T]().Nullable()
	colArrayRead = column.New[T]().Array()
	colNullableArrayRead = column.New[T]().Nullable().Array()
	colArrayArrayRead = column.New[T]().Array().Array()
	colNullableArrayArrayRead = column.New[T]().Nullable().Array().Array()
	colArrayArrayArrayRead = column.New[T]().Array().Array().Array()
	colNullableArrayArrayArrayRead = column.New[T]().Nullable().Array().Array().Array()
	colLCRead = column.New[T]().LowCardinality()
	colLCNullableRead = column.New[T]().LowCardinality().Nullable()
	colArrayLCRead = column.New[T]().LowCardinality().Array()
	colArrayLCNullableRead = column.New[T]().LowCardinality().Nullable().Array()
	if isLC {
		selectStmt, err = conn.Select(context.Background(), selectQuery,
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
			colArrayArrayRead,
			colNullableArrayArrayRead,
			colArrayArrayArrayRead,
			colNullableArrayArrayArrayRead,
			colLCRead,
			colLCNullableRead,
			colArrayLCRead,
			colArrayLCNullableRead,
		)
	} else {
		selectStmt, err = conn.Select(context.Background(), selectQuery,
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
			colArrayArrayRead,
			colNullableArrayArrayRead,
			colArrayArrayArrayRead,
			colNullableArrayArrayArrayRead,
		)
	}

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colData = colData[:0]
	colNullableData = colNullableData[:0]
	colArrayData = colArrayData[:0]
	colArrayNullableData = colArrayNullableData[:0]
	colArrayArrayData = colArrayArrayData[:0]
	colArrayArrayNullableData = colArrayArrayNullableData[:0]
	colArrayArrayArrayData = colArrayArrayArrayData[:0]
	colArrayArrayArrayNullableData = colArrayArrayArrayNullableData[:0]
	colLCData = colLCData[:0]
	colLCNullableData = colLCNullableData[:0]
	colLCArrayData = colLCArrayData[:0]
	colLCNullableArrayData = colLCNullableArrayData[:0]

	for selectStmt.Next() {
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			colData = append(colData, colRead.Row(i))
			colNullableData = append(colNullableData, colNullableRead.RowP(i))
			colArrayData = append(colArrayData, colArrayRead.Row(i))
			colArrayNullableData = append(colArrayNullableData, colNullableArrayRead.RowP(i))
			colArrayArrayData = append(colArrayArrayData, colArrayArrayRead.Row(i))
			colArrayArrayNullableData = append(colArrayArrayNullableData, colNullableArrayArrayRead.RowP(i))
			colArrayArrayArrayData = append(colArrayArrayArrayData, colArrayArrayArrayRead.Row(i))
			colArrayArrayArrayNullableData = append(colArrayArrayArrayNullableData, colNullableArrayArrayArrayRead.RowP(i))
			if isLC {
				colLCData = append(colLCData, colLCRead.Row(i))
				colLCNullableData = append(colLCNullableData, colLCNullableRead.RowP(i))
				colLCArrayData = append(colLCArrayData, colArrayLCRead.Row(i))
				colLCNullableArrayData = append(colLCNullableArrayData, colArrayLCNullableRead.RowP(i))
			}
		}
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNullableInsert, colNullableData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
	assert.Equal(t, colArrayArrayInsert, colArrayArrayData)
	assert.Equal(t, colArrayArrayNullableInsert, colArrayArrayNullableData)
	assert.Equal(t, colArrayArrayArrayInsert, colArrayArrayArrayData)
	assert.Equal(t, colArrayArrayArrayNullableInsert, colArrayArrayArrayNullableData)
	if isLC {
		assert.Equal(t, colLCInsert, colLCData)
		assert.Equal(t, colLCNullableInsert, colLCNullableData)
		assert.Equal(t, colLCArrayInsert, colLCArrayData)
		assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)
	}

	// check dynamic column

	selectStmt, err = conn.Select(context.Background(), selectQuery)

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()
	colData = colData[:0]
	colNullableData = colNullableData[:0]
	colArrayData = colArrayData[:0]
	colArrayNullableData = colArrayNullableData[:0]
	colArrayArrayData = colArrayArrayData[:0]
	colArrayArrayNullableData = colArrayArrayNullableData[:0]
	colArrayArrayArrayData = colArrayArrayArrayData[:0]
	colArrayArrayArrayNullableData = colArrayArrayArrayNullableData[:0]
	colLCData = colLCData[:0]
	colLCNullableData = colLCNullableData[:0]
	colLCArrayData = colLCArrayData[:0]
	colLCNullableArrayData = colLCNullableArrayData[:0]
	if isLC {
		assert.Len(t, autoColumns, 12)
		if tableName == "bool" {
			assert.Equal(t, fmt.Sprintf("%s ", "bool")+column.New[uint8]().FullType(), autoColumns[0].FullType())
			assert.Equal(t, fmt.Sprintf("%s_nullable ", "bool")+column.New[uint8]().Nullable().FullType(), autoColumns[1].FullType())
			assert.Equal(t, fmt.Sprintf("%s_array ", "bool")+column.New[uint8]().Array().FullType(), autoColumns[2].FullType())
			assert.Equal(t, fmt.Sprintf("%s_array_nullable ", "bool")+column.New[uint8]().Nullable().Array().FullType(), autoColumns[3].FullType())
			assert.Equal(t, fmt.Sprintf("%s_array_array ", "bool")+column.New[uint8]().Array().Array().FullType(), autoColumns[4].FullType())
			assert.Equal(t,
				fmt.Sprintf("%s_array_array_nullable ", "bool")+column.New[uint8]().Nullable().Array().Array().FullType(),
				autoColumns[5].FullType())
			assert.Equal(t,
				fmt.Sprintf("%s_array_array_array ", "bool")+column.New[uint8]().Array().Array().Array().FullType(),
				autoColumns[6].FullType())
			assert.Equal(t,
				fmt.Sprintf("%s_array_array_array_nullable ", "bool")+column.New[uint8]().Nullable().Array().Array().Array().FullType(),
				autoColumns[7].FullType())
			assert.Equal(t, fmt.Sprintf("%s_lc ", "bool")+column.New[uint8]().LowCardinality().FullType(), autoColumns[8].FullType())
			assert.Equal(t,
				fmt.Sprintf("%s_nullable_lc ", "bool")+column.New[uint8]().LowCardinality().Nullable().FullType(),
				autoColumns[9].FullType())
			assert.Equal(t, fmt.Sprintf("%s_array_lc ", "bool")+column.New[uint8]().LowCardinality().Array().FullType(), autoColumns[10].FullType())
			assert.Equal(t,
				fmt.Sprintf("%s_array_lc_nullable ", "bool")+column.New[uint8]().LowCardinality().Nullable().Array().FullType(),
				autoColumns[11].FullType())
		} else {
			assert.Equal(t, colRead.FullType(), autoColumns[0].FullType())
			assert.Equal(t, colNullableRead.FullType(), autoColumns[1].FullType())
			assert.Equal(t, colArrayRead.FullType(), autoColumns[2].FullType())
			assert.Equal(t, colNullableArrayRead.FullType(), autoColumns[3].FullType())
			assert.Equal(t, colArrayArrayRead.FullType(), autoColumns[4].FullType())
			assert.Equal(t, colNullableArrayArrayRead.FullType(), autoColumns[5].FullType())
			assert.Equal(t, colArrayArrayArrayRead.FullType(), autoColumns[6].FullType())
			assert.Equal(t, colNullableArrayArrayArrayRead.FullType(), autoColumns[7].FullType())
			assert.Equal(t, colLCRead.FullType(), autoColumns[8].FullType())
			assert.Equal(t, colLCNullableRead.FullType(), autoColumns[9].FullType())
			assert.Equal(t, colArrayLCRead.FullType(), autoColumns[10].FullType())
			assert.Equal(t, colArrayLCNullableRead.FullType(), autoColumns[11].FullType())
		}
	} else {
		assert.Len(t, autoColumns, 8)
		assert.Equal(t, colRead.FullType(), autoColumns[0].FullType())
		assert.Equal(t, colNullableRead.FullType(), autoColumns[1].FullType())
		assert.Equal(t, colArrayRead.FullType(), autoColumns[2].FullType())
		assert.Equal(t, colNullableArrayRead.FullType(), autoColumns[3].FullType())
		assert.Equal(t, colArrayArrayRead.FullType(), autoColumns[4].FullType())
		assert.Equal(t, colNullableArrayArrayRead.FullType(), autoColumns[5].FullType())
		assert.Equal(t, colArrayArrayArrayRead.FullType(), autoColumns[6].FullType())
		assert.Equal(t, colNullableArrayArrayArrayRead.FullType(), autoColumns[7].FullType())
	}

	rows := selectStmt.Rows()

	for rows.Next() {
		var colVal T
		var colNullableVal *T
		var colArrayVal []T
		var colArrayNullableVal []*T
		var colArrayArrayVal [][]T
		var colArrayArrayNullableVal [][]*T
		var colArrayArrayArrayVal [][][]T
		var colArrayArrayArrayNullableVal [][][]*T
		var colLCVal T
		var colLCNullableVal *T
		var colLCArrayVal []T
		var colLCNullableArrayVal []*T
		if isLC {
			err := rows.Scan(
				&colVal,
				&colNullableVal,
				&colArrayVal,
				&colArrayNullableVal,
				&colArrayArrayVal,
				&colArrayArrayNullableVal,
				&colArrayArrayArrayVal,
				&colArrayArrayArrayNullableVal,
				&colLCVal,
				&colLCNullableVal,
				&colLCArrayVal,
				&colLCNullableArrayVal,
			)
			require.NoError(t, err)
		} else {
			err := rows.Scan(
				&colVal,
				&colNullableVal,
				&colArrayVal,
				&colArrayNullableVal,
				&colArrayArrayVal,
				&colArrayArrayNullableVal,
				&colArrayArrayArrayVal,
				&colArrayArrayArrayNullableVal,
			)
			require.NoError(t, err)
		}

		colData = append(colData, colVal)
		colNullableData = append(colNullableData, colNullableVal)
		colArrayData = append(colArrayData, colArrayVal)
		colArrayNullableData = append(colArrayNullableData, colArrayNullableVal)
		colArrayArrayData = append(colArrayArrayData, colArrayArrayVal)
		colArrayArrayNullableData = append(colArrayArrayNullableData, colArrayArrayNullableVal)
		colArrayArrayArrayData = append(colArrayArrayArrayData, colArrayArrayArrayVal)
		colArrayArrayArrayNullableData = append(colArrayArrayArrayNullableData, colArrayArrayArrayNullableVal)
		colLCData = append(colLCData, colLCVal)
		colLCNullableData = append(colLCNullableData, colLCNullableVal)
		colLCArrayData = append(colLCArrayData, colLCArrayVal)
		colLCNullableArrayData = append(colLCNullableArrayData, colLCNullableArrayVal)
	}
	require.NoError(t, selectStmt.Err())
	if isLC {
		assert.Equal(t, colInsert, colData)
		assert.Equal(t, colNullableInsert, colNullableData)
		assert.Equal(t, colArrayInsert, colArrayData)
		assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
		assert.Equal(t, colArrayArrayInsert, colArrayArrayData)
		assert.Equal(t, colArrayArrayNullableInsert, colArrayArrayNullableData)
		assert.Equal(t, colArrayArrayArrayInsert, colArrayArrayArrayData)
		assert.Equal(t, colArrayArrayArrayNullableInsert, colArrayArrayArrayNullableData)
		assert.Equal(t, colLCInsert, colLCData)
		assert.Equal(t, colLCNullableInsert, colLCNullableData)
		assert.Equal(t, colLCArrayInsert, colLCArrayData)
		assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)
	} else {
		assert.Equal(t, colInsert, colData)
		assert.Equal(t, colNullableInsert, colNullableData)
		assert.Equal(t, colArrayInsert, colArrayData)
		assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
		assert.Equal(t, colArrayArrayInsert, colArrayArrayData)
		assert.Equal(t, colArrayArrayNullableInsert, colArrayArrayNullableData)
		assert.Equal(t, colArrayArrayArrayInsert, colArrayArrayArrayData)
		assert.Equal(t, colArrayArrayArrayNullableInsert, colArrayArrayArrayNullableData)
	}

	selectStmt.Close()

	var chconnJSON []string
	jsonFormat := format.NewJSON(1000, func(b []byte, cb []column.ColumnBasic) {
		chconnJSON = append(chconnJSON, string(b))
	})

	// check JSON
	selectStmt, err = conn.Select(context.Background(), selectQuery)

	require.NoError(t, err)

	err = jsonFormat.ReadEachRow(selectStmt)
	require.NoError(t, err)

	jsonFromClickhouse := httpJSON(selectQuery)

	d := json.NewDecoder(strings.NewReader(strings.Join(chconnJSON, "\n")))
	var valsChconn []any
	for {
		var v any
		if err := d.Decode(&v); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		valsChconn = append(valsChconn, v)
	}

	d = json.NewDecoder(bytes.NewReader(jsonFromClickhouse))
	var valsClickhouse []any
	for {
		var v any
		if err := d.Decode(&v); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		valsClickhouse = append(valsClickhouse, v)
	}

	assert.Equal(t, valsClickhouse, valsChconn)
}

func httpJSON(query string) []byte {
	// URL of your ClickHouse server
	url := os.Getenv("CHX_TEST_HTTP_CONN_STRING")
	if url == "" {
		url = "http://localhost:8123"
	}

	url += "?output_format_json_quote_decimals=1"

	// Your ClickHouse query
	query += " FORMAT JSONEachRow"

	// Create a new HTTP request with the query
	req, err := http.NewRequest("POST", url, bytes.NewBufferString(query))
	if err != nil {
		panic(err)
	}

	// Set appropriate headers (if needed, e.g., for authentication)
	// req.Header.Set("X-Custom-Header", "my-value")

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	// The body contains the JSON response
	return body
}

func TestEmptyCollection(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	tableName := "empty_collection"
	err = conn.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_%s`, tableName),
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:  "allow_suspicious_low_cardinality_types",
			Value: "true",
		},
	}

	sqlCreate := fmt.Sprintf(`CREATE TABLE test_%[1]s (
			%[1]s_array Array(%[2]s),
			%[1]s_array_nullable Array(Nullable(%[2]s)),
			%[1]s_array_lc Array(LowCardinality(%[2]s)),
			%[1]s_array_lc_nullable Array(LowCardinality(Nullable(%[2]s)))
		) Engine=Memory`, tableName, "UInt16")

	err = conn.ExecWithOption(context.Background(), sqlCreate, &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)
	colArray := column.New[uint16]().Array()
	colNullableArray := column.New[uint16]().Nullable().Array()
	colArrayLC := column.New[uint16]().LC().Array()
	colArrayLCNullable := column.New[uint16]().LC().Nullable().Array()
	colArray.Append([]uint16{})
	colNullableArray.AppendP([]*uint16{})
	colArrayLC.Append([]uint16{})
	colArrayLCNullable.AppendP([]*uint16{})

	err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				%[1]s_array,
				%[1]s_array_nullable,
				%[1]s_array_lc,
				%[1]s_array_lc_nullable
			)
		VALUES`, tableName),
		colArray,
		colNullableArray,
		colArrayLC,
		colArrayLCNullable,
	)

	require.NoError(t, err)

	// test read all
	colArrayRead := column.New[uint16]().Array()
	colNullableArrayRead := column.New[uint16]().Nullable().Array()
	colArrayLCRead := column.New[uint16]().LC().Array()
	colArrayLCNullableRead := column.New[uint16]().LC().Nullable().Array()
	var selectStmt chconn.SelectStmt
	selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
		%[1]s_array,
		%[1]s_array_nullable,
		%[1]s_array_lc,
		%[1]s_array_lc_nullable
	FROM test_%[1]s `, tableName),
		colArrayRead,
		colNullableArrayRead,
		colArrayLCRead,
		colArrayLCNullableRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var colArrayData [][]uint16
	var colArrayNullableData [][]*uint16
	var colLCArrayData [][]uint16
	var colLCNullableArrayData [][]*uint16

	for selectStmt.Next() {
		colArrayData = colArrayRead.Read(colArrayData)
		colArrayNullableData = colNullableArrayRead.ReadP(colArrayNullableData)
		colLCArrayData = colArrayLCRead.Read(colLCArrayData)
		colLCNullableArrayData = colArrayLCNullableRead.ReadP(colLCNullableArrayData)
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, [][]uint16{{}}, colArrayData)
	assert.Equal(t, [][]*uint16{{}}, colArrayNullableData)
	assert.Equal(t, [][]uint16{{}}, colLCArrayData)
	assert.Equal(t, [][]*uint16{{}}, colLCNullableArrayData)

	selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
		%[1]s_array,
		%[1]s_array_nullable,
		%[1]s_array_lc,
		%[1]s_array_lc_nullable
	FROM test_%[1]s `, tableName),
		colArrayRead,
		colNullableArrayRead,
		colArrayLCRead,
		colArrayLCNullableRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	rows := selectStmt.Rows()
	assert.True(t, rows.Next())
	var colArrayVal []uint16
	var colArrayNullableVal []*uint16
	var colLCArrayVal []uint16
	var colLCNullableArrayVal []*uint16

	err = rows.Scan(
		&colArrayVal,
		&colArrayNullableVal,
		&colLCArrayVal,
		&colLCNullableArrayVal,
	)
	require.NoError(t, err)

	var colArrayResult []uint16
	var colArrayNullableResult []*uint16
	var colArrayLCResult []uint16
	var colArrayLCNullableResult []*uint16
	assert.Equal(t, colArrayResult, colArrayVal)
	assert.Equal(t, colArrayNullableResult, colArrayNullableVal)
	assert.Equal(t, colArrayLCResult, colLCArrayVal)
	assert.Equal(t, colArrayLCNullableResult, colLCNullableArrayVal)

	require.NoError(t, selectStmt.Err())
}
