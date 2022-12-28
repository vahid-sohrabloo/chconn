package column_test

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"net/netip"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v2"
	"github.com/vahid-sohrabloo/chconn/v2/column"
	"github.com/vahid-sohrabloo/chconn/v2/types"
)

func TestBool(t *testing.T) {
	testColumn(t, true, "UInt8", "bool", func(i int) bool {
		return true
	}, func(i int) bool {
		return false
	})
}

func TestUint8(t *testing.T) {
	testColumn(t, true, "UInt8", "uint8", func(i int) uint8 {
		return uint8(i)
	}, func(i int) uint8 {
		return uint8(i + 1)
	})
}

func TestUint16(t *testing.T) {
	testColumn(t, true, "UInt16", "uint16", func(i int) uint16 {
		return uint16(i)
	}, func(i int) uint16 {
		return uint16(i + 1)
	})
}

func TestUint32(t *testing.T) {
	testColumn(t, true, "UInt32", "uint32", func(i int) uint32 {
		return uint32(i)
	}, func(i int) uint32 {
		return uint32(i + 1)
	})
}

func TestUint64(t *testing.T) {
	testColumn(t, true, "UInt64", "uint64", func(i int) uint64 {
		return uint64(i)
	}, func(i int) uint64 {
		return uint64(i + 1)
	})
}

func TestUint128(t *testing.T) {
	testColumn(t, true, "UInt128", "uint128", func(i int) types.Uint128 {
		return types.Uint128FromBig(big.NewInt(int64(i)))
	}, func(i int) types.Uint128 {
		x := big.NewInt(int64(i))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Uint128FromBig(x)
	})
}

func TestUint256(t *testing.T) {
	testColumn(t, true, "UInt256", "uint256", func(i int) types.Uint256 {
		return types.Uint256FromBig(big.NewInt(int64(i)))
	}, func(i int) types.Uint256 {
		x := big.NewInt(int64(i))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Uint256FromBig(x)
	})
}

func TestInt8(t *testing.T) {
	testColumn(t, true, "Int8", "int8", func(i int) int8 {
		return int8(i)
	}, func(i int) int8 {
		return int8(i + 1)
	})
}

func TestInt16(t *testing.T) {
	testColumn(t, true, "Int16", "int16", func(i int) int16 {
		return int16(i)
	}, func(i int) int16 {
		return int16(i + 1)
	})
}

func TestInt32(t *testing.T) {
	testColumn(t, true, "Int32", "int32", func(i int) int32 {
		return int32(i)
	}, func(i int) int32 {
		return int32(i + 1)
	})
}

func TestInt64(t *testing.T) {
	testColumn(t, true, "Int64", "int64", func(i int) int64 {
		return int64(i)
	}, func(i int) int64 {
		return int64(i + 1)
	})
}

func TestInt128(t *testing.T) {
	testColumn(t, true, "Int128", "int128", func(i int) types.Int128 {
		return types.Int128FromBig(big.NewInt(int64(i * -1)))
	}, func(i int) types.Int128 {
		x := big.NewInt(int64(i) * -1)
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Int128FromBig(x)
	})
}

func TestInt256(t *testing.T) {
	testColumn(t, true, "Int256", "int256", func(i int) types.Int256 {
		return types.Int256FromBig(big.NewInt(int64(i)))
	}, func(i int) types.Int256 {
		x := big.NewInt(int64(i) * -1)
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		x = x.Mul(x, big.NewInt(math.MaxInt64))
		return types.Int256FromBig(x)
	})
}
func TestFixedString(t *testing.T) {
	testColumn(t, true, "FixedString(2)", "fixedString", func(i int) [2]byte {
		return [2]byte{byte(i), byte(i + 1)}
	}, func(i int) [2]byte {
		return [2]byte{byte(i + 1), byte(i + 2)}
	})
}

func TestFloat32(t *testing.T) {
	testColumn(t, true, "Float32", "float32", func(i int) float32 {
		return float32(i)
	}, func(i int) float32 {
		return float32(i + 1)
	})
}

func TestFloat64(t *testing.T) {
	testColumn(t, true, "Float64", "float64", func(i int) float64 {
		return float64(i)
	}, func(i int) float64 {
		return float64(i + 1)
	})
}

func TestDecimal32(t *testing.T) {
	testColumn(t, false, "Decimal32(3)", "decimal32", func(i int) types.Decimal32 {
		return types.Decimal32(i)
	}, func(i int) types.Decimal32 {
		return types.Decimal32(i + 1)
	})
}
func TestDecimal64(t *testing.T) {
	testColumn(t, false, "Decimal64(3)", "decimal64", func(i int) types.Decimal64 {
		return types.Decimal64(i)
	}, func(i int) types.Decimal64 {
		return types.Decimal64(i + 1)
	})
}

func TestDecimal128(t *testing.T) {
	testColumn(t, false, "Decimal128(3)", "decimal128", func(i int) types.Decimal128 {
		return types.Decimal128(types.Int128FromBig(big.NewInt(int64(i))))
	}, func(i int) types.Decimal128 {
		return types.Decimal128(types.Int128FromBig(big.NewInt(int64(i + 1))))
	})
}

func TestDecimal256(t *testing.T) {
	testColumn(t, false, "Decimal256(3)", "decimal256", func(i int) types.Decimal256 {
		return types.Decimal256(types.Int256FromBig(big.NewInt(int64(i))))
	}, func(i int) types.Decimal256 {
		return types.Decimal256(types.Int256FromBig(big.NewInt(int64(i + 1))))
	})
}

func TestIPv4(t *testing.T) {
	testColumn(t, true, "IPv4", "ipv4", func(i int) types.IPv4 {
		// or directly return types.IPv4
		return types.IPv4FromAddr(netip.AddrFrom4([4]byte{0, 0, 0, byte(i)}))
	}, func(i int) types.IPv4 {
		// or directly return types.IPv4
		return types.IPv4FromAddr(netip.AddrFrom4([4]byte{0, 0, byte(i), 0}))
	})
}

func TestIPv6(t *testing.T) {
	testColumn(t, true, "IPv6", "ipv6", func(i int) types.IPv6 {
		// or directly return types.IPv6
		return types.IPv6FromAddr(netip.MustParseAddr("2001:0db8:85a3:0000:0000:8a2e:0370:7334"))
	}, func(i int) types.IPv6 {
		// or directly return types.IPv6
		return types.IPv6FromAddr(netip.AddrFrom16([16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, byte(i + 1)}))
	})
}

func TestUUID(t *testing.T) {
	testColumn(t, true, "UUID", "uuid", func(i int) types.UUID {
		return types.UUIDFromBigEndian(uuid.New())
	}, func(i int) types.UUID {
		return types.UUIDFromBigEndian(uuid.New())
	})
}

func testColumn[T comparable](
	t *testing.T,
	isLC bool,
	chType, tableName string,
	firstVal func(i int) T,
	secondVal func(i int) T,
) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

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
			%[1]s_array_nullable Array(Nullable(%[2]s))
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
	colLCNullable := column.New[T]().LC().Nullable()
	colArrayLC := column.New[T]().LC().Array()
	colArrayLCNullable := column.New[T]().LC().Nullable().Array()
	var colInsert []T
	var colNullableInsert []*T
	var colArrayInsert [][]T
	var colArrayNullableInsert [][]*T
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

			colArray.Append(valArray)
			colArrayInsert = append(colArrayInsert, valArray)

			colNullableArray.AppendP(valArrayNil)
			colArrayNullableInsert = append(colArrayNullableInsert, valArrayNil)

			colLCInsert = append(colLCInsert, val)
			colLC.Append(val)

			colLCArrayInsert = append(colLCArrayInsert, valArray)
			colArrayLC.Append(valArray)

			colLCNullableArrayInsert = append(colLCNullableArrayInsert, valArrayNil)
			colArrayLCNullable.AppendP(valArrayNil)
		}
		if isLC {
			err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				block_id,
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable,
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
				%[1]s_array_nullable
			)
		VALUES`, tableName),
				blockID,
				col,
				colNullable,
				colArray,
				colNullableArray,
			)
		}

		require.NoError(t, err)
	}

	// test read all
	colRead := column.New[T]()
	colNullableRead := column.New[T]().Nullable()
	colArrayRead := column.New[T]().Array()
	colNullableArrayRead := column.New[T]().Nullable().Array()
	colLCRead := column.New[T]().LC()
	colLCNullableRead := column.New[T]().LC().Nullable()
	colArrayLCRead := column.New[T]().LC().Array()
	colArrayLCNullableRead := column.New[T]().LC().Nullable().Array()
	var selectStmt chconn.SelectStmt
	if isLC {
		selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
		%[1]s,
		%[1]s_nullable,
		%[1]s_array,
		%[1]s_array_nullable,
		%[1]s_lc,
		%[1]s_nullable_lc,
		%[1]s_array_lc,
		%[1]s_array_lc_nullable
	FROM test_%[1]s order by block_id`, tableName),
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
			colLCRead,
			colLCNullableRead,
			colArrayLCRead,
			colArrayLCNullableRead,
		)
	} else {
		selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
			%[1]s,
			%[1]s_nullable,
			%[1]s_array,
			%[1]s_array_nullable
		FROM test_%[1]s order by block_id`, tableName),
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
		)
	}

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var colData []T
	var colNullableData []*T
	var colArrayData [][]T
	var colArrayNullableData [][]*T
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
	colLCRead = column.New[T]().LowCardinality()
	colLCNullableRead = column.New[T]().LowCardinality().Nullable()
	colArrayLCRead = column.New[T]().LowCardinality().Array()
	colArrayLCNullableRead = column.New[T]().LowCardinality().Nullable().Array()
	if isLC {
		selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
			%[1]s,
			%[1]s_nullable,
			%[1]s_array,
			%[1]s_array_nullable,
			%[1]s_lc,
			%[1]s_nullable_lc,
			%[1]s_array_lc,
			%[1]s_array_lc_nullable
		FROM test_%[1]s order by block_id`, tableName),
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
			colLCRead,
			colLCNullableRead,
			colArrayLCRead,
			colArrayLCNullableRead,
		)
	} else {
		selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable
			FROM test_%[1]s order by block_id`, tableName),
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
		)
	}

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colData = colData[:0]
	colNullableData = colNullableData[:0]
	colArrayData = colArrayData[:0]
	colArrayNullableData = colArrayNullableData[:0]
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
	if isLC {
		assert.Equal(t, colLCInsert, colLCData)
		assert.Equal(t, colLCNullableInsert, colLCNullableData)
		assert.Equal(t, colLCArrayInsert, colLCArrayData)
		assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)
	}

	// check dynamic column
	if isLC {
		selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
		%[1]s,
		%[1]s_nullable,
		%[1]s_array,
		%[1]s_array_nullable,
		%[1]s_lc,
		%[1]s_nullable_lc,
		%[1]s_array_lc,
		%[1]s_array_lc_nullable
		FROM test_%[1]s order by block_id`, tableName),
		)
	} else {
		selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable
			FROM test_%[1]s order by block_id`, tableName),
		)
	}
	require.NoError(t, err)
	autoColumns := selectStmt.Columns()
	colData = colData[:0]
	colNullableData = colNullableData[:0]
	colArrayData = colArrayData[:0]
	colArrayNullableData = colArrayNullableData[:0]
	colLCData = colLCData[:0]
	colLCNullableData = colLCNullableData[:0]
	colLCArrayData = colLCArrayData[:0]
	colLCNullableArrayData = colLCNullableArrayData[:0]
	if isLC {
		assert.Len(t, autoColumns, 8)
		if tableName == "bool" {
			assert.Equal(t, column.New[uint8]().ColumnType(), autoColumns[0].ColumnType())
			assert.Equal(t, column.New[uint8]().Nullable().ColumnType(), autoColumns[1].ColumnType())
			assert.Equal(t, column.New[uint8]().Array().ColumnType(), autoColumns[2].ColumnType())
			assert.Equal(t, column.New[uint8]().Nullable().Array().ColumnType(), autoColumns[3].ColumnType())
			assert.Equal(t, column.New[uint8]().LowCardinality().ColumnType(), autoColumns[4].ColumnType())
			assert.Equal(t, column.New[uint8]().LowCardinality().Nullable().ColumnType(), autoColumns[5].ColumnType())
			assert.Equal(t, column.New[uint8]().LowCardinality().Array().ColumnType(), autoColumns[6].ColumnType())
			assert.Equal(t, column.New[uint8]().LowCardinality().Nullable().Array().ColumnType(), autoColumns[7].ColumnType())
		} else {
			assert.Equal(t, colRead.ColumnType(), autoColumns[0].ColumnType())
			assert.Equal(t, colNullableRead.ColumnType(), autoColumns[1].ColumnType())
			assert.Equal(t, colArrayRead.ColumnType(), autoColumns[2].ColumnType())
			assert.Equal(t, colNullableArrayRead.ColumnType(), autoColumns[3].ColumnType())
			assert.Equal(t, colLCRead.ColumnType(), autoColumns[4].ColumnType())
			assert.Equal(t, colLCNullableRead.ColumnType(), autoColumns[5].ColumnType())
			assert.Equal(t, colArrayLCRead.ColumnType(), autoColumns[6].ColumnType())
			assert.Equal(t, colArrayLCNullableRead.ColumnType(), autoColumns[7].ColumnType())
		}
	} else {
		assert.Len(t, autoColumns, 4)
		assert.Equal(t, colRead.ColumnType(), autoColumns[0].ColumnType())
		assert.Equal(t, colNullableRead.ColumnType(), autoColumns[1].ColumnType())
		assert.Equal(t, colArrayRead.ColumnType(), autoColumns[2].ColumnType())
		assert.Equal(t, colNullableArrayRead.ColumnType(), autoColumns[3].ColumnType())
	}
	rows := selectStmt.Rows()

	for rows.Next() {
		var colVal T
		var colNullableVal *T
		var colArrayVal []T
		var colArrayNullableVal []*T
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
			)
			require.NoError(t, err)
		}

		colData = append(colData, colVal)
		colNullableData = append(colNullableData, colNullableVal)
		colArrayData = append(colArrayData, colArrayVal)
		colArrayNullableData = append(colArrayNullableData, colArrayNullableVal)
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
		assert.Equal(t, colLCInsert, colLCData)
		assert.Equal(t, colLCNullableInsert, colLCNullableData)
		assert.Equal(t, colLCArrayInsert, colLCArrayData)
		assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)
	} else {
		assert.Equal(t, colInsert, colData)
		assert.Equal(t, colNullableInsert, colNullableData)
		assert.Equal(t, colArrayInsert, colArrayData)
		assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
	}

	selectStmt.Close()
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
	colArray.Append()
	colArray.Append([]uint16{})
	colNullableArray.AppendP()
	colNullableArray.AppendP([]*uint16{})
	colArrayLC.Append()
	colArrayLC.Append([]uint16{})
	colArrayLCNullable.AppendP()
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

	colArrayData = colArrayData[:0]
	colArrayNullableData = colArrayNullableData[:0]
	colLCArrayData = colLCArrayData[:0]
	colLCNullableArrayData = colLCNullableArrayData[:0]

	rows := selectStmt.Rows()

	for rows.Next() {
		var colArrayVal []uint16
		var colArrayNullableVal []*uint16
		var colLCArrayVal []uint16
		var colLCNullableArrayVal []*uint16

		err := rows.Scan(
			&colArrayVal,
			&colArrayNullableVal,
			&colLCArrayVal,
			&colLCNullableArrayVal,
		)
		require.NoError(t, err)

		colArrayData = append(colArrayData, colArrayVal)
		colArrayNullableData = append(colArrayNullableData, colArrayNullableVal)

		colLCArrayData = append(colLCArrayData, colLCArrayVal)
		colLCNullableArrayData = append(colLCNullableArrayData, colLCNullableArrayVal)
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, [][]uint16{{}}, colArrayData)
	assert.Equal(t, [][]*uint16{{}}, colArrayNullableData)
	assert.Equal(t, [][]uint16{{}}, colLCArrayData)
	assert.Equal(t, [][]*uint16{{}}, colLCNullableArrayData)

}
