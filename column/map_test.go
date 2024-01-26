package column_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestMapUint8(t *testing.T) {
	testMapColumn(t, false, "UInt8", "uint8", func(i int) []uint8 {
		d := make([]uint8, 2)
		d[0] = uint8(i)
		d[1] = uint8(i + 1)
		return d
	}, func(i int) []uint8 {
		d := make([]uint8, 2)
		d[0] = uint8(i)
		d[1] = uint8(i + 1)
		return d
	})
}

func TestMapUint16(t *testing.T) {
	testMapColumn(t, false, "UInt16", "uint16", func(i int) []uint16 {
		d := make([]uint16, 2)
		d[0] = uint16(i)
		d[1] = uint16(i + 1)
		return d
	}, func(i int) []uint16 {
		d := make([]uint16, 2)
		d[0] = uint16(i)
		d[1] = uint16(i + 1)
		return d
	})
}

func TestMapUint32(t *testing.T) {
	testMapColumn(t, false, "UInt32", "uint32", func(i int) []uint32 {
		d := make([]uint32, 2)
		d[0] = uint32(i)
		d[1] = uint32(i + 1)
		return d
	}, func(i int) []uint32 {
		d := make([]uint32, 2)
		d[0] = uint32(i)
		d[1] = uint32(i + 1)
		return d
	})
}

func TestMapUint64(t *testing.T) {
	testMapColumn(t, false, "UInt64", "uint64", func(i int) []uint64 {
		d := make([]uint64, 2)
		d[0] = uint64(i)
		d[1] = uint64(i + 1)
		return d
	}, func(i int) []uint64 {
		d := make([]uint64, 2)
		d[0] = uint64(i)
		d[1] = uint64(i + 1)
		return d
	})
}
func TestMapInt8(t *testing.T) {
	testMapColumn(t, false, "Int8", "int8", func(i int) []int8 {
		d := make([]int8, 2)
		d[0] = int8(i)
		d[1] = int8(i + 1)
		return d
	}, func(i int) []int8 {
		d := make([]int8, 2)
		d[0] = int8(i)
		d[1] = int8(i + 1)
		return d
	})
}

func TestMapInt16(t *testing.T) {
	testMapColumn(t, false, "Int16", "int16", func(i int) []int16 {
		d := make([]int16, 2)
		d[0] = int16(i)
		d[1] = int16(i + 1)
		return d
	}, func(i int) []int16 {
		d := make([]int16, 2)
		d[0] = int16(i)
		d[1] = int16(i + 1)
		return d
	})
}

func TestMapInt32(t *testing.T) {
	testMapColumn(t, false, "Int32", "int32", func(i int) []int32 {
		d := make([]int32, 2)
		d[0] = int32(i)
		d[1] = int32(i + 1)
		return d
	}, func(i int) []int32 {
		d := make([]int32, 2)
		d[0] = int32(i)
		d[1] = int32(i + 1)
		return d
	})
}

func TestMapInt64(t *testing.T) {
	testMapColumn(t, false, "Int64", "int64", func(i int) []int64 {
		d := make([]int64, 2)
		d[0] = int64(i)
		d[1] = int64(i + 1)
		return d
	}, func(i int) []int64 {
		d := make([]int64, 2)
		d[0] = int64(i)
		d[1] = int64(i + 1)
		return d
	})
}

func TestMapFloat32(t *testing.T) {
	testMapColumn(t, false, "Float32", "float32", func(i int) []float32 {
		d := make([]float32, 2)
		d[0] = float32(i)
		d[1] = float32(i + 1)
		return d
	}, func(i int) []float32 {
		d := make([]float32, 2)
		d[0] = float32(i)
		d[1] = float32(i + 1)
		return d
	})
}

func TestMapFloat64(t *testing.T) {
	testMapColumn(t, false, "Float64", "float64", func(i int) []float64 {
		d := make([]float64, 2)
		d[0] = float64(i)
		d[1] = float64(i + 1)
		return d
	}, func(i int) []float64 {
		d := make([]float64, 2)
		d[0] = float64(i)
		d[1] = float64(i + 1)
		return d
	})
}

func TestMapWihDeleteFloat64(t *testing.T) {
	testMapColumn(t, true, "Float64", "float64", func(i int) []float64 {
		d := make([]float64, 2)
		d[0] = float64(i)
		d[1] = float64(i + 1)
		return d
	}, func(i int) []float64 {
		d := make([]float64, 2)
		d[0] = float64(i)
		d[1] = float64(i + 1)
		return d
	})
}

func testMapColumn[V column.BaseType](
	t *testing.T,
	withDelete bool,
	chType, tableName string,
	firstVal func(i int) []V,
	secondVal func(i int) []V,
) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	if withDelete {
		tableName += "_with_delete"
	}

	err = conn.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_map_%s`, tableName),
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:  "allow_suspicious_low_cardinality_types",
			Value: "true",
		},
	}
	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_map_%[1]s (
				block_id UInt8,
				%[1]s Map(String,%[2]s),
				%[1]s_nullable Map(String,Nullable(%[2]s)),
				%[1]s_array Map(String,Array(%[2]s)),
				%[1]s_array_nullable Map(String,Array(Nullable(%[2]s))),
				%[1]s_lc Map(String,LowCardinality(%[2]s)),
				%[1]s_nullable_lc Map(String,LowCardinality(Nullable(%[2]s))),
				%[1]s_array_lc Map(String,Array(LowCardinality(%[2]s))),
				%[1]s_array_lc_nullable Map(String,Array(LowCardinality(Nullable(%[2]s))))
			) Engine=Memory`, tableName, chType), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)
	blockID := column.New[uint8]()
	col := column.NewMap[string, V](
		column.NewString(),
		column.New[V](),
	)
	colNullable := column.NewMapNullable[string, V](
		column.NewString(),
		column.New[V]().Nullable(),
	)
	colArray := column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().Array(),
	)
	colNullableArray := column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().Nullable().Array(),
	)
	colLC := column.NewMap[string, V](
		column.NewString(),
		column.New[V]().LC(),
	)
	colLCNullable := column.NewMapNullable[string, V](
		column.NewString(),
		column.New[V]().LC().Nullable(),
	)
	colArrayLC := column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().LC().Array(),
	)
	colArrayLCNullable := column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().LC().Nullable().Array(),
	)
	var colInsert []map[string]V
	var colNullableInsert []map[string]*V
	var colArrayInsert []map[string][]V
	var colArrayNullableInsert []map[string][]*V
	var colLCInsert []map[string]V
	var colLCNullableInsert []map[string]*V
	var colLCArrayInsert []map[string][]V
	var colLCNullableArrayInsert []map[string][]*V

	var colInsertStruct [][]struct {
		K string
		V V
	}
	var colNullableInsertStruct [][]struct {
		K string
		V *V
	}
	var colArrayInsertStruct [][]struct {
		K string
		V []V
	}
	var colArrayNullableInsertStruct [][]struct {
		K string
		V []*V
	}
	var colLCInsertStruct [][]struct {
		K string
		V V
	}
	var colLCNullableInsertStruct [][]struct {
		K string
		V *V
	}
	var colLCArrayInsertStruct [][]struct {
		K string
		V []V
	}
	var colLCNullableArrayInsertStruct [][]struct {
		K string
		V []*V
	}

	// SetWriteBufferSize is not necessary. this just to show how to set write buffer
	col.SetWriteBufferSize(10)
	colNullable.SetWriteBufferSize(10)
	colArray.SetWriteBufferSize(10)
	colNullableArray.SetWriteBufferSize(10)
	colLC.SetWriteBufferSize(10)
	colLCNullable.SetWriteBufferSize(10)
	colArrayLC.SetWriteBufferSize(10)
	colArrayLCNullable.SetWriteBufferSize(10)
	for insertN := 0; insertN < 2; insertN++ {
		rows := 2
		for i := 0; i < rows; i++ {
			blockID.Append(uint8(insertN))
			valData := firstVal(i)
			val2Data := secondVal(i)
			val := map[string]V{
				"a": valData[0],
				"b": valData[1],
			}
			valNullable := map[string]*V{
				"a": &valData[0],
				"b": &valData[1],
			}
			valNullable2 := map[string]*V{
				"a": &valData[1],
				"b": nil,
			}
			valArray := map[string][]V{
				"a": valData,
				"b": val2Data,
			}

			valArrayNil := map[string][]*V{
				"a": {&valData[0], &valData[1]},
				"b": {&valData[1], nil},
			}
			col.Append(val)
			colInsert = append(colInsert, val)
			colInsertStruct = append(colInsertStruct, []struct {
				K string
				V V
			}{{
				K: "a",
				V: valData[0],
			}, {
				K: "b",
				V: valData[1],
			}})

			// example add nullable

			if i%2 == 0 {
				colNullableInsert = append(colNullableInsert, valNullable)
				colNullableInsertStruct = append(colNullableInsertStruct, []struct {
					K string
					V *V
				}{{
					K: "a",
					V: &valData[0],
				}, {
					K: "b",
					V: &valData[1],
				}})

				colNullable.AppendP(valNullable)
				colLCNullableInsert = append(colLCNullableInsert, valNullable)
				colLCNullableInsertStruct = append(colLCNullableInsertStruct, []struct {
					K string
					V *V
				}{{
					K: "a",
					V: &valData[0],
				}, {
					K: "b",
					V: &valData[1],
				}})

				colLCNullable.AppendP(valNullable)
			} else {
				colNullableInsert = append(colNullableInsert, valNullable2)
				colNullableInsertStruct = append(colNullableInsertStruct, []struct {
					K string
					V *V
				}{{
					K: "a",
					V: &valData[1],
				}, {
					K: "b",
					V: nil,
				}})

				colNullable.AppendP(valNullable2)
				colLCNullableInsert = append(colLCNullableInsert, valNullable2)
				colLCNullableInsertStruct = append(colLCNullableInsertStruct, []struct {
					K string
					V *V
				}{{
					K: "a",
					V: &valData[1],
				}, {
					K: "b",
					V: nil,
				}})

				colLCNullable.AppendP(valNullable2)
			}

			colArray.Append(valArray)
			colArrayInsert = append(colArrayInsert, valArray)
			colArrayInsertStruct = append(colArrayInsertStruct, []struct {
				K string
				V []V
			}{{
				K: "a",
				V: valData,
			}, {
				K: "b",
				V: val2Data,
			}})

			colNullableArray.AppendLen(len(valArrayNil))
			for k, v := range valArrayNil {
				colNullableArray.KeyColumn().Append(k)
				colNullableArray.ValueColumn().(*column.ArrayNullable[V]).AppendP(v)
			}
			colArrayNullableInsert = append(colArrayNullableInsert, valArrayNil)
			colArrayNullableInsertStruct = append(colArrayNullableInsertStruct, []struct {
				K string
				V []*V
			}{{
				K: "a",
				V: valArrayNil["a"],
			}, {
				K: "b",
				V: valArrayNil["b"],
			}})

			colLCInsert = append(colLCInsert, val)
			colLCInsertStruct = append(colLCInsertStruct, []struct {
				K string
				V V
			}{{
				K: "a",
				V: valData[0],
			}, {
				K: "b",
				V: valData[1],
			}})

			colLC.Append(val)

			colLCArrayInsert = append(colLCArrayInsert, valArray)
			colLCArrayInsertStruct = append(colLCArrayInsertStruct, []struct {
				K string
				V []V
			}{{
				K: "a",
				V: valData,
			}, {
				K: "b",
				V: val2Data,
			}})

			colArrayLC.Append(valArray)

			colLCNullableArrayInsert = append(colLCNullableArrayInsert, valArrayNil)
			colLCNullableArrayInsertStruct = append(colLCNullableArrayInsertStruct, []struct {
				K string
				V []*V
			}{{
				K: "a",
				V: valArrayNil["a"],
			}, {
				K: "b",
				V: valArrayNil["b"],
			}})

			colArrayLCNullable.AppendLen(len(valArrayNil))
			for k, v := range valArrayNil {
				colArrayLCNullable.KeyColumn().Append(k)
				colArrayLCNullable.ValueColumn().(*column.ArrayNullable[V]).AppendP(v)
			}
		}

		if withDelete && insertN == 0 {
			blockID.Remove(rows / 2)
			col.Remove(rows / 2)
			colNullable.Remove(rows / 2)
			colArray.Remove(rows / 2)
			colNullableArray.Remove(rows / 2)
			colLC.Remove(rows / 2)
			colLCNullable.Remove(rows / 2)
			colArrayLC.Remove(rows / 2)
			colArrayLCNullable.Remove(rows / 2)

			colInsert = colInsert[:rows/2]
			colInsertStruct = colInsertStruct[:rows/2]
			colNullableInsert = colNullableInsert[:rows/2]
			colNullableInsertStruct = colNullableInsertStruct[:rows/2]
			colArrayInsert = colArrayInsert[:rows/2]
			colArrayInsertStruct = colArrayInsertStruct[:rows/2]
			colArrayNullableInsert = colArrayNullableInsert[:rows/2]
			colArrayNullableInsertStruct = colArrayNullableInsertStruct[:rows/2]
			colLCInsert = colLCInsert[:rows/2]
			colLCInsertStruct = colLCInsertStruct[:rows/2]
			colLCNullableInsert = colLCNullableInsert[:rows/2]
			colLCNullableInsertStruct = colLCNullableInsertStruct[:rows/2]
			colLCArrayInsert = colLCArrayInsert[:rows/2]
			colLCArrayInsertStruct = colLCArrayInsertStruct[:rows/2]
			colLCNullableArrayInsert = colLCNullableArrayInsert[:rows/2]
			colLCNullableArrayInsertStruct = colLCNullableArrayInsertStruct[:rows/2]
		}

		err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
		test_map_%[1]s (
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
		require.NoError(t, err)
	}

	// test read all
	colRead := column.NewMap[string, V](
		column.NewString(),
		column.New[V](),
	)
	colNullableRead := column.NewMapNullable[string, V](
		column.NewString(),
		column.New[V]().Nullable(),
	)
	colArrayRead := column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().Array(),
	)
	colNullableArrayRead := column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().Nullable().Array(),
	)
	colLCRead := column.NewMap[string, V](
		column.NewString(),
		column.New[V]().LC(),
	)
	colLCNullableRead := column.NewMapNullable[string, V](
		column.NewString(),
		column.New[V]().LC().Nullable(),
	)
	colArrayLCRead := column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().LC().Array(),
	)
	colArrayLCNullableRead := column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().LC().Nullable().Array(),
	)
	var colData []map[string]V
	var colNullableData []map[string]*V
	var colArrayData []map[string][]V
	var colArrayNullableData []map[string][]*V
	var colLCData []map[string]V
	var colLCNullableData []map[string]*V
	var colLCArrayData []map[string][]V
	var colLCNullableArrayData []map[string][]*V
	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
	%[1]s,
	%[1]s_nullable,
	%[1]s_array,
	%[1]s_array_nullable,
	%[1]s_lc,
	%[1]s_nullable_lc,
	%[1]s_array_lc,
	%[1]s_array_lc_nullable
	FROM test_map_%[1]s order by block_id`, tableName),
		colRead,
		colNullableRead,
		colArrayRead,
		colNullableArrayRead,
		colLCRead,
		colLCNullableRead,
		colArrayLCRead,
		colArrayLCNullableRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	for selectStmt.Next() {
		colData = colRead.Read(colData)
		colNullableData = colNullableRead.ReadP(colNullableData)
		colArrayData = colArrayRead.Read(colArrayData)
		colNullableArrayReadKey := colNullableArrayRead.KeyColumn().Data()
		colNullableArrayReadValue := colNullableArrayRead.ValueColumn().(*column.ArrayNullable[V]).DataP()
		colNullableArrayRead.Each(func(start, end uint64) bool {
			val := make(map[string][]*V)
			for ki, key := range colNullableArrayReadKey[start:end] {
				val[key] = colNullableArrayReadValue[start:end][ki]
			}
			colArrayNullableData = append(colArrayNullableData, val)
			return true
		})
		colLCData = colLCRead.Read(colLCData)
		colLCNullableData = colLCNullableRead.ReadP(colLCNullableData)
		colLCArrayData = colArrayLCRead.Read(colLCArrayData)

		colArrayLCNullableReadKey := colArrayLCNullableRead.KeyColumn().Data()
		colArrayLCNullableReadValue := colArrayLCNullableRead.ValueColumn().(*column.ArrayNullable[V]).DataP()
		colArrayLCNullableRead.Each(func(start, end uint64) bool {
			val := make(map[string][]*V)
			for ki, key := range colArrayLCNullableReadKey[start:end] {
				val[key] = colArrayLCNullableReadValue[start:end][ki]
			}
			colLCNullableArrayData = append(colLCNullableArrayData, val)
			return true
		})
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNullableInsert, colNullableData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
	assert.Equal(t, colLCInsert, colLCData)
	assert.Equal(t, colLCNullableInsert, colLCNullableData)
	assert.Equal(t, colLCArrayInsert, colLCArrayData)
	assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)

	// test read Row
	colRead = column.NewMap[string, V](
		column.NewString(),
		column.New[V](),
	)
	colNullableRead = column.NewMapNullable[string, V](
		column.NewString(),
		column.New[V]().Nullable(),
	)
	colArrayRead = column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().Array(),
	)
	colNullableArrayRead = column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().Nullable().Array(),
	)
	colLCRead = column.NewMap[string, V](
		column.NewString(),
		column.New[V]().LC(),
	)
	colLCNullableRead = column.NewMapNullable[string, V](
		column.NewString(),
		column.New[V]().LC().Nullable(),
	)
	colArrayLCRead = column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().LC().Array(),
	)
	colArrayLCNullableRead = column.NewMap[string, []V](
		column.NewString(),
		column.New[V]().LC().Nullable().Array(),
	)
	colData = colData[:0]
	colNullableData = colNullableData[:0]
	colArrayData = colArrayData[:0]
	colArrayNullableData = colArrayNullableData[:0]
	colLCData = colLCData[:0]
	colLCNullableData = colLCNullableData[:0]
	colLCArrayData = colLCArrayData[:0]
	colLCNullableArrayData = colLCNullableArrayData[:0]
	selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
		%[1]s,
		%[1]s_nullable,
		%[1]s_array,
		%[1]s_array_nullable,
		%[1]s_lc,
		%[1]s_nullable_lc,
		%[1]s_array_lc,
		%[1]s_array_lc_nullable
		FROM test_map_%[1]s order by block_id`, tableName),
		colRead,
		colNullableRead,
		colArrayRead,
		colNullableArrayRead,
		colLCRead,
		colLCNullableRead,
		colArrayLCRead,
		colArrayLCNullableRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	for selectStmt.Next() {
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			colData = append(colData, colRead.Row(i))
			colNullableData = append(colNullableData, colNullableRead.RowP(i))
			colArrayData = append(colArrayData, colArrayRead.Row(i))
			colLCData = append(colLCData, colLCRead.Row(i))
			colLCNullableData = append(colLCNullableData, colLCNullableRead.RowP(i))
			colLCArrayData = append(colLCArrayData, colArrayLCRead.Row(i))
		}
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNullableInsert, colNullableData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colLCInsert, colLCData)
	assert.Equal(t, colLCNullableInsert, colLCNullableData)
	assert.Equal(t, colLCArrayInsert, colLCArrayData)

	// check dynamic column
	selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
		%[1]s,
		%[1]s_nullable,
		%[1]s_array,
		%[1]s_array_nullable,
		%[1]s_lc,
		%[1]s_nullable_lc,
		%[1]s_array_lc,
		%[1]s_array_lc_nullable
		FROM test_map_%[1]s order by block_id`, tableName),
	)

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()

	assert.Len(t, autoColumns, 8)

	assert.Equal(t, colRead.FullType(), autoColumns[0].FullType())
	assert.Equal(t, colRead.FullType(), autoColumns[0].FullType())
	assert.Equal(t, colNullableRead.FullType(), autoColumns[1].FullType())
	assert.Equal(t, colArrayRead.FullType(), autoColumns[2].FullType())
	assert.Equal(t, colNullableArrayRead.FullType(), autoColumns[3].FullType())
	assert.Equal(t, colLCRead.FullType(), autoColumns[4].FullType())
	assert.Equal(t, colLCNullableRead.FullType(), autoColumns[5].FullType())
	assert.Equal(t, colArrayLCRead.FullType(), autoColumns[6].FullType())
	assert.Equal(t, colArrayLCNullableRead.FullType(), autoColumns[7].FullType())
	colData = colData[:0]
	colNullableData = colNullableData[:0]
	colArrayData = colArrayData[:0]
	colArrayNullableData = colArrayNullableData[:0]
	colLCData = colLCData[:0]
	colLCNullableData = colLCNullableData[:0]
	colLCArrayData = colLCArrayData[:0]
	colLCNullableArrayData = colLCNullableArrayData[:0]

	var colDataStruct [][]struct {
		K string
		V V
	}
	var colNullableDataStruct [][]struct {
		K string
		V *V
	}
	var colArrayDataStruct [][]struct {
		K string
		V []V
	}
	var colArrayNullableDataStruct [][]struct {
		K string
		V []*V
	}
	var colLCDataStruct [][]struct {
		K string
		V V
	}
	var colLCNullableDataStruct [][]struct {
		K string
		V *V
	}
	var colLCArrayDataStruct [][]struct {
		K string
		V []V
	}
	var colLCNullableArrayDataStruct [][]struct {
		K string
		V []*V
	}
	rows := selectStmt.Rows()
	for rows.Next() {
		var colVal map[string]V
		var colNullableVal map[string]*V
		var colArrayVal map[string][]V
		var colArrayNullableVal map[string][]*V
		var colLCVal map[string]V
		var colLCNullableVal map[string]*V
		var colLCArrayVal map[string][]V
		var colLCNullableArrayVal map[string][]*V

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
		colData = append(colData, colVal)
		colNullableData = append(colNullableData, colNullableVal)
		colArrayData = append(colArrayData, colArrayVal)
		colArrayNullableData = append(colArrayNullableData, colArrayNullableVal)
		colLCData = append(colLCData, colLCVal)
		colLCNullableData = append(colLCNullableData, colLCNullableVal)
		colLCArrayData = append(colLCArrayData, colLCArrayVal)
		colLCNullableArrayData = append(colLCNullableArrayData, colLCNullableArrayVal)

		var colValStruct []struct {
			K string
			V V
		}
		var colNullableValStruct []struct {
			K string
			V *V
		}
		var colArrayValStruct []struct {
			K string
			V []V
		}
		var colArrayNullableValStruct []struct {
			K string
			V []*V
		}
		var colLCValStruct []struct {
			K string
			V V
		}
		var colLCNullableValStruct []struct {
			K string
			V *V
		}
		var colLCArrayValStruct []struct {
			K string
			V []V
		}
		var colLCNullableArrayValStruct []struct {
			K string
			V []*V
		}

		err = rows.Scan(
			&colValStruct,
			&colNullableValStruct,
			&colArrayValStruct,
			&colArrayNullableValStruct,
			&colLCValStruct,
			&colLCNullableValStruct,
			&colLCArrayValStruct,
			&colLCNullableArrayValStruct,
		)
		require.NoError(t, err)
		colDataStruct = append(colDataStruct, colValStruct)
		colNullableDataStruct = append(colNullableDataStruct, colNullableValStruct)
		colArrayDataStruct = append(colArrayDataStruct, colArrayValStruct)
		colArrayNullableDataStruct = append(colArrayNullableDataStruct, colArrayNullableValStruct)
		colLCDataStruct = append(colLCDataStruct, colLCValStruct)
		colLCNullableDataStruct = append(colLCNullableDataStruct, colLCNullableValStruct)
		colLCArrayDataStruct = append(colLCArrayDataStruct, colLCArrayValStruct)
		colLCNullableArrayDataStruct = append(colLCNullableArrayDataStruct, colLCNullableArrayValStruct)
	}
	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNullableInsert, colNullableData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
	assert.Equal(t, colLCInsert, colLCData)
	assert.Equal(t, colLCNullableInsert, colLCNullableData)
	assert.Equal(t, colLCArrayInsert, colLCArrayData)
	assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)
	for i := range colInsertStruct {
		sort.Slice(colInsertStruct[i], func(b, j int) bool {
			return colInsertStruct[i][b].K < colInsertStruct[i][j].K
		})
	}
	for i := range colDataStruct {
		sort.Slice(colDataStruct[i], func(b, j int) bool {
			return colDataStruct[i][b].K < colDataStruct[i][j].K
		})
	}
	assert.Equal(t, colInsertStruct, colDataStruct)
	for i := range colNullableInsertStruct {
		sort.Slice(colNullableInsertStruct[i], func(b, j int) bool {
			return colNullableInsertStruct[i][b].K < colNullableInsertStruct[i][j].K
		})
	}
	for i := range colNullableDataStruct {
		sort.Slice(colNullableDataStruct[i], func(b, j int) bool {
			return colNullableDataStruct[i][b].K < colNullableDataStruct[i][j].K
		})
	}
	assert.Equal(t, colNullableInsertStruct, colNullableDataStruct)
	for i := range colArrayInsertStruct {
		sort.Slice(colArrayInsertStruct[i], func(b, j int) bool {
			return colArrayInsertStruct[i][b].K < colArrayInsertStruct[i][j].K
		})
	}
	for i := range colArrayDataStruct {
		sort.Slice(colArrayDataStruct[i], func(b, j int) bool {
			return colArrayDataStruct[i][b].K < colArrayDataStruct[i][j].K
		})
	}
	assert.Equal(t, colArrayInsertStruct, colArrayDataStruct)
	for i := range colArrayNullableInsertStruct {
		sort.Slice(colArrayNullableInsertStruct[i], func(b, j int) bool {
			return colArrayNullableInsertStruct[i][b].K < colArrayNullableInsertStruct[i][j].K
		})
	}
	for i := range colArrayNullableDataStruct {
		sort.Slice(colArrayNullableDataStruct[i], func(b, j int) bool {
			return colArrayNullableDataStruct[i][b].K < colArrayNullableDataStruct[i][j].K
		})
	}
	assert.Equal(t, colArrayNullableInsertStruct, colArrayNullableDataStruct)
	for i := range colLCInsertStruct {
		sort.Slice(colLCInsertStruct[i], func(b, j int) bool {
			return colLCInsertStruct[i][b].K < colLCInsertStruct[i][j].K
		})
	}
	for i := range colLCDataStruct {
		sort.Slice(colLCDataStruct[i], func(b, j int) bool {
			return colLCDataStruct[i][b].K < colLCDataStruct[i][j].K
		})
	}
	assert.Equal(t, colLCInsertStruct, colLCDataStruct)
	for i := range colLCNullableInsertStruct {
		sort.Slice(colLCNullableInsertStruct[i], func(b, j int) bool {
			return colLCNullableInsertStruct[i][b].K < colLCNullableInsertStruct[i][j].K
		})
	}
	for i := range colLCNullableDataStruct {
		sort.Slice(colLCNullableDataStruct[i], func(b, j int) bool {
			return colLCNullableDataStruct[i][b].K < colLCNullableDataStruct[i][j].K
		})
	}
	assert.Equal(t, colLCNullableInsertStruct, colLCNullableDataStruct)
	for i := range colLCArrayInsertStruct {
		sort.Slice(colLCArrayInsertStruct[i], func(b, j int) bool {
			return colLCArrayInsertStruct[i][b].K < colLCArrayInsertStruct[i][j].K
		})
	}
	for i := range colLCArrayDataStruct {
		sort.Slice(colLCArrayDataStruct[i], func(b, j int) bool {
			return colLCArrayDataStruct[i][b].K < colLCArrayDataStruct[i][j].K
		})
	}
	assert.Equal(t, colLCArrayInsertStruct, colLCArrayDataStruct)
	for i := range colLCNullableArrayInsertStruct {
		sort.Slice(colLCNullableArrayInsertStruct[i], func(b, j int) bool {
			return colLCNullableArrayInsertStruct[i][b].K < colLCNullableArrayInsertStruct[i][j].K
		})
	}
	for i := range colLCNullableArrayDataStruct {
		sort.Slice(colLCNullableArrayDataStruct[i], func(b, j int) bool {
			return colLCNullableArrayDataStruct[i][b].K < colLCNullableArrayDataStruct[i][j].K
		})
	}
	assert.Equal(t, colLCNullableArrayInsertStruct, colLCNullableArrayDataStruct)

	selectStmt.Close()
}

func TestMapEmptyResult(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	// test read all
	colRead := column.NewMap[uint64, uint64](
		column.New[uint64](),
		column.New[uint64](),
	)

	selectStmt, err := conn.Select(context.Background(), `SELECT map(number,number) from system.numbers limit 0`,
		colRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	for selectStmt.Next() {
	}

	require.NoError(t, selectStmt.Err())
	assert.Equal(t, colRead.Data(), []map[uint64]uint64{})
	assert.Equal(t, colRead.TotalRows(), 0)
	colRead.Each(func(start, end uint64) bool {
		assert.Fail(t, "should not be called")
		return true
	})
}

func TestMapEmpty(t *testing.T) {
	t.Parallel()

	tableName := "map_empty"

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	err = conn.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_map_%s`, tableName),
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:  "allow_suspicious_low_cardinality_types",
			Value: "true",
		},
	}
	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_map_%[1]s (
				%[1]s Map(String,%[2]s),
				%[1]s_nullable Map(String,Nullable(%[2]s)),
				%[1]s_array Map(String,Array(%[2]s)),
				%[1]s_array_nullable Map(String,Array(Nullable(%[2]s))),
				%[1]s_lc Map(String,LowCardinality(%[2]s)),
				%[1]s_nullable_lc Map(String,LowCardinality(Nullable(%[2]s))),
				%[1]s_array_lc Map(String,Array(LowCardinality(%[2]s))),
				%[1]s_array_lc_nullable Map(String,Array(LowCardinality(Nullable(%[2]s))))
			) Engine=Memory`, tableName, "UInt16"), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	col := column.NewMap[string, uint16](
		column.NewString(),
		column.New[uint16](),
	)
	colNullable := column.NewMapNullable[string, uint16](
		column.NewString(),
		column.New[uint16]().Nullable(),
	)
	colArray := column.NewMap[string, []uint16](
		column.NewString(),
		column.New[uint16]().Array(),
	)
	colNullableArray := column.NewMap[string, []uint16](
		column.NewString(),
		column.New[uint16]().Nullable().Array(),
	)
	colLC := column.NewMap[string, uint16](
		column.NewString(),
		column.New[uint16]().LC(),
	)
	colLCNullable := column.NewMapNullable[string, uint16](
		column.NewString(),
		column.New[uint16]().LC().Nullable(),
	)
	colArrayLC := column.NewMap[string, []uint16](
		column.NewString(),
		column.New[uint16]().LC().Array(),
	)
	colArrayLCNullable := column.NewMap[string, []uint16](
		column.NewString(),
		column.New[uint16]().LC().Nullable().Array(),
	)

	col.Append(nil)
	col.Append(map[string]uint16{})
	colNullable.Append(nil)
	colNullable.AppendP(map[string]*uint16{})
	colArray.Append(nil)
	colArray.Append(map[string][]uint16{})
	colNullableArray.Append(nil)
	colNullableArray.Append(map[string][]uint16{})
	colLC.Append(nil)
	colLC.Append(map[string]uint16{})
	colLCNullable.Append(nil)
	colLCNullable.AppendP(map[string]*uint16{})
	colArrayLC.Append(nil)
	colArrayLC.Append(map[string][]uint16{})
	colArrayLCNullable.Append(nil)
	colArrayLCNullable.Append(map[string][]uint16{})
	err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
		test_map_%[1]s (
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
		col,
		colNullable,
		colArray,
		colNullableArray,
		colLC,
		colLCNullable,
		colArrayLC,
		colArrayLCNullable,
	)
	require.NoError(t, err)
	// test read all
	colRead := column.NewMap[string, uint16](
		column.NewString(),
		column.New[uint16](),
	)
	colNullableRead := column.NewMapNullable[string, uint16](
		column.NewString(),
		column.New[uint16]().Nullable(),
	)
	colArrayRead := column.NewMap[string, []uint16](
		column.NewString(),
		column.New[uint16]().Array(),
	)
	colNullableArrayRead := column.NewMap[string, []uint16](
		column.NewString(),
		column.New[uint16]().Nullable().Array(),
	)
	colLCRead := column.NewMap[string, uint16](
		column.NewString(),
		column.New[uint16]().LC(),
	)
	colLCNullableRead := column.NewMapNullable[string, uint16](
		column.NewString(),
		column.New[uint16]().LC().Nullable(),
	)
	colArrayLCRead := column.NewMap[string, []uint16](
		column.NewString(),
		column.New[uint16]().LC().Array(),
	)
	colArrayLCNullableRead := column.NewMap[string, []uint16](
		column.NewString(),
		column.New[uint16]().LC().Nullable().Array(),
	)
	var colData []map[string]uint16
	var colNullableData []map[string]*uint16
	var colArrayData []map[string][]uint16
	var colArrayNullableData []map[string][]*uint16
	var colLCData []map[string]uint16
	var colLCNullableData []map[string]*uint16
	var colLCArrayData []map[string][]uint16
	var colLCNullableArrayData []map[string][]*uint16
	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
	%[1]s,
	%[1]s_nullable,
	%[1]s_array,
	%[1]s_array_nullable,
	%[1]s_lc,
	%[1]s_nullable_lc,
	%[1]s_array_lc,
	%[1]s_array_lc_nullable
	FROM test_map_%[1]s`, tableName),
		colRead,
		colNullableRead,
		colArrayRead,
		colNullableArrayRead,
		colLCRead,
		colLCNullableRead,
		colArrayLCRead,
		colArrayLCNullableRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	for selectStmt.Next() {
		colData = colRead.Read(colData)
		colNullableData = colNullableRead.ReadP(colNullableData)
		colArrayData = colArrayRead.Read(colArrayData)
		colNullableArrayReadKey := colNullableArrayRead.KeyColumn().Data()
		colNullableArrayReadValue := colNullableArrayRead.ValueColumn().(*column.ArrayNullable[uint16]).DataP()
		colNullableArrayRead.Each(func(start, end uint64) bool {
			val := make(map[string][]*uint16)
			for ki, key := range colNullableArrayReadKey[start:end] {
				val[key] = colNullableArrayReadValue[start:end][ki]
			}
			colArrayNullableData = append(colArrayNullableData, val)
			return true
		})
		colLCData = colLCRead.Read(colLCData)
		colLCNullableData = colLCNullableRead.ReadP(colLCNullableData)
		colLCArrayData = colArrayLCRead.Read(colLCArrayData)

		colArrayLCNullableReadKey := colArrayLCNullableRead.KeyColumn().Data()
		colArrayLCNullableReadValue := colArrayLCNullableRead.ValueColumn().(*column.ArrayNullable[uint16]).DataP()
		colArrayLCNullableRead.Each(func(start, end uint64) bool {
			val := make(map[string][]*uint16)
			for ki, key := range colArrayLCNullableReadKey[start:end] {
				val[key] = colArrayLCNullableReadValue[start:end][ki]
			}
			colLCNullableArrayData = append(colLCNullableArrayData, val)
			return true
		})
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, []map[string]uint16{{}, {}}, colData)
	assert.Equal(t, []map[string]uint16{{}, {}}, colRead.Data())
	assert.Equal(t, []map[string]*uint16{{}, {}}, colNullableData)
	assert.Equal(t, []map[string]*uint16{{}, {}}, colNullableRead.DataP())
	assert.Equal(t, []map[string][]uint16{{}, {}}, colArrayData)
	assert.Equal(t, []map[string][]uint16{{}, {}}, colArrayRead.Data())
	assert.Equal(t, []map[string][]*uint16{{}, {}}, colArrayNullableData)
	assert.Equal(t, []map[string]uint16{{}, {}}, colLCData)
	assert.Equal(t, []map[string]uint16{{}, {}}, colLCRead.Data())
	assert.Equal(t, []map[string]*uint16{{}, {}}, colLCNullableData)
	assert.Equal(t, []map[string]*uint16{{}, {}}, colLCNullableRead.DataP())
	assert.Equal(t, []map[string][]uint16{{}, {}}, colLCArrayData)
	assert.Equal(t, []map[string][]uint16{{}, {}}, colArrayLCRead.Data())
	assert.Equal(t, []map[string][]*uint16{{}, {}}, colLCNullableArrayData)
}
