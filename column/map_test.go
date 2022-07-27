package column_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v2"
	"github.com/vahid-sohrabloo/chconn/v2/column"
)

func TestMapUint8(t *testing.T) {
	testMapColumn(t, "UInt8", "uint8", func(i int) []uint8 {
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
	testMapColumn(t, "UInt16", "uint16", func(i int) []uint16 {
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
	testMapColumn(t, "UInt32", "uint32", func(i int) []uint32 {
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
	testMapColumn(t, "UInt64", "uint64", func(i int) []uint64 {
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
	testMapColumn(t, "Int8", "int8", func(i int) []int8 {
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
	testMapColumn(t, "Int16", "int16", func(i int) []int16 {
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
	testMapColumn(t, "Int32", "int32", func(i int) []int32 {
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
	testMapColumn(t, "Int64", "int64", func(i int) []int64 {
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
	testMapColumn(t, "Float32", "float32", func(i int) []float32 {
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
	testMapColumn(t, "Float64", "float64", func(i int) []float64 {
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

func testMapColumn[V comparable](
	t *testing.T,
	chType, tableName string,
	firstVal func(i int) []V,
	secondVal func(i int) []V,
) {
	t.Parallel()

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
			) Engine=Memory`, tableName, chType), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	col := column.NewMap[string, V](
		column.NewString[string](),
		column.New[V](),
	)
	colNullable := column.NewMapNullable[string, V](
		column.NewString[string](),
		column.New[V]().Nullable(),
	)
	colArray := column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().Array(),
	)
	colNullableArray := column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().Nullable().Array(),
	)
	colLC := column.NewMap[string, V](
		column.NewString[string](),
		column.New[V]().LC(),
	)
	colLCNullable := column.NewMapNullable[string, V](
		column.NewString[string](),
		column.New[V]().Nullable().LC(),
	)
	colArrayLC := column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().LC().Array(),
	)
	colArrayLCNullable := column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().Nullable().LC().Array(),
	)
	var colInsert []map[string]V
	var colNullableInsert []map[string]*V
	var colArrayInsert []map[string][]V
	var colArrayNullableInsert []map[string][]*V
	var colLCInsert []map[string]V
	var colLCNullableInsert []map[string]*V
	var colLCArrayInsert []map[string][]V
	var colLCNullableArrayInsert []map[string][]*V

	// SetWriteBuffer is not necessary. this just to show how to set write buffer
	col.SetWriteBuffer(10)
	colNullable.SetWriteBuffer(10)
	colArray.SetWriteBuffer(10)
	colNullableArray.SetWriteBuffer(10)
	colLC.SetWriteBuffer(10)
	colLCNullable.SetWriteBuffer(10)
	colArrayLC.SetWriteBuffer(10)
	colArrayLCNullable.SetWriteBuffer(10)
	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		for i := 0; i < rows; i++ {
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

			// example add nullable

			if i%2 == 0 {
				colNullableInsert = append(colNullableInsert, valNullable)
				colNullable.AppendP(valNullable)
				colLCNullableInsert = append(colLCNullableInsert, valNullable)
				colLCNullable.AppendP(valNullable)
			} else {
				colNullableInsert = append(colNullableInsert, valNullable2)
				colNullable.AppendP(valNullable2)
				colLCNullableInsert = append(colLCNullableInsert, valNullable2)
				colLCNullable.AppendP(valNullable2)
			}

			colArray.Append(valArray)
			colArrayInsert = append(colArrayInsert, valArray)

			colNullableArray.AppendLen(len(valArrayNil))
			for k, v := range valArrayNil {
				colNullableArray.KeyColumn().Append(k)
				colNullableArray.ValueColumn().(*column.ArrayNullable[V]).AppendP(v)
			}
			colArrayNullableInsert = append(colArrayNullableInsert, valArrayNil)

			colLCInsert = append(colLCInsert, val)
			colLC.Append(val)

			colLCArrayInsert = append(colLCArrayInsert, valArray)
			colArrayLC.Append(valArray)

			colLCNullableArrayInsert = append(colLCNullableArrayInsert, valArrayNil)
			colArrayLCNullable.AppendLen(len(valArrayNil))
			for k, v := range valArrayNil {
				colArrayLCNullable.KeyColumn().Append(k)
				colArrayLCNullable.ValueColumn().(*column.ArrayNullable[V]).AppendP(v)
			}
		}

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
	}

	// test read all
	colRead := column.NewMap[string, V](
		column.NewString[string](),
		column.New[V](),
	)
	colNullableRead := column.NewMapNullable[string, V](
		column.NewString[string](),
		column.New[V]().Nullable(),
	)
	colArrayRead := column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().Array(),
	)
	colNullableArrayRead := column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().Nullable().Array(),
	)
	colLCRead := column.NewMap[string, V](
		column.NewString[string](),
		column.New[V]().LC(),
	)
	colLCNullableRead := column.NewMapNullable[string, V](
		column.NewString[string](),
		column.New[V]().Nullable().LC(),
	)
	colArrayLCRead := column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().LC().Array(),
	)
	colArrayLCNullableRead := column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().Nullable().LC().Array(),
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
		colNullableArrayReadValue := colNullableArrayRead.ValueColumn().(*column.ArrayNullable[V]).DataP()
		colNullableArrayRead.Each(func(start, end uint64) {
			val := make(map[string][]*V)
			for ki, key := range colNullableArrayReadKey[start:end] {
				val[key] = colNullableArrayReadValue[start:end][ki]
			}
			colArrayNullableData = append(colArrayNullableData, val)
		})
		colLCData = colLCRead.Read(colLCData)
		colLCNullableData = colLCNullableRead.ReadP(colLCNullableData)
		colLCArrayData = colArrayLCRead.Read(colLCArrayData)

		colArrayLCNullableReadKey := colArrayLCNullableRead.KeyColumn().Data()
		colArrayLCNullableReadValue := colArrayLCNullableRead.ValueColumn().(*column.ArrayNullable[V]).DataP()
		colArrayLCNullableRead.Each(func(start, end uint64) {
			val := make(map[string][]*V)
			for ki, key := range colArrayLCNullableReadKey[start:end] {
				val[key] = colArrayLCNullableReadValue[start:end][ki]
			}
			colLCNullableArrayData = append(colLCNullableArrayData, val)
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
		column.NewString[string](),
		column.New[V](),
	)
	colNullableRead = column.NewMapNullable[string, V](
		column.NewString[string](),
		column.New[V]().Nullable(),
	)
	colArrayRead = column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().Array(),
	)
	colNullableArrayRead = column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().Nullable().Array(),
	)
	colLCRead = column.NewMap[string, V](
		column.NewString[string](),
		column.New[V]().LC(),
	)
	colLCNullableRead = column.NewMapNullable[string, V](
		column.NewString[string](),
		column.New[V]().Nullable().LC(),
	)
	colArrayLCRead = column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().LC().Array(),
	)
	colArrayLCNullableRead = column.NewMap[string, []V](
		column.NewString[string](),
		column.New[V]().Nullable().LC().Array(),
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
		FROM test_map_%[1]s`, tableName),
	)

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()

	assert.Len(t, autoColumns, 8)

	assert.IsType(t, &column.MapBase{}, autoColumns[0])
	assert.IsType(t, colRead.MapBase.KeyColumn(), autoColumns[0].(*column.MapBase).KeyColumn())
	assert.IsType(t, colRead.MapBase.ValueColumn(), autoColumns[0].(*column.MapBase).ValueColumn())
	assert.IsType(t, &column.MapBase{}, autoColumns[1])
	assert.IsType(t, colNullableRead.MapBase.KeyColumn(), autoColumns[1].(*column.MapBase).KeyColumn())
	assert.IsType(t, colNullableRead.MapBase.ValueColumn(), autoColumns[1].(*column.MapBase).ValueColumn())
	assert.IsType(t, &column.MapBase{}, autoColumns[2])
	assert.IsType(t, colArrayRead.MapBase.KeyColumn(), autoColumns[2].(*column.MapBase).KeyColumn())
	assert.IsType(t, colArrayRead.MapBase.ValueColumn(), autoColumns[2].(*column.MapBase).ValueColumn())
	assert.IsType(t, &column.MapBase{}, autoColumns[3])
	assert.IsType(t, colNullableArrayRead.MapBase.KeyColumn(), autoColumns[3].(*column.MapBase).KeyColumn())
	assert.IsType(t, colNullableArrayRead.MapBase.ValueColumn(), autoColumns[3].(*column.MapBase).ValueColumn())
	assert.IsType(t, &column.MapBase{}, autoColumns[4])
	assert.IsType(t, colLCRead.MapBase.KeyColumn(), autoColumns[4].(*column.MapBase).KeyColumn())
	assert.IsType(t, colLCRead.MapBase.ValueColumn(), autoColumns[4].(*column.MapBase).ValueColumn())
	assert.IsType(t, &column.MapBase{}, autoColumns[5])
	assert.IsType(t, colLCNullableRead.MapBase.KeyColumn(), autoColumns[5].(*column.MapBase).KeyColumn())
	assert.IsType(t, colLCNullableRead.MapBase.ValueColumn(), autoColumns[5].(*column.MapBase).ValueColumn())
	assert.IsType(t, &column.MapBase{}, autoColumns[6])
	assert.IsType(t, colArrayLCRead.MapBase.KeyColumn(), autoColumns[6].(*column.MapBase).KeyColumn())
	assert.IsType(t, colArrayLCRead.MapBase.ValueColumn(), autoColumns[6].(*column.MapBase).ValueColumn())
	assert.IsType(t, &column.MapBase{}, autoColumns[7])
	assert.IsType(t, colArrayLCNullableRead.MapBase.KeyColumn(), autoColumns[7].(*column.MapBase).KeyColumn())
	assert.IsType(t, colArrayLCNullableRead.MapBase.ValueColumn(), autoColumns[7].(*column.MapBase).ValueColumn())

	for selectStmt.Next() {
	}
	require.NoError(t, selectStmt.Err())
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
	colRead.Each(func(start, end uint64) {
		assert.Fail(t, "should not be called")
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
		column.NewString[string](),
		column.New[uint16](),
	)
	colNullable := column.NewMapNullable[string, uint16](
		column.NewString[string](),
		column.New[uint16]().Nullable(),
	)
	colArray := column.NewMap[string, []uint16](
		column.NewString[string](),
		column.New[uint16]().Array(),
	)
	colNullableArray := column.NewMap[string, []uint16](
		column.NewString[string](),
		column.New[uint16]().Nullable().Array(),
	)
	colLC := column.NewMap[string, uint16](
		column.NewString[string](),
		column.New[uint16]().LC(),
	)
	colLCNullable := column.NewMapNullable[string, uint16](
		column.NewString[string](),
		column.New[uint16]().Nullable().LC(),
	)
	colArrayLC := column.NewMap[string, []uint16](
		column.NewString[string](),
		column.New[uint16]().LC().Array(),
	)
	colArrayLCNullable := column.NewMap[string, []uint16](
		column.NewString[string](),
		column.New[uint16]().Nullable().LC().Array(),
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
		column.NewString[string](),
		column.New[uint16](),
	)
	colNullableRead := column.NewMapNullable[string, uint16](
		column.NewString[string](),
		column.New[uint16]().Nullable(),
	)
	colArrayRead := column.NewMap[string, []uint16](
		column.NewString[string](),
		column.New[uint16]().Array(),
	)
	colNullableArrayRead := column.NewMap[string, []uint16](
		column.NewString[string](),
		column.New[uint16]().Nullable().Array(),
	)
	colLCRead := column.NewMap[string, uint16](
		column.NewString[string](),
		column.New[uint16]().LC(),
	)
	colLCNullableRead := column.NewMapNullable[string, uint16](
		column.NewString[string](),
		column.New[uint16]().Nullable().LC(),
	)
	colArrayLCRead := column.NewMap[string, []uint16](
		column.NewString[string](),
		column.New[uint16]().LC().Array(),
	)
	colArrayLCNullableRead := column.NewMap[string, []uint16](
		column.NewString[string](),
		column.New[uint16]().Nullable().LC().Array(),
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
		colNullableArrayRead.Each(func(start, end uint64) {
			val := make(map[string][]*uint16)
			for ki, key := range colNullableArrayReadKey[start:end] {
				val[key] = colNullableArrayReadValue[start:end][ki]
			}
			colArrayNullableData = append(colArrayNullableData, val)
		})
		colLCData = colLCRead.Read(colLCData)
		colLCNullableData = colLCNullableRead.ReadP(colLCNullableData)
		colLCArrayData = colArrayLCRead.Read(colLCArrayData)

		colArrayLCNullableReadKey := colArrayLCNullableRead.KeyColumn().Data()
		colArrayLCNullableReadValue := colArrayLCNullableRead.ValueColumn().(*column.ArrayNullable[uint16]).DataP()
		colArrayLCNullableRead.Each(func(start, end uint64) {
			val := make(map[string][]*uint16)
			for ki, key := range colArrayLCNullableReadKey[start:end] {
				val[key] = colArrayLCNullableReadValue[start:end][ki]
			}
			colLCNullableArrayData = append(colLCNullableArrayData, val)
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
