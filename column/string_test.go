package column_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestString(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	tableName := "string"
	chType := "String"
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
	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
			    block_id UInt8,
				%[1]s %[2]s,
				%[1]s_nullable Nullable(%[2]s),
				%[1]s_array Array(%[2]s),
				%[1]s_array_nullable Array(Nullable(%[2]s)),
				%[1]s_lc LowCardinality(%[2]s),
				%[1]s_nullable_lc LowCardinality(Nullable(%[2]s)),
				%[1]s_array_lc Array(LowCardinality(%[2]s)),
				%[1]s_array_lc_nullable Array(LowCardinality(Nullable(%[2]s)))
			) Engine=Memory`, tableName, chType), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	blockID := column.New[uint8]()
	col := column.NewString()
	colNullable := column.NewString().Nullable()
	colArray := column.NewString().Array()
	colNullableArray := column.NewString().Nullable().Array()
	colLC := column.NewString().LC()
	colLCNullable := column.NewString().LC().Nullable()
	colArrayLC := column.NewString().LC().Array()
	colArrayLCNullable := column.NewString().LC().Nullable().Array()
	var colInsert []string
	var colInsertByte [][]byte
	var colNullableInsert []*string
	var colArrayInsert [][]string
	var colArrayNullableInsert [][]*string
	var colLCInsert []string
	var colLCNullableInsert []*string
	var colLCArrayInsert [][]string
	var colLCNullableArrayInsert [][]*string

	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		for i := 0; i < rows; i++ {
			blockID.Append(uint8(insertN))
			val := fmt.Sprintf("string %d", i)
			val2 := strings.Repeat(val, 50)
			valArray := []string{val, val2}
			valArrayNil := []*string{&val, nil}

			col.Append(val)
			colInsert = append(colInsert, val)
			colInsertByte = append(colInsertByte, []byte(val))

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

		if insertN == 0 {
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
			colInsertByte = colInsertByte[:rows/2]
			colNullableInsert = colNullableInsert[:rows/2]
			colArrayInsert = colArrayInsert[:rows/2]
			colArrayNullableInsert = colArrayNullableInsert[:rows/2]
			colLCInsert = colLCInsert[:rows/2]
			colLCNullableInsert = colLCNullableInsert[:rows/2]
			colLCArrayInsert = colLCArrayInsert[:rows/2]
			colLCNullableArrayInsert = colLCNullableArrayInsert[:rows/2]
		}

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
		require.NoError(t, err)
	}

	// example read all

	colRead := column.NewString()
	colNullableRead := column.NewString().Nullable()
	colArrayRead := column.NewString().Array()
	colNullableArrayRead := column.NewString().Nullable().Array()
	colLCRead := column.NewString().LC()
	colLCNullableRead := column.NewString().LC().Nullable()
	colArrayLCRead := column.NewString().LC().Array()
	colArrayLCNullableRead := column.NewString().LC().Nullable().Array()
	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
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
		colArrayLCNullableRead)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var colData []string
	var colDataByte [][]byte
	var colDataByteByData [][]byte
	var colDataByteByRow [][]byte
	var colNullableData []*string
	var colArrayData [][]string
	var colArrayNullableData [][]*string
	var colLCData []string
	var colLCNullableData []*string
	var colLCArrayData [][]string
	var colLCNullableArrayData [][]*string

	for selectStmt.Next() {
		require.NoError(t, err)

		colData = colRead.Read(colData)
		colDataByte = colRead.ReadBytes(colDataByte)
		colDataByteByData = append(colDataByteByData, colRead.DataBytes()...)
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			colDataByteByRow = append(colDataByteByRow, colRead.RowBytes(i))
		}
		colNullableData = colNullableRead.ReadP(colNullableData)
		colArrayData = colArrayRead.Read(colArrayData)
		colArrayNullableData = colNullableArrayRead.ReadP(colArrayNullableData)
		colLCData = colLCRead.Read(colLCData)
		colLCNullableData = colLCNullableRead.ReadP(colLCNullableData)
		colLCArrayData = colArrayLCRead.Read(colLCArrayData)
		colLCNullableArrayData = colArrayLCNullableRead.ReadP(colLCNullableArrayData)
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colInsertByte, colDataByte)
	assert.Equal(t, colInsertByte, colDataByteByData)
	assert.Equal(t, colInsertByte, colDataByteByRow)
	assert.Equal(t, colNullableInsert, colNullableData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
	assert.Equal(t, colLCInsert, colLCData)
	assert.Equal(t, colLCNullableInsert, colLCNullableData)
	assert.Equal(t, colLCArrayInsert, colLCArrayData)
	assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)

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
		FROM test_%[1]s order by block_id`, tableName),
	)

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()

	assert.Len(t, autoColumns, 8)

	assert.Equal(t, colRead.ColumnType(), autoColumns[0].ColumnType())
	assert.Equal(t, colNullableRead.ColumnType(), autoColumns[1].ColumnType())
	assert.Equal(t, colArrayRead.ColumnType(), autoColumns[2].ColumnType())
	assert.Equal(t, colNullableArrayRead.ColumnType(), autoColumns[3].ColumnType())
	assert.Equal(t, colLCRead.ColumnType(), autoColumns[4].ColumnType())
	assert.Equal(t, colLCNullableRead.ColumnType(), autoColumns[5].ColumnType())
	assert.Equal(t, colArrayLCRead.ColumnType(), autoColumns[6].ColumnType())
	assert.Equal(t, colArrayLCNullableRead.ColumnType(), autoColumns[7].ColumnType())

	for selectStmt.Next() {
	}
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()
}
