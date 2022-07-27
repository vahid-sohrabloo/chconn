package column_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v2"
	"github.com/vahid-sohrabloo/chconn/v2/column"
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
	col := column.NewString[string]()
	colNullable := column.NewString[string]().Nullable()
	colArray := column.NewString[string]().Array()
	colNullableArray := column.NewString[string]().Nullable().Array()
	colLC := column.NewString[string]().LC()
	colLCNullable := column.NewString[string]().Nullable().LC()
	colArrayLC := column.NewString[string]().LC().Array()
	colArrayLCNullable := column.NewString[string]().Nullable().LC().Array()
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

	colRead := column.NewString[string]()
	colNullableRead := column.NewString[string]().Nullable()
	colArrayRead := column.NewString[string]().Array()
	colNullableArrayRead := column.NewString[string]().Nullable().Array()
	colLCRead := column.NewString[string]().LC()
	colLCNullableRead := column.NewString[string]().Nullable().LC()
	colArrayLCRead := column.NewString[string]().LC().Array()
	colArrayLCNullableRead := column.NewString[string]().Nullable().LC().Array()
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

	assert.IsType(t, colRead, autoColumns[0])
	assert.IsType(t, colNullableRead, autoColumns[1])
	assert.IsType(t, colArrayRead, autoColumns[2])
	assert.IsType(t, colNullableArrayRead, autoColumns[3])
	assert.IsType(t, colLCRead, autoColumns[4])
	assert.IsType(t, colLCNullableRead, autoColumns[5])
	assert.IsType(t, colArrayLCRead, autoColumns[6])
	assert.IsType(t, colArrayLCNullableRead, autoColumns[7])

	for selectStmt.Next() {
	}
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()
}
