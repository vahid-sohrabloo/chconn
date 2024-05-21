package column_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestNullableAsNormal(t *testing.T) {
	tableName := "nullable"

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
			Name:  "allow_suspicious_low_cardinality_types",
			Value: "true",
		},
	}
	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
		block_id UInt8,
		%[1]s_nullable Nullable(Int64),
		%[1]s_array_nullable Array(Nullable(Int64)),
		%[1]s_nullable_lc LowCardinality(Nullable(Int64)),
		%[1]s_array_lc_nullable Array(LowCardinality(Nullable(Int64)))
			) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	blockID := column.New[uint8]()
	colNullable := column.New[int64]().Nullable()
	colNullableArray := column.New[int64]().Nullable().Array()
	colLCNullable := column.New[int64]().LC().Nullable()
	colArrayLCNullable := column.New[int64]().LC().Nullable().Array()

	var colInsert []int64
	var colArrayInsert [][]int64

	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		for i := 0; i < rows; i++ {
			val := int64(i + 1)
			blockID.Append(uint8(insertN))
			colNullable.Append(val)
			colNullableArray.Append([]int64{val, val + 1})
			colLCNullable.Append(val)
			colArrayLCNullable.Append([]int64{val, val + 1})
			colInsert = append(colInsert, val)
			colArrayInsert = append(colArrayInsert, []int64{val, val + 1})
		}

		err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				block_id,
				%[1]s_nullable,
				%[1]s_array_nullable,
				%[1]s_nullable_lc,
				%[1]s_array_lc_nullable
			)
		VALUES`, tableName),
			blockID,
			colNullable,
			colNullableArray,
			colLCNullable,
			colArrayLCNullable,
		)
		require.NoError(t, err)
	}

	// test read row
	colNullableRead := column.New[int64]().Nullable()
	colNullableArrayRead := column.New[int64]().Nullable().Array()
	colLCNullableRead := column.New[int64]().LC().Nullable()
	colArrayLCNullableRead := column.New[int64]().LC().Nullable().Array()

	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
			%[1]s_nullable,
			%[1]s_array_nullable,
			%[1]s_nullable_lc,
			%[1]s_array_lc_nullable
		FROM test_%[1]s order by block_id`, tableName),
		colNullableRead,
		colNullableArrayRead,
		colLCNullableRead,
		colArrayLCNullableRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var colData []int64
	var colArrayData [][]int64
	var colLCData []int64
	var colLCArrayData [][]int64
	var colDataNilRead []bool
	var colDataNilData []bool

	for selectStmt.Next() {
		colData = colNullableRead.Read(colData)
		colDataNilRead = colNullableRead.ReadNil(colDataNilRead)
		colDataNilData = append(colDataNilData, colNullableRead.DataNil()...)
		colArrayData = colNullableArrayRead.Read(colArrayData)
		colLCData = colLCNullableRead.Read(colLCData)
		colLCArrayData = colArrayLCNullableRead.Read(colLCArrayData)
	}

	require.NoError(t, selectStmt.Err())
	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colInsert, colLCData)
	assert.Equal(t, colArrayInsert, colLCArrayData)
	assert.Equal(t, colDataNilRead, colDataNilData)
	assert.Equal(t, make([]bool, len(colInsert)), colDataNilRead)

	// test read all
	colNullableRead = column.New[int64]().Nullable()
	colNullableArrayRead = column.New[int64]().Nullable().Array()
	colLCNullableRead = column.New[int64]().LC().Nullable()
	colArrayLCNullableRead = column.New[int64]().LC().Nullable().Array()
	selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
			%[1]s_nullable,
			%[1]s_array_nullable,
			%[1]s_nullable_lc,
			%[1]s_array_lc_nullable
		FROM test_%[1]s order by block_id`, tableName),
		colNullableRead,
		colNullableArrayRead,
		colLCNullableRead,
		colArrayLCNullableRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colData = colData[:0]
	colArrayData = colArrayData[:0]
	colLCData = colLCData[:0]
	colLCArrayData = colLCArrayData[:0]

	for selectStmt.Next() {
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			colData = append(colData, colNullableRead.Row(i))
			colArrayData = append(colArrayData, colNullableArrayRead.Row(i))
			colLCData = append(colLCData, colLCNullableRead.Row(i))
			colLCArrayData = append(colLCArrayData, colArrayLCNullableRead.Row(i))
		}
	}

	require.NoError(t, selectStmt.Err())
	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colInsert, colLCData)
	assert.Equal(t, colArrayInsert, colLCArrayData)
}
