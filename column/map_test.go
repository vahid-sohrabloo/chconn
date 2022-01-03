package column_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
)

func TestMap(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_map`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_map (
				map Map(String, UInt64)
		) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	colKey := column.NewString(false)
	colVal := column.NewUint64(false)
	colMap := column.NewMap(colKey, colVal)

	// it doesn't necessary to use map. we can use two array.
	var colInsert []map[string]uint64

	rows := 10
	for i := 1; i <= rows; i++ {
		val := map[string]uint64{
			"key1": uint64(i),
			"key2": uint64(i + 1),
			"key3": uint64(i + 2),
		}

		// example insert array
		colInsert = append(colInsert, val)
		colMap.AppendLen(len(val))
		for k, v := range val {
			colKey.AppendString(k)
			colVal.Append(v)
		}
	}

	insertstmt, err := conn.Insert(context.Background(), `INSERT INTO
	test_map (map)
	VALUES`)

	require.NoError(t, err)
	require.Nil(t, res)

	err = insertstmt.Commit(context.Background(),
		colMap,
	)
	require.NoError(t, err)

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT
	map
	FROM test_map`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colKey = column.NewString(false)
	colVal = column.NewUint64(false)
	colMap = column.NewMap(colKey, colVal)
	var colDataKey [][]string
	var colDataValue [][]uint64
	var colArrayLens []int

	for selectStmt.Next() {
		// read array
		colArrayLens = colArrayLens[:0]
		err = selectStmt.NextColumn(colMap)
		require.NoError(t, err)
		colMap.ReadAll(&colArrayLens)

		for _, l := range colArrayLens {
			arrKey := make([]string, l)
			arrValue := make([]uint64, l)
			colKey.FillString(arrKey)
			colVal.Fill(arrValue)
			colDataKey = append(colDataKey, arrKey)
			colDataValue = append(colDataValue, arrValue)
		}
	}
	require.Equal(t, len(colInsert), len(colDataKey))
	require.Equal(t, len(colInsert), len(colDataValue))
	for i, val := range colDataKey {
		for y, kData := range val {
			assert.Equal(t, colInsert[i][kData], colDataValue[i][y])
		}
	}
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()

	conn.Close(context.Background())
}
