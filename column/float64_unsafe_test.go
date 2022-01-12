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

func TestFloat64Unsafe(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_float64_unsafe`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_float64_unsafe (
				float64 Float64
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewFloat64(false)

	var colInsert []float64

	rows := 10
	for i := 0; i < rows; i++ {
		val := float64(i * -4)
		col.Append(val)
		colInsert = append(colInsert, val)
	}

	err = conn.Insert(context.Background(), `INSERT INTO
		test_float64_unsafe (float64)
	VALUES`, col)

	require.NoError(t, err)

	// example get all
	selectStmt, err := conn.Select(context.Background(), `SELECT
		float64
	FROM test_float64_unsafe`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewFloat64(false)

	var colData []float64

	for selectStmt.Next() {
		err = selectStmt.NextColumn(colRead)
		require.NoError(t, err)
		colData = append(colData, colRead.GetAllUnsafe()...)
	}

	assert.Equal(t, colInsert, colData)

	selectStmt.Close()

	// example read all
	selectStmt, err = conn.Select(context.Background(), `SELECT
		float64
	FROM test_float64_unsafe`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead = column.NewFloat64(false)

	colData = colData[:0]

	for selectStmt.Next() {
		err = selectStmt.NextColumn(colRead)
		require.NoError(t, err)
		colRead.ReadAllUnsafe(&colData)
	}

	assert.Equal(t, colInsert, colData)

	selectStmt.Close()

	conn.Close(context.Background())
}
