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

func TestUint16Unsafe(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_uint16_unsafe`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_uint16_unsafe (
				uint16 UInt16
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewUint16(false)

	var colInsert []uint16

	rows := 10
	for i := 0; i < rows; i++ {
		val := uint16(i * 2)
		col.Append(val)
		colInsert = append(colInsert, val)
	}

	err = conn.Insert(context.Background(), `INSERT INTO
		test_uint16_unsafe (uint16)
	VALUES`, col)

	require.NoError(t, err)

	// example get all
	selectStmt, err := conn.Select(context.Background(), `SELECT
		uint16
	FROM test_uint16_unsafe`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewUint16(false)

	var colData []uint16

	for selectStmt.Next() {
		err = selectStmt.NextColumn(colRead)
		require.NoError(t, err)
		colData = append(colData, colRead.GetAllUnsafe()...)
	}

	assert.Equal(t, colInsert, colData)

	selectStmt.Close()

	// example read all
	selectStmt, err = conn.Select(context.Background(), `SELECT
		uint16
	FROM test_uint16_unsafe`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead = column.NewUint16(false)

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
