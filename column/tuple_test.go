package column_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
)

func TestTuple(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_tuple`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_tuple (
				tuple Tuple(Int64, LowCardinality(String), UInt8)
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	colInt64 := column.NewInt64(false)
	colString := column.NewString(false)
	colUint8 := column.NewUint8(false)
	colTuple := column.NewTuple(colInt64, column.NewLowCardinality(colString), colUint8)

	var colInt64Insert []int64
	var colStringInsert []string
	var colUint8Insert []uint8
	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		colInt64.Reset()
		colString.Reset()
		colUint8.Reset()

		for i := 0; i < rows; i++ {
			val := int64(i * -8)
			valStr := fmt.Sprintf("%d", val)

			colInt64.Append(val)
			colInt64Insert = append(colInt64Insert, val)

			colString.AppendStringDict(valStr)

			colStringInsert = append(colStringInsert, valStr)

			colUint8.Append(uint8(i))
			colUint8Insert = append(colUint8Insert, uint8(i))
		}

		err = conn.Insert(context.Background(), `INSERT INTO
			test_tuple (tuple)
		VALUES`,
			colTuple,
		)

		require.NoError(t, err)
	}

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT
	tuple
	FROM test_tuple`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colInt64Read := column.NewInt64(false)
	colStringRead := column.NewString(false)
	colStringLCRead := column.NewLC(colStringRead)
	colUint8Read := column.NewUint8(false)
	colTupleRead := column.NewTuple(colInt64Read, colStringLCRead, colUint8Read)

	var colInt64Data []int64
	var colStringData []string
	var colUint8Data []uint8

	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colTupleRead)
		require.NoError(t, err)
		colInt64Read.ReadAll(&colInt64Data)
		colStringDict := colStringRead.GetAllString()
		for colStringLCRead.Next() {
			colStringData = append(colStringData, colStringDict[colStringLCRead.Value()])
		}
		colUint8Read.ReadAll(&colUint8Data)
	}

	assert.Equal(t, colInt64Insert, colInt64Data)
	assert.Equal(t, colStringInsert, colStringData)
	assert.Equal(t, colUint8Insert, colUint8Data)
	require.NoError(t, selectStmt.Err())

	selectStmt.Close()

	conn.Close()
}
