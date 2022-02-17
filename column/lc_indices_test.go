package column_test

import (
	"context"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
	"github.com/vahid-sohrabloo/chconn/setting"
)

func TestLCIndicateUint16(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_lc_uint16_indicate`)
	require.NoError(t, err)
	require.Nil(t, res)
	settings := setting.NewSettings()
	settings.AllowSuspiciousLowCardinalityTypes(true)
	res, err = conn.ExecWithSetting(context.Background(), `CREATE TABLE test_lc_uint16_indicate (
				int64_lc LowCardinality(Int64)
			) Engine=Memory`, settings)
	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewInt64(false)
	colLC := column.NewLC(col)
	var colInsert []int64
	// to test uint16 indicate
	rows := math.MaxUint8 + 1
	for i := 1; i <= rows; i++ {
		val := int64(i)

		col.AppendDict(val)
		colInsert = append(colInsert, val)
	}

	err = conn.Insert(context.Background(), `INSERT INTO
		test_lc_uint16_indicate(int64_lc)
	VALUES`,
		colLC,
	)

	require.NoError(t, err)

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT int64_lc FROM
	test_lc_uint16_indicate`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewInt64(false)
	colLCRead := column.NewLC(colRead)

	var colDataDict []int64
	var colDataKeys []int
	var colData []int64

	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colLCRead)
		require.NoError(t, err)
		colRead.ReadAll(&colDataDict)
		colDataKeys = colDataKeys[:0]
		colLCRead.ReadAll(&colDataKeys)

		// it's just double check for Fill columns
		colDataFill := make([]int, selectStmt.RowsInBlock())
		colLCRead.Fill(colDataFill)
		assert.Equal(t, colDataKeys, colDataFill)

		for _, k := range colDataKeys {
			colData = append(colData, colDataDict[k])
		}
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colInsert, colData)

	selectStmt.Close()
}

func TestLCIndicateUint32(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_lc_uint32_indicate`)
	require.NoError(t, err)
	require.Nil(t, res)
	settings := setting.NewSettings()
	settings.AllowSuspiciousLowCardinalityTypes(true)
	res, err = conn.ExecWithSetting(context.Background(), `CREATE TABLE test_lc_uint32_indicate (
				int64_lc LowCardinality(Int64)
			) Engine=Memory`, settings)
	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewInt64(false)
	colLC := column.NewLC(col)
	var colInsert []int64
	// to test uint16 indicate
	rows := math.MaxUint16 + 1
	for i := 1; i <= rows; i++ {
		val := int64(i)

		col.AppendDict(val)
		colInsert = append(colInsert, val)
	}

	err = conn.Insert(context.Background(), `INSERT INTO
	test_lc_uint32_indicate(int64_lc)
	VALUES`,
		colLC,
	)

	require.NoError(t, err)

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT int64_lc FROM
	test_lc_uint32_indicate`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewInt64(false)
	colLCRead := column.NewLC(colRead)

	var colDataDict []int64
	var colDataKeys []int
	var colData []int64

	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colLCRead)
		require.NoError(t, err)
		colRead.ReadAll(&colDataDict)
		colDataKeys = colDataKeys[:0]
		colLCRead.ReadAll(&colDataKeys)

		// it's just double check for Fill columns
		colDataFill := make([]int, selectStmt.RowsInBlock())
		colLCRead.Fill(colDataFill)
		assert.Equal(t, colDataKeys, colDataFill)
		for _, k := range colDataKeys {
			colData = append(colData, colDataDict[k])
		}
	}

	require.NoError(t, selectStmt.Err())
	require.Equal(t, len(colInsert), len(colData))
	assert.Equal(t, colInsert, colData)

	selectStmt.Close()
}
