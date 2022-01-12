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

func TestDecimal256(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_decimal256`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_decimal256 (
				decimal256 Decimal256(3),
				decimal256_nullable Nullable(Decimal256(3)),
				decimal256_array Array(Decimal256(3)),
				decimal256_array_nullable Array(Nullable(Decimal256(3)))
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewDecimal256(false)

	colArrayValues := column.NewDecimal256(false)
	colArray := column.NewArray(colArrayValues)

	colArrayValuesNil := column.NewDecimal256(true)
	colArrayNil := column.NewArray(colArrayValuesNil)

	colNil := column.NewDecimal256(true)

	var colInsert [][]byte
	var colInsertArray [][][]byte
	var colInsertArrayNil [][]*[]byte
	var colNilInsert []*[]byte

	rows := 10
	for i := 1; i <= rows; i++ {
		val := make([]byte, column.Decimal256Size)
		val[0] = byte(i)
		val2 := make([]byte, column.Decimal256Size)
		val2[0] = byte(i + 1)
		valArray := [][]byte{val, val2}
		valArrayNil := []*[]byte{&val, nil}

		col.Append(val)
		colInsert = append(colInsert, val)

		// example insert array
		colInsertArray = append(colInsertArray, valArray)
		colArray.AppendLen(len(valArray))
		for _, v := range valArray {
			colArrayValues.Append(v)
		}

		// example insert nullable array
		colInsertArrayNil = append(colInsertArrayNil, valArrayNil)
		colArrayNil.AppendLen(len(valArrayNil))
		for _, v := range valArrayNil {
			colArrayValuesNil.AppendP(v)
		}

		// example add nullable
		if i%2 == 0 {
			colNilInsert = append(colNilInsert, &val)
			if i <= rows/2 {
				// example to add by pointer
				colNil.AppendP(&val)
			} else {
				// example to without pointer
				colNil.Append(val)
				colNil.AppendIsNil(false)
			}
		} else {
			colNilInsert = append(colNilInsert, nil)
			if i <= rows/2 {
				// example to add by pointer
				colNil.AppendP(nil)
			} else {
				// example to add without pointer
				colNil.AppendEmpty()
				colNil.AppendIsNil(true)
			}
		}
	}

	err = conn.Insert(context.Background(), `INSERT INTO
		test_decimal256 (decimal256,decimal256_nullable,decimal256_array,decimal256_array_nullable)
	VALUES`,
		col,
		colNil,
		colArray,
		colArrayNil)

	require.NoError(t, err)

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT
		decimal256,decimal256_nullable,decimal256_array,decimal256_array_nullable
	FROM test_decimal256`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewDecimal256(false)
	colNilRead := column.NewDecimal256(true)
	colArrayReadData := column.NewDecimal256(false)
	colArrayRead := column.NewArray(colArrayReadData)
	colArrayReadDataNil := column.NewDecimal256(true)
	colArrayReadNil := column.NewArray(colArrayReadDataNil)
	var colData [][]byte
	var colNilData []*[]byte
	var colArrayData [][][]byte
	var colArrayDataNil [][]*[]byte

	var colArrayLens []int

	for selectStmt.Next() {
		err = selectStmt.NextColumn(colRead)
		require.NoError(t, err)
		colRead.ReadAll(&colData)

		err = selectStmt.NextColumn(colNilRead)
		require.NoError(t, err)
		colNilRead.ReadAllP(&colNilData)

		// read array
		colArrayLens = colArrayLens[:0]
		err = selectStmt.NextColumn(colArrayRead)
		require.NoError(t, err)
		colArrayRead.ReadAll(&colArrayLens)

		for _, l := range colArrayLens {
			arr := make([][]byte, l)
			colArrayReadData.Fill(arr)
			colArrayData = append(colArrayData, arr)
		}

		// read nullable array
		colArrayLens = colArrayLens[:0]
		err = selectStmt.NextColumn(colArrayReadNil)
		require.NoError(t, err)
		colArrayRead.ReadAll(&colArrayLens)

		for _, l := range colArrayLens {
			arr := make([]*[]byte, l)
			colArrayReadDataNil.FillP(arr)
			colArrayDataNil = append(colArrayDataNil, arr)
		}
	}

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNilInsert, colNilData)
	assert.Equal(t, colInsertArray, colArrayData)
	assert.Equal(t, colInsertArrayNil, colArrayDataNil)
	require.NoError(t, selectStmt.Err())

	selectStmt.Close()

	// example one by one
	selectStmt, err = conn.Select(context.Background(), `SELECT
		decimal256,decimal256_nullable,decimal256_array,decimal256_array_nullable FROM
	test_decimal256`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead = column.NewDecimal256(false)
	colNilRead = column.NewDecimal256(true)
	colArrayReadData = column.NewDecimal256(false)
	colArrayRead = column.NewArray(colArrayReadData)
	colArrayReadDataNil = column.NewDecimal256(true)
	colArrayReadNil = column.NewArray(colArrayReadDataNil)
	colData = colData[:0]
	colNilData = colNilData[:0]
	colArrayData = colArrayData[:0]
	colArrayDataNil = colArrayDataNil[:0]

	for selectStmt.Next() {
		err = selectStmt.NextColumn(colRead)
		require.NoError(t, err)
		for colRead.Next() {
			colData = append(colData, colRead.Value())
		}

		// read nullable
		err = selectStmt.NextColumn(colNilRead)
		require.NoError(t, err)
		for colNilRead.Next() {
			colNilData = append(colNilData, colNilRead.ValueP())
		}

		// read array
		err = selectStmt.NextColumn(colArrayRead)
		require.NoError(t, err)
		for colArrayRead.Next() {
			arr := make([][]byte, colArrayRead.Value())
			colArrayReadData.Fill(arr)
			colArrayData = append(colArrayData, arr)
		}

		// read nullable array
		err = selectStmt.NextColumn(colArrayReadNil)
		require.NoError(t, err)
		for colArrayReadNil.Next() {
			arr := make([]*[]byte, colArrayReadNil.Value())
			colArrayReadDataNil.FillP(arr)
			colArrayDataNil = append(colArrayDataNil, arr)
		}
	}

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNilInsert, colNilData)
	assert.Equal(t, colInsertArray, colArrayData)
	assert.Equal(t, colInsertArrayNil, colArrayDataNil)
	require.NoError(t, selectStmt.Err())

	selectStmt.Close()

	conn.Close(context.Background())
}
