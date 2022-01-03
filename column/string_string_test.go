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

// var emptyByte = make(string, 1024*10)

func TestStringString(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_string_string`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_string_string (
				string String,
				string_nullable Nullable(String),
				string_array Array(String),
				string_array_nullable Array(Nullable(String))
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewString(false)

	colArrayValues := column.NewString(false)
	colArray := column.NewArray(colArrayValues)

	colArrayValuesNil := column.NewString(true)
	colArrayNil := column.NewArray(colArrayValuesNil)

	colNil := column.NewString(true)

	var colInsert []string
	var colInsertArray [][]string
	var colInsertArrayNil [][]*string
	var colNilInsert []*string

	rows := 10
	for i := 1; i <= rows; i++ {
		val := fmt.Sprintf("%d", i)
		valArray := []string{val, fmt.Sprintf("%d", i+1)}
		valArrayNil := []*string{&val, nil}

		col.AppendString(val)
		colInsert = append(colInsert, val)

		// example insert array
		colInsertArray = append(colInsertArray, valArray)
		colArray.AppendLen(len(valArray))
		for _, v := range valArray {
			colArrayValues.AppendString(v)
		}

		// example insert nullable array
		colInsertArrayNil = append(colInsertArrayNil, valArrayNil)
		colArrayNil.AppendLen(len(valArrayNil))
		for _, v := range valArrayNil {
			colArrayValuesNil.AppendStringP(v)
		}

		// example add nullable
		if i%2 == 0 {
			colNilInsert = append(colNilInsert, &val)
			if i <= rows/2 {
				// example to add by poiner
				colNil.AppendStringP(&val)
			} else {
				// example to without poiner
				colNil.AppendString(val)
				colNil.AppendIsNil(false)
			}
		} else {
			colNilInsert = append(colNilInsert, nil)
			if i <= rows/2 {
				// example to add by poiner
				colNil.AppendP(nil)
			} else {
				// example to add without poiner
				colNil.AppendEmpty()
				colNil.AppendIsNil(true)
			}
		}
	}

	insertstmt, err := conn.Insert(context.Background(), `INSERT INTO
		test_string_string (string,string_nullable,string_array,string_array_nullable)
	VALUES`)

	require.NoError(t, err)
	require.Nil(t, res)

	err = insertstmt.Commit(context.Background(),
		col,
		colNil,
		colArray,
		colArrayNil,
	)
	require.NoError(t, err)

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT
		string,string_nullable,string_array,string_array_nullable
	FROM test_string_string`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewString(false)
	colNilRead := column.NewString(true)
	colArrayReadData := column.NewString(false)
	colArrayRead := column.NewArray(colArrayReadData)
	colArrayReadDataNil := column.NewString(true)
	colArrayReadNil := column.NewArray(colArrayReadDataNil)
	var colData []string
	var colNilData []*string
	var colArrayData [][]string
	var colArrayDataNil [][]*string

	var colArrayLens []int

	for selectStmt.Next() {
		err = selectStmt.NextColumn(colRead)
		require.NoError(t, err)
		colRead.ReadAllString(&colData)

		err = selectStmt.NextColumn(colNilRead)
		require.NoError(t, err)
		colNilRead.ReadAllStringP(&colNilData)

		// read array
		colArrayLens = colArrayLens[:0]
		err = selectStmt.NextColumn(colArrayRead)
		require.NoError(t, err)
		colArrayRead.ReadAll(&colArrayLens)

		for _, l := range colArrayLens {
			arr := make([]string, l)
			colArrayReadData.FillString(arr)
			colArrayData = append(colArrayData, arr)
		}

		// read nullable array
		colArrayLens = colArrayLens[:0]
		err = selectStmt.NextColumn(colArrayReadNil)
		require.NoError(t, err)
		colArrayRead.ReadAll(&colArrayLens)

		for _, l := range colArrayLens {
			arr := make([]*string, l)
			colArrayReadDataNil.FillStringP(arr)
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
		string,string_nullable,string_array,string_array_nullable
	FROM test_string_string`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead = column.NewString(false)
	colNilRead = column.NewString(true)
	colArrayReadData = column.NewString(false)
	colArrayRead = column.NewArray(colArrayReadData)
	colArrayReadDataNil = column.NewString(true)
	colArrayReadNil = column.NewArray(colArrayReadDataNil)
	colData = colData[:0]
	colNilData = colNilData[:0]
	colArrayData = colArrayData[:0]
	colArrayDataNil = colArrayDataNil[:0]

	for selectStmt.Next() {
		err = selectStmt.NextColumn(colRead)
		require.NoError(t, err)
		for colRead.Next() {
			colData = append(colData, colRead.ValueString())
		}

		// read nullable
		err = selectStmt.NextColumn(colNilRead)
		require.NoError(t, err)
		for colNilRead.Next() {
			colNilData = append(colNilData, colNilRead.ValueStringP())
		}

		// read array
		err = selectStmt.NextColumn(colArrayRead)
		require.NoError(t, err)
		for colArrayRead.Next() {
			arr := make([]string, colArrayRead.Value())
			colArrayReadData.FillString(arr)
			colArrayData = append(colArrayData, arr)
		}

		// read nullable array
		err = selectStmt.NextColumn(colArrayReadNil)
		require.NoError(t, err)
		for colArrayReadNil.Next() {
			arr := make([]*string, colArrayReadNil.Value())
			colArrayReadDataNil.FillStringP(arr)
			colArrayDataNil = append(colArrayDataNil, arr)
		}
	}

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNilInsert, colNilData)
	assert.Equal(t, colInsertArray, colArrayData)
	assert.Equal(t, colInsertArrayNil, colArrayDataNil)
	require.NoError(t, selectStmt.Err())

	selectStmt.Close()

	conn.RawConn().Close()
}
