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

func TestSringStringLC(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_lc_string`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_lc_string (
				string_lc LowCardinality(String),
				string_lc_nullable LowCardinality(Nullable(String)),
				string_lc_array Array(LowCardinality(String)),
				string_lc_array_nullable Array(LowCardinality(Nullable(String)))
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewString(false)
	colLC := column.NewLC(col)

	colNil := column.NewString(true)
	colNilLC := column.NewLC(colNil)

	colArrayValues := column.NewString(false)
	collArrayLC := column.NewLC(colArrayValues)
	colArray := column.NewArray(collArrayLC)

	colArrayValuesNil := column.NewString(true)
	collArrayLCNil := column.NewLC(colArrayValuesNil)
	colArrayNil := column.NewArray(collArrayLCNil)

	var colInsert []string
	var colInsertArray [][]string
	var colInsertArrayNil [][]*string
	var colNilInsert []*string

	rows := 10
	for i := 1; i <= rows; i++ {
		val := fmt.Sprintf("%d", i)
		valArray := []string{val, fmt.Sprintf("%d", i+1)}
		valArrayNil := []*string{&val, nil}

		col.AppendStringDict(val)
		colInsert = append(colInsert, val)

		// // example insert array
		colInsertArray = append(colInsertArray, valArray)
		colArray.AppendLen(len(valArray))
		for _, v := range valArray {
			colArrayValues.AppendStringDict(v)
		}

		// example insert nullable array
		colInsertArrayNil = append(colInsertArrayNil, valArrayNil)
		colArrayNil.AppendLen(len(valArrayNil))
		for _, v := range valArrayNil {
			colArrayValuesNil.AppendStringDictP(v)
		}

		// example add nullable
		if i%2 == 0 {
			colNilInsert = append(colNilInsert, &val)
			if i <= rows/2 {
				// example to add by pointer
				colNil.AppendStringDictP(&val)
			} else {
				// example to without pointer
				colNil.AppendStringDict(val)
			}
		} else {
			colNilInsert = append(colNilInsert, nil)
			if i <= rows/2 {
				// example to add by pointer
				colNil.AppendDictP(nil)
			} else {
				// example to add without pointer
				colNil.AppendDictNil()
			}
		}
	}

	err = conn.Insert(context.Background(), `INSERT INTO
		test_lc_string(string_lc,string_lc_nullable,string_lc_array,string_lc_array_nullable)
	VALUES`,
		colLC,
		colNilLC,
		colArray,
		colArrayNil,
	)

	require.NoError(t, err)

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT
		string_lc,string_lc_nullable,string_lc_array,string_lc_array_nullable
	FROM test_lc_string`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewString(false)
	colLCRead := column.NewLC(colRead)

	colNilRead := column.NewString(true)
	colNilLCRead := column.NewLC(colNilRead)

	colArrayReadData := column.NewString(false)
	colArrayLCRead := column.NewLC(colArrayReadData)
	colArrayRead := column.NewArray(colArrayLCRead)

	colArrayReadDataNil := column.NewString(true)
	colArrayLCReadNil := column.NewLC(colArrayReadDataNil)
	colArrayReadNil := column.NewArray(colArrayLCReadNil)

	var colDataDict []string
	var colDataKeys []int
	var colData []string

	var colNilDataDict []string
	var colNilDataKeys []int
	var colNilData []*string

	var colArrayDataDict []string
	var colArrayData [][]string

	var colArrayDataDictNil []string
	var colArrayDataNil [][]*string

	var colArrayLens []int

	for selectStmt.Next() {
		err = selectStmt.NextColumn(colLCRead)
		require.NoError(t, err)
		colRead.ReadAllString(&colDataDict)
		colLCRead.ReadAll(&colDataKeys)

		for _, k := range colDataKeys {
			colData = append(colData, colDataDict[k])
		}
		err = selectStmt.NextColumn(colNilLCRead)
		require.NoError(t, err)
		colNilRead.ReadAllString(&colNilDataDict)
		colNilLCRead.ReadAll(&colNilDataKeys)

		for _, k := range colNilDataKeys {
			// 0 means nil
			if k == 0 {
				colNilData = append(colNilData, nil)
			} else {
				colNilData = append(colNilData, &colNilDataDict[k])
			}
		}

		// read array
		colArrayLens = colArrayLens[:0]
		err = selectStmt.NextColumn(colArrayRead)
		require.NoError(t, err)
		colArrayRead.ReadAll(&colArrayLens)
		colArrayReadData.ReadAllString(&colArrayDataDict)
		for _, l := range colArrayLens {
			arr := make([]int, l)
			arrData := make([]string, l)
			colArrayLCRead.Fill(arr)
			for i, k := range arr {
				arrData[i] = colArrayDataDict[k]
			}
			colArrayData = append(colArrayData, arrData)
		}

		// read array nil
		colArrayLens = colArrayLens[:0]
		err = selectStmt.NextColumn(colArrayReadNil)
		require.NoError(t, err)
		colArrayReadNil.ReadAll(&colArrayLens)
		colArrayReadDataNil.ReadAllString(&colArrayDataDictNil)
		for _, l := range colArrayLens {
			arr := make([]int, l)
			arrData := make([]*string, l)
			colArrayLCReadNil.Fill(arr)
			for i, k := range arr {
				// 0 means nil
				if k == 0 {
					arrData[i] = nil
				} else {
					arrData[i] = &colArrayDataDictNil[k]
				}
			}
			colArrayDataNil = append(colArrayDataNil, arrData)
		}
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNilInsert, colNilData)
	assert.Equal(t, colInsertArray, colArrayData)
	assert.Equal(t, colInsertArrayNil, colArrayDataNil)

	selectStmt.Close()

	// example one by one
	selectStmt, err = conn.Select(context.Background(), `SELECT
		string_lc,string_lc_nullable
	FROM test_lc_string`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead = column.NewString(false)
	colLCRead = column.NewLC(colRead)

	colNilRead = column.NewString(true)
	colNilLCRead = column.NewLC(colNilRead)

	colDataDict = colDataDict[:0]
	colData = colData[:0]

	colNilDataDict = colNilDataDict[:0]
	colNilData = colNilData[:0]

	for selectStmt.Next() {
		err = selectStmt.NextColumn(colLCRead)
		require.NoError(t, err)
		colRead.ReadAllString(&colDataDict)

		for colLCRead.Next() {
			colData = append(colData, colDataDict[colLCRead.Value()])
		}
		err = selectStmt.NextColumn(colNilLCRead)
		require.NoError(t, err)
		colNilRead.ReadAllString(&colNilDataDict)

		for colNilLCRead.Next() {
			k := colNilLCRead.Value()
			// 0 means nil
			if k == 0 {
				colNilData = append(colNilData, nil)
			} else {
				colNilData = append(colNilData, &colNilDataDict[k])
			}
		}
	}

	require.NoError(t, selectStmt.Err())

	selectStmt.Close()

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNilInsert, colNilData)
	conn.RawConn().Close()
}
