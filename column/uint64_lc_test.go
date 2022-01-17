package column_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
	"github.com/vahid-sohrabloo/chconn/setting"
)

func TestUint64LC(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_lc_uint64`)
	require.NoError(t, err)
	require.Nil(t, res)
	settings := setting.NewSettings()
	settings.AllowSuspiciousLowCardinalityTypes(true)
	res, err = conn.ExecWithSetting(context.Background(), `CREATE TABLE test_lc_uint64 (
				uint64_lc LowCardinality(UInt64),
				uint64_lc_nullable LowCardinality(Nullable(UInt64)),
				uint64_lc_array Array(LowCardinality(UInt64)),
				uint64_lc_array_nullable Array(LowCardinality(Nullable(UInt64)))
			) Engine=Memory`, settings)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewUint64(false)
	colLC := column.NewLC(col)

	colNil := column.NewUint64(true)
	colNilLC := column.NewLC(colNil)

	colArrayValues := column.NewUint64(false)
	collArrayLC := column.NewLC(colArrayValues)
	colArray := column.NewArray(collArrayLC)

	colArrayValuesNil := column.NewUint64(true)
	collArrayLCNil := column.NewLC(colArrayValuesNil)
	colArrayNil := column.NewArray(collArrayLCNil)

	var colInsert []uint64
	var colInsertArray [][]uint64
	var colInsertArrayNil [][]*uint64
	var colNilInsert []*uint64

	rows := 10
	for i := 1; i <= rows; i++ {
		val := uint64(i * 8)
		valArray := []uint64{val, uint64(i*8) + 1}
		valArrayNil := []*uint64{&val, nil}

		col.AppendDict(val)
		colInsert = append(colInsert, val)

		// // example insert array
		colInsertArray = append(colInsertArray, valArray)
		colArray.AppendLen(len(valArray))
		for _, v := range valArray {
			colArrayValues.AppendDict(v)
		}

		// example insert nullable array
		colInsertArrayNil = append(colInsertArrayNil, valArrayNil)
		colArrayNil.AppendLen(len(valArrayNil))
		for _, v := range valArrayNil {
			colArrayValuesNil.AppendDictP(v)
		}

		// example add nullable
		if i%2 == 0 {
			colNilInsert = append(colNilInsert, &val)
			if i <= rows/2 {
				// example to add by pointer
				colNil.AppendDictP(&val)
			} else {
				// example to without pointer
				colNil.AppendDict(val)
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
		test_lc_uint64(uint64_lc,uint64_lc_nullable,uint64_lc_array,uint64_lc_array_nullable)
	VALUES`,
		colLC,
		colNilLC,
		colArray,
		colArrayNil,
	)

	require.NoError(t, err)

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT uint64_lc,
		uint64_lc_nullable,uint64_lc_array,uint64_lc_array_nullable FROM
	test_lc_uint64`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewUint64(false)
	colLCRead := column.NewLC(colRead)

	colNilRead := column.NewUint64(true)
	colNilLCRead := column.NewLC(colNilRead)

	colArrayReadData := column.NewUint64(false)
	colArrayLCRead := column.NewLC(colArrayReadData)
	colArrayRead := column.NewArray(colArrayLCRead)

	colArrayReadDataNil := column.NewUint64(true)
	colArrayLCReadNil := column.NewLC(colArrayReadDataNil)
	colArrayReadNil := column.NewArray(colArrayLCReadNil)

	var colDataDict []uint64
	var colDataKeys []int
	var colData []uint64

	var colNilDataDict []uint64
	var colNilDataKeys []int
	var colNilData []*uint64

	var colArrayDataDict []uint64
	var colArrayData [][]uint64

	var colArrayDataDictNil []uint64
	var colArrayDataNil [][]*uint64

	var colArrayLens []int

	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colLCRead, colNilLCRead, colArrayRead, colArrayReadNil)
		require.NoError(t, err)
		colRead.ReadAll(&colDataDict)
		colLCRead.ReadAll(&colDataKeys)

		for _, k := range colDataKeys {
			colData = append(colData, colDataDict[k])
		}
		colNilRead.ReadAll(&colNilDataDict)
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
		colArrayRead.ReadAll(&colArrayLens)
		colArrayReadData.ReadAll(&colArrayDataDict)
		for _, l := range colArrayLens {
			arr := make([]int, l)
			arrData := make([]uint64, l)
			colArrayLCRead.Fill(arr)
			for i, k := range arr {
				arrData[i] = colArrayDataDict[k]
			}
			colArrayData = append(colArrayData, arrData)
		}

		// read array nil
		colArrayLens = colArrayLens[:0]
		colArrayReadNil.ReadAll(&colArrayLens)
		colArrayReadDataNil.ReadAll(&colArrayDataDictNil)
		for _, l := range colArrayLens {
			arr := make([]int, l)
			arrData := make([]*uint64, l)
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
		uint64_lc,uint64_lc_nullable FROM
	test_lc_uint64`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead = column.NewUint64(false)
	colLCRead = column.NewLC(colRead)

	colNilRead = column.NewUint64(true)
	colNilLCRead = column.NewLC(colNilRead)

	colDataDict = colDataDict[:0]
	colData = colData[:0]

	colNilDataDict = colNilDataDict[:0]
	colNilData = colNilData[:0]

	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colLCRead, colNilLCRead)
		require.NoError(t, err)
		colRead.ReadAll(&colDataDict)

		for colLCRead.Next() {
			colData = append(colData, colDataDict[colLCRead.Value()])
		}
		colNilRead.ReadAll(&colNilDataDict)

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
	conn.Close(context.Background())
}
