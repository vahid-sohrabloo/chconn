package column_test

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
)

func TestUUID(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_uuid`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_uuid (
				uuid UUID,
				uuid_nullable Nullable(UUID),
				uuid_array Array(UUID),
				uuid_array_nullable Array(Nullable(UUID))
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewUUID(false)

	colArrayValues := column.NewUUID(false)
	colArray := column.NewArray(colArrayValues)

	colArrayValuesNil := column.NewUUID(true)
	colArrayNil := column.NewArray(colArrayValuesNil)

	colNil := column.NewUUID(true)

	var colInsert [][16]byte
	var colInsertArray [][][16]byte
	var colInsertArrayNil [][]*[16]byte
	var colNilInsert []*[16]byte

	rows := 10
	val := uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a50")
	val2 := uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a51")
	valCast := [16]byte(val)
	for i := 1; i <= rows; i++ {
		valArray := [][16]byte{val, val2}
		valArrayNil := []*[16]byte{&valCast, nil}

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
			colNilInsert = append(colNilInsert, &valCast)
			if i <= rows/2 {
				// example to add by pointer
				colNil.AppendP(&valCast)
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
		test_uuid (uuid,uuid_nullable,uuid_array,uuid_array_nullable)
	VALUES`,
		col,
		colNil,
		colArray,
		colArrayNil)

	require.NoError(t, err)

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT
		uuid,uuid_nullable,uuid_array,uuid_array_nullable
	FROM test_uuid`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewUUID(false)
	colNilRead := column.NewUUID(true)
	colArrayReadData := column.NewUUID(false)
	colArrayRead := column.NewArray(colArrayReadData)
	colArrayReadDataNil := column.NewUUID(true)
	colArrayReadNil := column.NewArray(colArrayReadDataNil)
	var colData [][16]byte
	var colNilData []*[16]byte
	var colArrayData [][][16]byte
	var colArrayDataNil [][]*[16]byte

	var colArrayLens []int

	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colRead, colNilRead, colArrayRead, colArrayReadNil)
		require.NoError(t, err)
		colRead.ReadAll(&colData)

		colNilRead.ReadAllP(&colNilData)

		// read array
		colArrayLens = colArrayLens[:0]
		colArrayRead.ReadAll(&colArrayLens)

		for _, l := range colArrayLens {
			arr := make([][16]byte, l)
			colArrayReadData.Fill(arr)
			colArrayData = append(colArrayData, arr)
		}

		// read nullable array
		colArrayLens = colArrayLens[:0]
		colArrayRead.ReadAll(&colArrayLens)

		for _, l := range colArrayLens {
			arr := make([]*[16]byte, l)
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
		uuid,uuid_nullable,uuid_array,uuid_array_nullable
	FROM test_uuid`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead = column.NewUUID(false)
	colNilRead = column.NewUUID(true)
	colArrayReadData = column.NewUUID(false)
	colArrayRead = column.NewArray(colArrayReadData)
	colArrayReadDataNil = column.NewUUID(true)
	colArrayReadNil = column.NewArray(colArrayReadDataNil)
	colData = colData[:0]
	colNilData = colNilData[:0]
	colArrayData = colArrayData[:0]
	colArrayDataNil = colArrayDataNil[:0]

	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colRead, colNilRead, colArrayRead, colArrayReadNil)
		require.NoError(t, err)
		for colRead.Next() {
			colData = append(colData, colRead.Value())
		}

		// read nullable
		for colNilRead.Next() {
			colNilData = append(colNilData, colNilRead.ValueP())
		}

		// read array
		for colArrayRead.Next() {
			arr := make([][16]byte, colArrayRead.Value())
			colArrayReadData.Fill(arr)
			colArrayData = append(colArrayData, arr)
		}

		// read nullable array
		for colArrayReadNil.Next() {
			arr := make([]*[16]byte, colArrayReadNil.Value())
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

	conn.RawConn().Close()
}
