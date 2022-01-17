package column_test

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
)

func TestIPv4(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_ipv4`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_ipv4 (
				ipv4 IPv4,
				ipv4_nullable Nullable(IPv4),
				ipv4_array Array(IPv4),
				ipv4_array_nullable Array(Nullable(IPv4))
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewIPv4(false)

	colArrayValues := column.NewIPv4(false)
	colArray := column.NewArray(colArrayValues)

	colArrayValuesNil := column.NewIPv4(true)
	colArrayNil := column.NewArray(colArrayValuesNil)

	colNil := column.NewIPv4(true)

	var colInsert []net.IP
	var colInsertArray [][]net.IP
	var colInsertArrayNil [][]*net.IP
	var colNilInsert []*net.IP
	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		col.Reset()
		colArrayValues.Reset()
		colArray.Reset()
		colArrayValuesNil.Reset()
		colArrayNil.Reset()
		colNil.Reset()
		for i := 0; i < rows; i++ {
			val := net.IPv4(byte(i), byte(i+1), byte(i+2), byte(i+3)).To4()
			valArray := []net.IP{val, net.IPv4(byte(i+1), byte(i+2), byte(i+3), byte(i+4)).To4()}
			valArrayNil := []*net.IP{&val, nil}

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
			test_ipv4 (ipv4,ipv4_nullable,ipv4_array,ipv4_array_nullable)
		VALUES`,
			col,
			colNil,
			colArray,
			colArrayNil,
		)

		require.NoError(t, err)
	}

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT
		ipv4,ipv4_nullable,ipv4_array,ipv4_array_nullable
	FROM test_ipv4`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewIPv4(false)
	colNilRead := column.NewIPv4(true)
	colArrayReadData := column.NewIPv4(false)
	colArrayRead := column.NewArray(colArrayReadData)
	colArrayReadDataNil := column.NewIPv4(true)
	colArrayReadNil := column.NewArray(colArrayReadDataNil)
	var colData []net.IP
	var colNilData []*net.IP
	var colArrayData [][]net.IP
	var colArrayDataNil [][]*net.IP

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
			arr := make([]net.IP, l)
			colArrayReadData.Fill(arr)
			colArrayData = append(colArrayData, arr)
		}

		// read nullable array
		colArrayLens = colArrayLens[:0]
		colArrayRead.ReadAll(&colArrayLens)

		for _, l := range colArrayLens {
			arr := make([]*net.IP, l)
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
		ipv4,ipv4_nullable,ipv4_array,ipv4_array_nullable
	FROM test_ipv4`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead = column.NewIPv4(false)
	colNilRead = column.NewIPv4(true)
	colArrayReadData = column.NewIPv4(false)
	colArrayRead = column.NewArray(colArrayReadData)
	colArrayReadDataNil = column.NewIPv4(true)
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
			arr := make([]net.IP, colArrayRead.Value())
			colArrayReadData.Fill(arr)
			colArrayData = append(colArrayData, arr)
		}

		// read nullable array
		for colArrayReadNil.Next() {
			arr := make([]*net.IP, colArrayReadNil.Value())
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
