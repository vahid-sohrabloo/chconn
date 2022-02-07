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

func TestIPv6(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_ipv6`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_ipv6 (
				ipv6 IPv6,
				ipv6_nullable Nullable(IPv6),
				ipv6_array Array(IPv6),
				ipv6_array_nullable Array(Nullable(IPv6))
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewIPv6(false)

	colArrayValues := column.NewIPv6(false)
	colArray := column.NewArray(colArrayValues)

	colArrayValuesNil := column.NewIPv6(true)
	colArrayNil := column.NewArray(colArrayValuesNil)

	colNil := column.NewIPv6(true)

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
			val := net.IP{
				byte(i), byte(i + 1), byte(i + 2), byte(i + 3),
				byte(i), byte(i + 1), byte(i + 2), byte(i + 3),
				byte(i), byte(i + 1), byte(i + 2), byte(i + 3),
				byte(i), byte(i + 1), byte(i + 2), byte(i + 3),
			}
			valArray := []net.IP{val, {
				byte(i), byte(i + 1), byte(i + 2), byte(i + 3),
				byte(i), byte(i + 1), byte(i + 2), byte(i + 3),
				byte(i), byte(i + 1), byte(i + 2), byte(i + 3),
				byte(i), byte(i + 1), byte(i + 2), byte(i + 3),
			}}
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
			test_ipv6 (ipv6,ipv6_nullable,ipv6_array,ipv6_array_nullable)
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
		ipv6,ipv6_nullable,ipv6_array,ipv6_array_nullable
	FROM test_ipv6`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewIPv6(false)
	colNilRead := column.NewIPv6(true)
	colArrayReadData := column.NewIPv6(false)
	colArrayRead := column.NewArray(colArrayReadData)
	colArrayReadDataNil := column.NewIPv6(true)
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
		ipv6,ipv6_nullable,ipv6_array,ipv6_array_nullable
	FROM test_ipv6`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead = column.NewIPv6(false)
	colNilRead = column.NewIPv6(true)
	colArrayReadData = column.NewIPv6(false)
	colArrayRead = column.NewArray(colArrayReadData)
	colArrayReadDataNil = column.NewIPv6(true)
	colArrayReadNil = column.NewArray(colArrayReadDataNil)
	colData = colData[:0]
	colNilData = colNilData[:0]
	colArrayData = colArrayData[:0]
	colArrayDataNil = colArrayDataNil[:0]

	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colRead, colNilRead, colArrayRead, colArrayReadNil)
		require.NoError(t, err)
		i := 0
		for colRead.Next() {
			colData = append(colData, colRead.Value())
			// Or use Row
			require.Equal(t, colRead.Value(), colRead.Row(i))
			i++
		}

		// read nullable
		i = 0
		for colNilRead.Next() {
			colNilData = append(colNilData, colNilRead.ValueP())
			// Or use RowP
			require.Equal(t, colNilRead.ValueP(), colNilRead.RowP(i))
			i++
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

	conn.Close()
}
