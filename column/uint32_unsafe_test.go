//go:build (386 || amd64 || amd64p32 || arm || arm64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64) && !purego
// +build 386 amd64 amd64p32 arm arm64 mipsle mips64le mips64p32le ppc64le riscv riscv64
// +build !purego

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

func TestUint32Unsafe(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_uint32_unsafe`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_uint32_unsafe (
				uint32 UInt32
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewUint32(false)

	var colInsert []uint32

	rows := 10
	for i := 0; i < rows; i++ {
		val := uint32(i * 4)
		col.Append(val)
		colInsert = append(colInsert, val)
	}

	err = conn.Insert(context.Background(), `INSERT INTO
		test_uint32_unsafe (uint32)
	VALUES`, col)

	require.NoError(t, err)

	// example get all
	selectStmt, err := conn.Select(context.Background(), `SELECT
		uint32
	FROM test_uint32_unsafe`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewUint32(false)

	var colData []uint32

	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colRead)
		require.NoError(t, err)
		colData = append(colData, colRead.GetAllUnsafe()...)
	}

	assert.Equal(t, colInsert, colData)

	selectStmt.Close()

	conn.Close()
}
