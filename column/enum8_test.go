package column_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
)

func TestEnum8(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_enum8`)
	require.NoError(t, err)
	require.Nil(t, res)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_enum8 (
		enum8 Nullable(Enum8('hello' = 1, 'world' = 2))
	) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewEnum8(true)
	col.Append(1)
	col.AppendIsNil(false)

	err = conn.Insert(ctx, `INSERT INTO
	test_enum8 (enum8)
	VALUES`,
		col,
	)

	require.NoError(t, err)
	require.Nil(t, res)

	// example read all
	selectStmt, err := conn.Select(ctx, `SELECT
enum8
FROM test_enum8`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())
	colRead := column.NewEnum8(true)
	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colRead)
		require.NoError(t, err)
	}
	intMap, err := colRead.IntToStringMap()
	require.NoError(t, err)
	assert.Equal(t, intMap, map[int8]string{
		1: "hello",
		2: "world",
	})

	stringMap, err := colRead.StringToIntMap()
	require.NoError(t, err)
	assert.Equal(t, stringMap, map[string]int8{
		"hello": 1,
		"world": 2,
	})

	// double check for test cache

	intMap, err = colRead.IntToStringMap()
	require.NoError(t, err)
	assert.Equal(t, intMap, map[int8]string{
		1: "hello",
		2: "world",
	})

	stringMap, err = colRead.StringToIntMap()
	require.NoError(t, err)
	assert.Equal(t, stringMap, map[string]int8{
		"hello": 1,
		"world": 2,
	})

	require.NoError(t, selectStmt.Err())

	// set invalid num param
	colRead.SetType([]byte("Nullable(Enum8('hello' ))"))
	stringMap, err = colRead.StringToIntMap()
	assert.Equal(t, err.Error(), "invalid enum: 'hello' ")
	assert.Nil(t, stringMap)

	// set invalid  id
	colRead.SetType([]byte("Nullable(Enum8('hello' = d, 'world' = 2))"))
	stringMap, err = colRead.StringToIntMap()
	assert.Nil(t, stringMap)
	assert.Equal(t, err.Error(), "invalid enum id: d")

	selectStmt.Close()

	conn.RawConn().Close()
}
