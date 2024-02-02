package column_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestArrayScanError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT ['1', '2', '3'] AS arr")
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())

	var invalidArr int
	err = rows.Scan(&invalidArr)
	require.Equal(t, "can't scan into dest[0]: dest must be a pointer to slice", err.Error())

	var invalidArrInside []int
	err = rows.Scan(&invalidArrInside)
	require.Equal(t, "can't scan into dest[0]: cannot scan array item 0: cannot scan text into *int", err.Error())
}
func TestArrayNullableScanError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT [NULL, '2', '3'] AS arr")
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())

	var invalidArr int
	err = rows.Scan(&invalidArr)
	require.Equal(t, "can't scan into dest[0]: dest must be a pointer to slice", err.Error())

	var invalidArrInside []int
	err = rows.Scan(&invalidArrInside)
	require.Equal(t, "can't scan into dest[0]: cannot scan array item 1: cannot scan text into *int", err.Error())
}

func TestArray2ScanError(t *testing.T) {
	t.Parallel()
	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT [['1', '2', '3']] AS arr")
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())

	var invalidArr int
	err = rows.Scan(&invalidArr)
	require.Equal(t, "can't scan into dest[0]: dest must be a pointer to slice", err.Error())

	var invalidArrInside []int
	err = rows.Scan(&invalidArrInside)
	require.Equal(t, "can't scan into dest[0]: cannot scan array item 0: dest must be a pointer to slice", err.Error())
}

func TestArray2NullableScanError(t *testing.T) {
	t.Parallel()
	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT [[NULL, '2', '3']] AS arr")
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())

	var invalidArr int
	err = rows.Scan(&invalidArr)
	require.Equal(t, "can't scan into dest[0]: dest must be a pointer to slice", err.Error())

	var invalidArrInside []int
	err = rows.Scan(&invalidArrInside)
	require.Equal(t, "can't scan into dest[0]: cannot scan array item 0: dest must be a pointer to slice", err.Error())
}

func TestArray3ScanError(t *testing.T) {
	t.Parallel()
	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT [[['1', '2', '3']]] AS arr")
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())

	var invalidArr int
	err = rows.Scan(&invalidArr)
	require.Equal(t, "can't scan into dest[0]: dest must be a pointer to slice", err.Error())

	var invalidArrInside []int
	err = rows.Scan(&invalidArrInside)
	require.Equal(t, "can't scan into dest[0]: cannot scan array item 0: dest must be a pointer to slice", err.Error())
}

func TestArray3NullableScanError(t *testing.T) {
	t.Parallel()
	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT [[[NULL, '2', '3']]] AS arr")
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())

	var invalidArr int
	err = rows.Scan(&invalidArr)
	require.Equal(t, "can't scan into dest[0]: dest must be a pointer to slice", err.Error())

	var invalidArrInside []int
	err = rows.Scan(&invalidArrInside)
	require.Equal(t, "can't scan into dest[0]: cannot scan array item 0: dest must be a pointer to slice", err.Error())
}

func TestArrayData(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	col := column.NewString().Array()
	stmt, err := conn.Select(ctx, "SELECT ['1', '2', '3'] AS arr", col)
	require.NoError(t, err)
	defer stmt.Close()
	assert.True(t, stmt.Next())
	assert.Equal(t, [][]string{{"1", "2", "3"}}, col.Data())
	assert.Equal(t, []string{"1", "2", "3"}, col.RowAny(0))
}

func TestArrayNullableData(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	col := column.NewString().Nullable().Array()
	stmt, err := conn.Select(ctx, "SELECT [NULL, '2', '3'] AS arr", col)
	require.NoError(t, err)
	defer stmt.Close()
	assert.True(t, stmt.Next())
	require.NoError(t, stmt.Err())
	str2 := "2"
	str3 := "3"
	assert.Equal(t, [][]*string{{nil, &str2, &str3}}, col.DataP())
	assert.Equal(t, []*string{nil, &str2, &str3}, col.RowAny(0))
}
func TestArray2Data(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	col := column.NewString().Array().Array()
	stmt, err := conn.Select(ctx, "SELECT [['1', '2', '3']] AS arr", col)
	require.NoError(t, err)
	defer stmt.Close()
	assert.True(t, stmt.Next())
	require.NoError(t, stmt.Err())
	assert.Equal(t, [][][]string{{{"1", "2", "3"}}}, col.Data())
	assert.Equal(t, [][]string{{"1", "2", "3"}}, col.RowAny(0))
}

func TestArray2NullableData(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	col := column.NewString().Nullable().Array().Array()
	stmt, err := conn.Select(ctx, "SELECT [[NULL, '2', '3']] AS arr", col)
	require.NoError(t, err)
	defer stmt.Close()
	assert.True(t, stmt.Next())
	require.NoError(t, stmt.Err())
	str2 := "2"
	str3 := "3"
	assert.Equal(t, [][][]*string{{{nil, &str2, &str3}}}, col.DataP())
	assert.Equal(t, [][]*string{{nil, &str2, &str3}}, col.RowAny(0))
}

func TestArray3Data(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	col := column.NewString().Array().Array().Array()
	stmt, err := conn.Select(ctx, "SELECT [[['1', '2', '3']]] AS arr", col)
	require.NoError(t, err)
	defer stmt.Close()
	assert.True(t, stmt.Next())
	require.NoError(t, stmt.Err())
	assert.Equal(t, [][][][]string{{{{"1", "2", "3"}}}}, col.Data())
	assert.Equal(t, [][][]string{{{"1", "2", "3"}}}, col.RowAny(0))
}

func TestArray3NullableData(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	col := column.NewString().Nullable().Array().Array().Array()
	stmt, err := conn.Select(ctx, "SELECT [[[NULL, '2', '3']]] AS arr", col)
	require.NoError(t, err)
	defer stmt.Close()
	assert.True(t, stmt.Next())
	require.NoError(t, stmt.Err())
	str2 := "2"
	str3 := "3"
	assert.Equal(t, [][][][]*string{{{{nil, &str2, &str3}}}}, col.DataP())
	assert.Equal(t, [][][]*string{{{nil, &str2, &str3}}}, col.RowAny(0))
}
