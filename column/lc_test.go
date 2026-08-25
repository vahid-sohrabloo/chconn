package column_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestLcIndicator16(t *testing.T) {
	tableName := "lc_indicator_16"

	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	err = conn.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_%s`, tableName),
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:  "allow_suspicious_low_cardinality_types",
			Value: "true",
		},
	}

	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
		%[1]s_lc LowCardinality(Int64)
			) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	col := column.New[int64]().LC()

	var colInsert []int64

	rows := int(^uint8(0)) + 10
	for i := range rows {
		val := int64(i + 1)
		col.Append(val)
		colInsert = append(colInsert, val)
	}

	err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				%[1]s_lc
			)
		VALUES`, tableName),
		col,
	)
	require.NoError(t, err)

	// test read row
	colRead := column.New[int64]().LC()

	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
			%[1]s_lc
		FROM test_%[1]s`, tableName),
		colRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var colData []int64

	for selectStmt.Next() {
		colData = colRead.Read(colData)
	}

	require.NoError(t, selectStmt.Err())
	assert.Equal(t, colInsert, colData)
}

func TestLcIndicator32(t *testing.T) {
	tableName := "lc_indicator_32"

	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	err = conn.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_%s`, tableName),
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:  "allow_suspicious_low_cardinality_types",
			Value: "true",
		},
	}

	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
		%[1]s_lc LowCardinality(Int64)
			) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	col := column.New[int64]().LC()

	var colInsert []int64

	rows := int(^uint16(0)) + 10
	for i := range rows {
		val := int64(i + 1)
		col.Append(val)
		colInsert = append(colInsert, val)
	}

	err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				%[1]s_lc
			)
		VALUES`, tableName),
		col,
	)
	require.NoError(t, err)

	// test read row
	colRead := column.New[int64]().LC()

	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
			%[1]s_lc
		FROM test_%[1]s`, tableName),
		colRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var colData []int64

	for selectStmt.Next() {
		colData = colRead.Read(colData)
	}

	require.NoError(t, selectStmt.Err())
	assert.Equal(t, colInsert, colData)
}

// TestLowCardinalityReuseAcrossConnections guards the reuse-across-readers bug:
// the indices sub-column used to be cached by index width, keeping the reader it
// was built with. A decode column reused across Selects on different pooled
// connections then read its indices from a stale reader and returned keys
// pointing past the freshly rebuilt dictionary -- panicking in Row with an
// out-of-range index. ReadRaw now rebuilds the indices column with the current
// reader every block.
func TestLowCardinalityReuseAcrossConnections(t *testing.T) {
	tableName := "lc_reuse_across_conns"

	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn1, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	conn2, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	err = conn1.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_%s`, tableName))
	require.NoError(t, err)
	err = conn1.Exec(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
			val LowCardinality(String)
		) Engine=Memory`, tableName))
	require.NoError(t, err)

	col := column.NewString().LC()
	want := []string{"a", "a", "a", "b", "b", "c"}
	for _, v := range want {
		col.Append(v)
	}
	err = conn1.Insert(context.Background(),
		fmt.Sprintf(`INSERT INTO test_%[1]s (val) VALUES`, tableName), col)
	require.NoError(t, err)

	// One decode column, reused across two Selects on two different connections,
	// exactly as a connection pool hands them out.
	colRead := column.NewString().LC()

	read := func(conn chconn.Conn) []string {
		selectStmt, errSelect := conn.Select(context.Background(),
			fmt.Sprintf(`SELECT val FROM test_%[1]s ORDER BY val`, tableName), colRead)
		require.NoError(t, errSelect)
		var got []string
		for selectStmt.Next() {
			got = colRead.Read(got)
		}
		require.NoError(t, selectStmt.Err())
		return got
	}

	first := read(conn1)
	second := read(conn2)

	assert.Equal(t, want, first)
	assert.Equal(t, first, second, "a reused column must decode identically on a second connection")
}
