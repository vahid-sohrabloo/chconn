package column_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestDynamicJSONReadReinsert(t *testing.T) {
	conn, err := chconn.Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer conn.Close()
	skipIfCHBelow(t, conn.ServerInfo(), 25, 3, "Dynamic and JSON types")

	t.Run("Dynamic", func(t *testing.T) {
		col := column.NewDynamic()
		rows, err := conn.Select(context.Background(),
			"SELECT if(number = 0, 1::Dynamic, 'two'::Dynamic) AS v FROM numbers(2)", col)
		require.NoError(t, err)
		require.True(t, rows.Next())
		assert.Equal(t, 2, col.NumRow())
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		col.Delete(0, 1)
		require.NoError(t, col.AppendAny(float64(9.5)))
		require.NoError(t, conn.Exec(context.Background(), "CREATE TEMPORARY TABLE read_append_dynamic (v Dynamic)"))
		require.NoError(t, conn.Insert(context.Background(), "INSERT INTO read_append_dynamic VALUES", col))
		out := column.NewDynamic()
		rows, err = conn.Select(context.Background(), "SELECT v FROM read_append_dynamic", out)
		require.NoError(t, err)
		require.True(t, rows.Next())
		assert.Equal(t, []any{"two", float64(9.5)}, out.Data())
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})
	t.Run("JSON", func(t *testing.T) {
		col := column.NewJSON()
		rows, err := conn.Select(context.Background(),
			`SELECT concat('{"a":', toString(number + 1), '}')::JSON AS v FROM numbers(2)`, col)
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		col.Delete(0, 1)
		require.NoError(t, col.AppendAny(map[string]any{"a": int64(9)}))
		require.NoError(t, conn.Exec(context.Background(), "CREATE TEMPORARY TABLE read_append_json (v JSON)"))
		require.NoError(t, conn.Insert(context.Background(), "INSERT INTO read_append_json VALUES", col))
		out := column.NewJSON()
		rows, err = conn.Select(context.Background(), "SELECT v FROM read_append_json", out)
		require.NoError(t, err)
		require.True(t, rows.Next())
		assert.Equal(t, []any{map[string]any{"a": int64(2)}, map[string]any{"a": int64(9)}}, out.Data())
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})
}
