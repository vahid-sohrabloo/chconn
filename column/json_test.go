package column_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/testdata/githubmodel"
)

func TestJSON(t *testing.T) {
	tableName := "json"

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
			Name:  "allow_experimental_object_type",
			Value: "true",
		},
	}
	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
		%[1]s Object('JSON')
			) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	colJSON := column.NewJSONString()

	gg := githubmodel.GithubEvent{
		Title: "test",
		Type:  "test",
	}

	bb, _ := json.Marshal(gg)
	colJSON.AppendBytes(bb)
	err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				%[1]s
			)
		VALUES`, tableName),
		colJSON,
	)
	require.NoError(t, err)

	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
	%[1]s
	FROM test_%[1]s`, tableName))

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	for selectStmt.Next() {
		fmt.Println("aa")

	}
}
