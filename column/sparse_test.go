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

func TestSparseColumn(
	t *testing.T,
) {
	tableName := "sparse"
	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	err = conn.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_%s`, tableName),
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:      "allow_suspicious_low_cardinality_types",
			Value:     "true",
			Important: true,
		},
	}

	sqlCreate := fmt.Sprintf(`CREATE TABLE test_%[1]s (
			int32 Int32,
			string String,
		)  ENGINE = MergeTree 
        ORDER BY tuple()
        SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1`, tableName)

	err = conn.ExecWithOption(context.Background(), sqlCreate, &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)
	col := column.New[int32]()
	colString := column.NewString()
	var colInsert []int32
	var colStringInsert []string

	// SetWriteBufferSize is not necessary. this just to show how to set the write buffer
	col.SetWriteBufferSize(10)
	colString.SetWriteBufferSize(10)

	rows := 10
	for numberInsert := 0; numberInsert < 2; numberInsert++ {
		for i := 0; i < rows; i++ {
			val := int32(0)
			valString := ""
			if i%3 == 0 {
				val = int32(1)
				valString = "a"
			}
			col.Append(val)
			colInsert = append(colInsert, val)
			colString.Append(valString)
			colStringInsert = append(colStringInsert, valString)
		}

		err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				int32,
				string
			)
		VALUES`, tableName),
			col,
			colString,
		)

		require.NoError(t, err)
	}

	// test read all
	colRead := column.New[int32]()
	colStringRead := column.NewString()

	selectQuery := fmt.Sprintf(`SELECT
		int32,
		string
	FROM test_%[1]s`, tableName)
	for numRead := 0; numRead < 2; numRead++ {
		selectStmt, err := conn.Select(context.Background(), selectQuery,
			colRead,
			colStringRead,
		)

		require.NoError(t, err)
		require.True(t, conn.IsBusy())

		var colData []int32
		var colStringData []string

		for selectStmt.Next() {
			colData = colRead.Read(colData)
			colStringData = colStringRead.Read(colStringData)
		}

		require.NoError(t, selectStmt.Err())

		assert.Equal(t, colInsert, colData)
		assert.Equal(t, colStringInsert, colStringData)
	}
}
