package column_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v2"
	"github.com/vahid-sohrabloo/chconn/v2/column"
	"github.com/vahid-sohrabloo/chconn/v2/types"
)

func TestNestedNoFlattened(t *testing.T) {
	tableName := "nested_no_flattened"

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
		{
			Name:  "flatten_nested",
			Value: "false",
		},
	}

	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
			col1 Nested(col1_n1 Int64, col2_n1 String),
			col2 Nested(col1_n2 Int64, col2_n2 Nested(col1_n2_n1 Int64, col2_n2_n2 String))
		) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)
	type Col1Type types.Tuple2[int64, string]
	col1 := column.NewNested2[Col1Type, int64, string](column.New[int64](), column.NewString())

	type Col2Type types.Tuple2[int64, []Col1Type]

	col2N2 := column.NewNested2[Col1Type, int64, string](column.New[int64](), column.NewString())
	col2 := column.NewNested2[Col2Type, int64, []Col1Type](column.New[int64](), col2N2)

	// var colStringInsert []string

	var col1Insert [][]Col1Type
	var col2Insert [][]Col2Type

	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		for i := 0; i < rows; i++ {
			valString := fmt.Sprintf("string %d", i)
			valInt := int64(i)
			val2String := fmt.Sprintf("string %d", i+1)
			val2Int := int64(i + 1)
			col1.Append([]Col1Type{
				{
					Col1: valInt,
					Col2: valString,
				},
			},
			)
			col1Insert = append(col1Insert, []Col1Type{
				{
					Col1: valInt,
					Col2: valString,
				},
			})

			col2.Append([]Col2Type{
				{
					Col1: valInt,
					Col2: []Col1Type{
						{
							Col1: val2Int,
							Col2: val2String,
						},
					},
				},
			})
			col2Insert = append(col2Insert, []Col2Type{
				{
					Col1: valInt,
					Col2: []Col1Type{
						{
							Col1: val2Int,
							Col2: val2String,
						},
					},
				},
			})
		}

		err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				col1,
				col2
			)
		VALUES`, tableName),
			col1,
			col2,
		)
		require.NoError(t, err)
	}

	// example read all

	col1Read := column.NewTuple2[Col1Type, int64, string](column.New[int64](), column.NewString()).Array()

	col2N2Read := column.NewNested2[Col1Type, int64, string](column.New[int64](), column.NewString())
	col2Read := column.NewNested2[Col2Type, int64, []Col1Type](column.New[int64](), col2N2Read)
	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
	col1,col2
	FROM test_%[1]s`, tableName),
		col1Read,
		col2Read)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())
	var col1Data [][]Col1Type
	var col2Data [][]Col2Type

	for selectStmt.Next() {
		col1Data = col1Read.Read(col1Data)
		col2Data = col2Read.Read(col2Data)
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, col1Insert, col1Data)
	assert.Equal(t, col2Insert, col2Data)

	// // check dynamic column
	selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
	col1, col2
	FROM test_%[1]s`, tableName))

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()

	assert.Len(t, autoColumns, 2)

	assert.Equal(t, column.NewTuple(column.New[int64](), column.NewString()).Array().ColumnType(), autoColumns[0].ColumnType())
	assert.Equal(t,
		column.NewTuple(column.New[int64](),
			column.NewTuple(column.New[int64](), column.NewString()).Array()).Array().
			ColumnType(), autoColumns[1].ColumnType())

	for selectStmt.Next() {
	}
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()
}
