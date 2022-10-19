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

func TestTuples(t *testing.T) {
	tableName := "tuples"

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
		%[1]s1 Tuple(Int64),
		%[1]s1_array Array(Tuple(Int64)),
		%[1]s2 Tuple(Int64, Int64),
		%[1]s2_array Array(Tuple(Int64, Int64)),
		%[1]s3 Tuple(Int64, Int64, Int64),
		%[1]s3_array Array(Tuple(Int64, Int64, Int64)),
		%[1]s4 Tuple(Int64, Int64, Int64, Int64),
		%[1]s4_array Array(Tuple(Int64, Int64, Int64, Int64)),
		%[1]s5 Tuple(Int64, Int64, Int64, Int64, Int64),
		%[1]s5_array Array(Tuple(Int64, Int64, Int64, Int64, Int64))
		) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	col1 := column.NewTuple1[int64](column.New[int64]())
	col1Array := column.NewTuple1[int64](column.New[int64]()).Array()
	type Tuple2 types.Tuple2[int64, int64]
	col2 := column.NewTuple2[Tuple2, int64, int64](column.New[int64](), column.New[int64]())
	col2Array := column.NewTuple2[Tuple2, int64, int64](column.New[int64](), column.New[int64]()).Array()
	type Tuple3 types.Tuple3[int64, int64, int64]
	col3 := column.NewTuple3[Tuple3, int64, int64, int64](column.New[int64](), column.New[int64](), column.New[int64]())
	col3Array := column.NewTuple3[Tuple3, int64, int64, int64](column.New[int64](), column.New[int64](), column.New[int64]()).Array()
	type Tuple4 types.Tuple4[int64, int64, int64, int64]
	col4 := column.NewTuple4[
		Tuple4,
		int64,
		int64,
		int64,
		int64](
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
	)
	col4Array := column.NewTuple4[
		Tuple4,
		int64,
		int64,
		int64,
		int64,
	](
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
	).Array()
	type Tuple5 types.Tuple5[
		int64,
		int64,
		int64,
		int64,
		int64,
	]
	col5 := column.NewTuple5[
		Tuple5,
		int64,
		int64,
		int64,
		int64,
		int64,
	](
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
	)
	col5Array := column.NewTuple5[
		Tuple5,
		int64,
		int64,
		int64,
		int64,
		int64,
	](
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
	).Array()

	var col1Insert []int64
	var col1ArrayInsert [][]int64
	var col2Insert []Tuple2
	var col2ArrayInsert [][]Tuple2
	var col3Insert []Tuple3
	var col3ArrayInsert [][]Tuple3
	var col4Insert []Tuple4
	var col4ArrayInsert [][]Tuple4
	var col5Insert []Tuple5
	var col5ArrayInsert [][]Tuple5

	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		for i := 0; i < rows; i++ {
			col1.Append(int64(i))
			col1Insert = append(col1Insert, int64(i))
			col1Array.Append([]int64{int64(i), int64(i + 1)})
			col1ArrayInsert = append(col1ArrayInsert, []int64{int64(i), int64(i + 1)})
			col2.Append(Tuple2{int64(i), int64(i + 1)})
			col2Insert = append(col2Insert, Tuple2{int64(i), int64(i + 1)})
			col2Array.Append([]Tuple2{{int64(i), int64(i + 1)}, {int64(i + 2), int64(i + 3)}})
			col2ArrayInsert = append(col2ArrayInsert, []Tuple2{{int64(i), int64(i + 1)}, {int64(i + 2), int64(i + 3)}})
			col3.Append(Tuple3{int64(i), int64(i + 1), int64(i + 2)})
			col3Insert = append(col3Insert, Tuple3{int64(i), int64(i + 1), int64(i + 2)})
			col3Array.Append([]Tuple3{
				{int64(i), int64(i + 1), int64(i + 2)},
				{int64(i + 3), int64(i + 4), int64(i + 5)},
			})
			col3ArrayInsert = append(col3ArrayInsert, []Tuple3{
				{int64(i), int64(i + 1), int64(i + 2)},
				{int64(i + 3), int64(i + 4), int64(i + 5)},
			})
			col4.Append(Tuple4{int64(i), int64(i + 1), int64(i + 2), int64(i + 3)})
			col4Insert = append(col4Insert, Tuple4{int64(i), int64(i + 1), int64(i + 2), int64(i + 3)})
			col4Array.Append([]Tuple4{
				{int64(i), int64(i + 1), int64(i + 2), int64(i + 3)},
				{int64(i + 4), int64(i + 5), int64(i + 6), int64(i + 7)},
			})
			col4ArrayInsert = append(col4ArrayInsert, []Tuple4{
				{int64(i), int64(i + 1), int64(i + 2), int64(i + 3)},
				{int64(i + 4), int64(i + 5), int64(i + 6), int64(i + 7)},
			})
			col5.Append(Tuple5{int64(i), int64(i + 1), int64(i + 2), int64(i + 3), int64(i + 4)})
			col5Insert = append(col5Insert, Tuple5{int64(i), int64(i + 1), int64(i + 2), int64(i + 3), int64(i + 4)})
			col5Array.Append([]Tuple5{
				{int64(i), int64(i + 1), int64(i + 2), int64(i + 3), int64(i + 4)},
				{int64(i + 5), int64(i + 6), int64(i + 7), int64(i + 8), int64(i + 9)},
			})
			col5ArrayInsert = append(col5ArrayInsert, []Tuple5{
				{int64(i), int64(i + 1), int64(i + 2), int64(i + 3), int64(i + 4)},
				{int64(i + 5), int64(i + 6), int64(i + 7), int64(i + 8), int64(i + 9)},
			})
		}

		err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				%[1]s1,
				%[1]s1_array,
				%[1]s2,
				%[1]s2_array,
				%[1]s3,
				%[1]s3_array,
				%[1]s4,
				%[1]s4_array,
				%[1]s5,
				%[1]s5_array
			)
		VALUES`, tableName),
			col1, col1Array, col2, col2Array, col3, col3Array, col4, col4Array, col5, col5Array,
		)
		require.NoError(t, err)
	}

	// example read all
	col1Read := column.NewTuple1[int64](column.New[int64]())
	col1ArrayRead := column.NewTuple1[int64](column.New[int64]()).Array()
	col2Read := column.NewTuple2[Tuple2, int64, int64](column.New[int64](), column.New[int64]())
	col2ArrayRead := column.NewTuple2[Tuple2, int64, int64](column.New[int64](), column.New[int64]()).Array()
	col3Read := column.NewTuple3[
		Tuple3,
		int64,
		int64,
		int64,
	](
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
	)
	col3ArrayRead := column.NewTuple3[
		Tuple3,
		int64,
		int64,
		int64,
	](
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
	).Array()
	col4Read := column.NewTuple4[
		Tuple4,
		int64,
		int64,
		int64,
		int64,
	](
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
	)
	col4ArrayRead := column.NewTuple4[
		Tuple4,
		int64,
		int64,
		int64,
		int64,
	](
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
	).Array()
	col5Read := column.NewTuple5[
		Tuple5,
		int64,
		int64,
		int64,
		int64,
		int64,
	](
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
	)
	col5ArrayRead := column.NewTuple5[
		Tuple5,
		int64,
		int64,
		int64,
		int64,
		int64,
	](
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
		column.New[int64](),
	).Array()
	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
		%[1]s1,
		%[1]s1_array,
		%[1]s2,
		%[1]s2_array,
		%[1]s3,
		%[1]s3_array,
		%[1]s4,
		%[1]s4_array,
		%[1]s5,
		%[1]s5_array

	FROM test_%[1]s`, tableName),
		col1Read, col1ArrayRead, col2Read, col2ArrayRead, col3Read, col3ArrayRead, col4Read, col4ArrayRead, col5Read, col5ArrayRead)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var col1ReadData []int64
	var col1ArrayReadData [][]int64
	var col2ReadData []Tuple2
	var col2ArrayReadData [][]Tuple2
	var col3ReadData []Tuple3
	var col3ArrayReadData [][]Tuple3
	var col4ReadData []Tuple4
	var col4ArrayReadData [][]Tuple4
	var col5ReadData []Tuple5
	var col5ArrayReadData [][]Tuple5

	for selectStmt.Next() {
		col1ReadData = col1Read.Read(col1ReadData)
		col1ArrayReadData = col1ArrayRead.Read(col1ArrayReadData)
		col2ReadData = col2Read.Read(col2ReadData)
		col2ArrayReadData = col2ArrayRead.Read(col2ArrayReadData)
		col3ReadData = col3Read.Read(col3ReadData)
		col3ArrayReadData = col3ArrayRead.Read(col3ArrayReadData)
		col4ReadData = col4Read.Read(col4ReadData)
		col4ArrayReadData = col4ArrayRead.Read(col4ArrayReadData)
		col5ReadData = col5Read.Read(col5ReadData)
		col5ArrayReadData = col5ArrayRead.Read(col5ArrayReadData)
	}

	require.NoError(t, selectStmt.Err())
	selectStmt.Close()

	assert.Equal(t, col1Insert, col1ReadData)
	assert.Equal(t, col1ArrayInsert, col1ArrayReadData)
	assert.Equal(t, col2Insert, col2ReadData)
	assert.Equal(t, col2ArrayInsert, col2ArrayReadData)
	assert.Equal(t, col3Insert, col3ReadData)
	assert.Equal(t, col3ArrayInsert, col3ArrayReadData)
	assert.Equal(t, col4Insert, col4ReadData)
	assert.Equal(t, col4ArrayInsert, col4ArrayReadData)
	assert.Equal(t, col5Insert, col5ReadData)
	assert.Equal(t, col5ArrayInsert, col5ArrayReadData)
}
