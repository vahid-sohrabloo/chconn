package column_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v2"
	"github.com/vahid-sohrabloo/chconn/v2/column"
	"github.com/vahid-sohrabloo/chconn/v2/testdata/githubmodel"
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

	// colJSON.AppendString(`[{"date f":"2009-06-16T09:55:57.320","user":"Elton John (22595)"},{"date f":"2009-06-17T12:34:22.643","user":"Jack Black (77153)"}]`)
	// colJSON.AppendString("{\"Int64`,\":\"sss\",\"dd\":\"bbb\",\"ff\":\"aaa\"}")
	// colJSON.AppendString(`{"answers2":[{"date":"2009-06-16T09:55:57.320","user":"Elton John (22595)"},{"date":"2009-06-17T12:34:22.643","user":"Jack Black (77153)"}],"creationDate":"2009-06-16T07:28:42.770","qid":"1000000","tag":["vb6","progress-bar"],"title":"Display Progress Bar at the Time of Processing","user":"Jash"}`)
	gg := githubmodel.GithubEvent{
		Title: "test",
		Type:  "test",
	}
	// colJSON2 := githubmodel.NewGithubEventJSONColumn()
	// colJSON2.Append(gg)
	bb, _ := json.Marshal(gg)
	// colJSON.AppendString(`{"a": 1, "b": { "c": 2, "d": [1, 2, 3] }}`)
	colJSON.AppendBytes(bb)
	err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				%[1]s
			)
		VALUES`, tableName),
		colJSON,
		// colJSON2.Tuple,
	)
	require.NoError(t, err)

	// colJSON.AppendString(`{"a": 10245, "b": { "c": 2, "d": [1, 2, 3] }}`)

	// err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
	// 		test_%[1]s (
	// 			%[1]s
	// 		)
	// 	VALUES`, tableName),
	// 	colJSON,
	// )
	// require.NoError(t, err)

	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
	%[1]s
	FROM test_%[1]s`, tableName))

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	for selectStmt.Next() {
		fmt.Println("aa")

	}

	// for insertN := 0; insertN < 2; insertN++ {
	// 	rows := 10
	// 	for i := 0; i < rows; i++ {
	// 		valString := fmt.Sprintf("string %d", i)
	// 		valInt := int64(i)
	// 		val2String := fmt.Sprintf("string %d", i+1)
	// 		val2Int := int64(i + 1)
	// 		valArrayString := []string{valString, val2String}
	// 		valArrayInt := []int64{valInt, val2Int}
	// 		valArrayNilString := []*string{&valString, nil}
	// 		valArrayNilInt := []*int64{&valInt, nil}

	// 		colStringInsert = append(colStringInsert, valString)
	// 		colIntInsert = append(colIntInsert, valInt)

	// 		colString.Append(valString)
	// 		colInt.Append(valInt)

	// 		// example add nullable
	// 		if i%2 == 0 {
	// 			colNullableStringInsert = append(colNullableStringInsert, &valString)
	// 			colNullableIntInsert = append(colNullableIntInsert, &valInt)
	// 			colNullableString.Append(valString)
	// 			colNullableInt.Append(valInt)
	// 			colLCNullableStringInsert = append(colLCNullableStringInsert, &valString)
	// 			colLCNullableIntInsert = append(colLCNullableIntInsert, &valInt)
	// 			colLCNullableString.Append(valString)
	// 			colLCNullableInt.Append(valInt)
	// 		} else {
	// 			colNullableStringInsert = append(colNullableStringInsert, nil)
	// 			colNullableIntInsert = append(colNullableIntInsert, nil)
	// 			colNullableString.AppendNil()
	// 			colNullableInt.AppendNil()
	// 			colLCNullableStringInsert = append(colLCNullableStringInsert, nil)
	// 			colLCNullableIntInsert = append(colLCNullableIntInsert, nil)
	// 			colLCNullableString.AppendNil()
	// 			colLCNullableInt.AppendNil()
	// 		}

	// 		colArrayString.Append(valArrayString)
	// 		colArrayInt.Append(valArrayInt)
	// 		colArrayStringInsert = append(colArrayStringInsert, valArrayString)
	// 		colArrayIntInsert = append(colArrayIntInsert, valArrayInt)

	// 		colNullableArrayString.AppendP(valArrayNilString)
	// 		colNullableArrayInt.AppendP(valArrayNilInt)
	// 		colArrayNullableStringInsert = append(colArrayNullableStringInsert, valArrayNilString)
	// 		colArrayNullableIntInsert = append(colArrayNullableIntInsert, valArrayNilInt)

	// 		colLCStringInsert = append(colLCStringInsert, valString)
	// 		colLCIntInsert = append(colLCIntInsert, valInt)
	// 		colLCString.Append(valString)
	// 		colLCInt.Append(valInt)

	// 		colLCArrayStringInsert = append(colLCArrayStringInsert, valArrayString)
	// 		colLCArrayIntInsert = append(colLCArrayIntInsert, valArrayInt)
	// 		colArrayLCString.Append(valArrayString)
	// 		colArrayLCInt.Append(valArrayInt)

	// 		colLCNullableArrayStringInsert = append(colLCNullableArrayStringInsert, valArrayNilString)
	// 		colLCNullableArrayIntInsert = append(colLCNullableArrayIntInsert, valArrayNilInt)
	// 		colArrayLCNullableString.AppendP(valArrayNilString)
	// 		colArrayLCNullableInt.AppendP(valArrayNilInt)
	// 	}

	// 	err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
	// 		test_%[1]s (
	// 			%[1]s,
	// 			%[1]s_nullable,
	// 			%[1]s_array,
	// 			%[1]s_array_nullable,
	// 			%[1]s_lc,
	// 			%[1]s_nullable_lc,
	// 			%[1]s_array_lc,
	// 			%[1]s_array_lc_nullable
	// 		)
	// 	VALUES`, tableName),
	// 		col,
	// 		colNullable,
	// 		colArray,
	// 		colNullableArray,
	// 		colLC,
	// 		colLCNullable,
	// 		colArrayLC,
	// 		colArrayLCNullable,
	// 	)
	// 	require.NoError(t, err)
	// }

	// // example read all

	// colStringRead := column.NewString()
	// colIntRead := column.New[int64]()
	// colRead := column.NewTuple(colStringRead, colIntRead)

	// colNullableStringRead := column.NewString().Nullable()
	// colNullableIntRead := column.New[int64]().Nullable()
	// colNullableRead := column.NewTuple(colNullableStringRead, colNullableIntRead)

	// colArrayStringRead := column.NewString().Array()
	// colArrayIntRead := column.New[int64]().Array()
	// colArrayRead := column.NewTuple(colArrayStringRead, colArrayIntRead)

	// colNullableArrayStringRead := column.NewString().Nullable().Array()
	// colNullableArrayIntRead := column.New[int64]().Nullable().Array()
	// colNullableArrayRead := column.NewTuple(colNullableArrayStringRead, colNullableArrayIntRead)

	// colLCStringRead := column.NewString().LowCardinality()
	// colLCIntRead := column.New[int64]().LowCardinality()
	// colLCRead := column.NewTuple(colLCStringRead, colLCIntRead)

	// colLCNullableStringRead := column.NewString().LowCardinality().Nullable()
	// colLCNullableIntRead := column.New[int64]().LowCardinality().Nullable()
	// colLCNullableRead := column.NewTuple(colLCNullableStringRead, colLCNullableIntRead)

	// colArrayLCStringRead := column.NewString().LowCardinality().Array()
	// colArrayLCIntRead := column.New[int64]().LowCardinality().Array()
	// colArrayLCRead := column.NewTuple(colArrayLCStringRead, colArrayLCIntRead)

	// colArrayLCNullableStringRead := column.NewString().LowCardinality().Nullable().Array()
	// colArrayLCNullableIntRead := column.New[int64]().LowCardinality().Nullable().Array()
	// colArrayLCNullableRead := column.NewTuple(colArrayLCNullableStringRead, colArrayLCNullableIntRead)
	// selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
	// %[1]s,
	// %[1]s_nullable,
	// %[1]s_array,
	// %[1]s_array_nullable,
	// %[1]s_lc,
	// %[1]s_nullable_lc,
	// %[1]s_array_lc,
	// %[1]s_array_lc_nullable
	// FROM test_%[1]s`, tableName),
	// 	colRead,
	// 	colNullableRead,
	// 	colArrayRead,
	// 	colNullableArrayRead,
	// 	colLCRead,
	// 	colLCNullableRead,
	// 	colArrayLCRead,
	// 	colArrayLCNullableRead)

	// require.NoError(t, err)
	// require.True(t, conn.IsBusy())

	// var colStringData []string
	// var colIntData []int64
	// var colNullableStringData []*string
	// var colNullableIntData []*int64
	// var colArrayStringData [][]string
	// var colArrayIntData [][]int64
	// var colArrayNullableStringData [][]*string
	// var colArrayNullableIntData [][]*int64
	// var colLCStringData []string
	// var colLCIntData []int64
	// var colLCNullableStringData []*string
	// var colLCNullableIntData []*int64
	// var colLCArrayStringData [][]string
	// var colLCArrayIntData [][]int64
	// var colLCNullableArrayStringData [][]*string
	// var colLCNullableArrayIntData [][]*int64

	// for selectStmt.Next() {
	// 	colStringData = colStringRead.Read(colStringData)
	// 	colNullableStringData = colNullableStringRead.ReadP(colNullableStringData)
	// 	colArrayStringData = colArrayStringRead.Read(colArrayStringData)
	// 	colArrayNullableStringData = colNullableArrayStringRead.ReadP(colArrayNullableStringData)
	// 	colLCStringData = colLCStringRead.Read(colLCStringData)
	// 	colLCNullableStringData = colLCNullableStringRead.ReadP(colLCNullableStringData)
	// 	colLCArrayStringData = colArrayLCStringRead.Read(colLCArrayStringData)
	// 	colLCNullableArrayStringData = colArrayLCNullableStringRead.ReadP(colLCNullableArrayStringData)

	// 	colIntData = colIntRead.Read(colIntData)
	// 	colNullableIntData = colNullableIntRead.ReadP(colNullableIntData)
	// 	colArrayIntData = colArrayIntRead.Read(colArrayIntData)
	// 	colArrayNullableIntData = colNullableArrayIntRead.ReadP(colArrayNullableIntData)
	// 	colLCIntData = colLCIntRead.Read(colLCIntData)
	// 	colLCNullableIntData = colLCNullableIntRead.ReadP(colLCNullableIntData)
	// 	colLCArrayIntData = colArrayLCIntRead.Read(colLCArrayIntData)
	// 	colLCNullableArrayIntData = colArrayLCNullableIntRead.ReadP(colLCNullableArrayIntData)
	// }

	// require.NoError(t, selectStmt.Err())

	// assert.Equal(t, colStringInsert, colStringData)
	// assert.Equal(t, colIntInsert, colIntData)
	// assert.Equal(t, colNullableStringInsert, colNullableStringData)
	// assert.Equal(t, colNullableIntInsert, colNullableIntData)
	// assert.Equal(t, colArrayStringInsert, colArrayStringData)
	// assert.Equal(t, colArrayIntInsert, colArrayIntData)
	// assert.Equal(t, colArrayNullableStringInsert, colArrayNullableStringData)
	// assert.Equal(t, colArrayNullableIntInsert, colArrayNullableIntData)
	// assert.Equal(t, colLCStringInsert, colLCStringData)
	// assert.Equal(t, colLCIntInsert, colLCIntData)
	// assert.Equal(t, colLCNullableStringInsert, colLCNullableStringData)
	// assert.Equal(t, colLCNullableIntInsert, colLCNullableIntData)
	// assert.Equal(t, colLCArrayStringInsert, colLCArrayStringData)
	// assert.Equal(t, colLCArrayIntInsert, colLCArrayIntData)
	// assert.Equal(t, colLCNullableArrayStringInsert, colLCNullableArrayStringData)
	// assert.Equal(t, colLCNullableArrayIntInsert, colLCNullableArrayIntData)

	// // check dynamic column
	// selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
	// 	%[1]s,
	// 	%[1]s_nullable,
	// 	%[1]s_array,
	// 	%[1]s_array_nullable,
	// 	%[1]s_lc,
	// 	%[1]s_nullable_lc,
	// 	%[1]s_array_lc,
	// 	%[1]s_array_lc_nullable
	// 	FROM test_%[1]s`, tableName),
	// )

	// require.NoError(t, err)
	// autoColumns := selectStmt.Columns()

	// assert.Len(t, autoColumns, 8)

	// assert.IsType(t, colRead, autoColumns[0])
	// assert.IsType(t, colRead.Column()[0], autoColumns[0].(*column.Tuple).Column()[0])
	// assert.IsType(t, colRead.Column()[1], autoColumns[0].(*column.Tuple).Column()[1])
	// assert.IsType(t, colNullableRead, autoColumns[1])
	// assert.IsType(t, colNullableRead.Column()[0], autoColumns[1].(*column.Tuple).Column()[0])
	// assert.IsType(t, colNullableRead.Column()[1], autoColumns[1].(*column.Tuple).Column()[1])
	// assert.IsType(t, colArrayRead, autoColumns[2])
	// assert.IsType(t, colArrayRead.Column()[0], autoColumns[2].(*column.Tuple).Column()[0])
	// assert.IsType(t, colArrayRead.Column()[1], autoColumns[2].(*column.Tuple).Column()[1])
	// assert.IsType(t, colNullableArrayRead, autoColumns[3])
	// assert.IsType(t, colNullableArrayRead.Column()[0], autoColumns[3].(*column.Tuple).Column()[0])
	// assert.IsType(t, colNullableArrayRead.Column()[1], autoColumns[3].(*column.Tuple).Column()[1])
	// assert.IsType(t, colLCRead, autoColumns[4])
	// assert.IsType(t, colLCRead.Column()[0], autoColumns[4].(*column.Tuple).Column()[0])
	// assert.IsType(t, colLCRead.Column()[1], autoColumns[4].(*column.Tuple).Column()[1])
	// assert.IsType(t, colLCNullableRead, autoColumns[5])
	// assert.IsType(t, colLCNullableRead.Column()[0], autoColumns[5].(*column.Tuple).Column()[0])
	// assert.IsType(t, colLCNullableRead.Column()[1], autoColumns[5].(*column.Tuple).Column()[1])
	// assert.IsType(t, colArrayLCRead, autoColumns[6])
	// assert.IsType(t, colArrayLCRead.Column()[0], autoColumns[6].(*column.Tuple).Column()[0])
	// assert.IsType(t, colArrayLCRead.Column()[1], autoColumns[6].(*column.Tuple).Column()[1])
	// assert.IsType(t, colArrayLCNullableRead, autoColumns[7])
	// assert.IsType(t, colArrayLCNullableRead.Column()[0], autoColumns[7].(*column.Tuple).Column()[0])
	// assert.IsType(t, colArrayLCNullableRead.Column()[1], autoColumns[7].(*column.Tuple).Column()[1])

	// for selectStmt.Next() {
	// }
	// require.NoError(t, selectStmt.Err())
	// selectStmt.Close()
}
