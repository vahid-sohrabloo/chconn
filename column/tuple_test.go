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

func TestTuple(t *testing.T) {
	tableName := "tuple"

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
		%[1]s Tuple(String, Int64),
		%[1]s_nullable Tuple(Nullable(String), Nullable(Int64)),
		%[1]s_array Tuple(Array(String),Array(Int64)),
		%[1]s_array_nullable Tuple(Array(Nullable(String)),Array(Nullable(Int64))),
		%[1]s_lc Tuple(LowCardinality(String),LowCardinality(Int64)),
		%[1]s_nullable_lc Tuple(LowCardinality(Nullable(String)),LowCardinality(Nullable(Int64))),
		%[1]s_array_lc Tuple(Array(LowCardinality(String)),Array(LowCardinality(Int64))),
		%[1]s_array_lc_nullable Tuple(Array(LowCardinality(Nullable(String))),Array(LowCardinality(Nullable(Int64))))
			) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	colString := column.NewString[string]()
	colInt := column.New[int64]()
	col := column.NewTuple(colString, colInt)

	colNullableString := column.NewString[string]().Nullable()
	colNullableInt := column.New[int64]().Nullable()
	colNullable := column.NewTuple(colNullableString, colNullableInt)

	colArrayString := column.NewString[string]().Array()
	colArrayInt := column.New[int64]().Array()
	colArray := column.NewTuple(colArrayString, colArrayInt)

	colNullableArrayString := column.NewString[string]().Nullable().Array()
	colNullableArrayInt := column.New[int64]().Nullable().Array()
	colNullableArray := column.NewTuple(colNullableArrayString, colNullableArrayInt)

	colLCString := column.NewString[string]().LowCardinality()
	colLCInt := column.New[int64]().LowCardinality()
	colLC := column.NewTuple(colLCString, colLCInt)

	colLCNullableString := column.NewString[string]().Nullable().LowCardinality()
	colLCNullableInt := column.New[int64]().Nullable().LowCardinality()
	colLCNullable := column.NewTuple(colLCNullableString, colLCNullableInt)

	colArrayLCString := column.NewString[string]().LowCardinality().Array()
	colArrayLCInt := column.New[int64]().LowCardinality().Array()
	colArrayLC := column.NewTuple(colArrayLCString, colArrayLCInt)

	colArrayLCNullableString := column.NewString[string]().Nullable().LowCardinality().Array()
	colArrayLCNullableInt := column.New[int64]().Nullable().LowCardinality().Array()
	colArrayLCNullable := column.NewTuple(colArrayLCNullableString, colArrayLCNullableInt)

	var colStringInsert []string
	var colIntInsert []int64
	var colNullableStringInsert []*string
	var colNullableIntInsert []*int64
	var colArrayStringInsert [][]string
	var colArrayIntInsert [][]int64
	var colArrayNullableStringInsert [][]*string
	var colArrayNullableIntInsert [][]*int64
	var colLCStringInsert []string
	var colLCIntInsert []int64
	var colLCNullableStringInsert []*string
	var colLCNullableIntInsert []*int64
	var colLCArrayStringInsert [][]string
	var colLCArrayIntInsert [][]int64
	var colLCNullableArrayStringInsert [][]*string
	var colLCNullableArrayIntInsert [][]*int64

	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		for i := 0; i < rows; i++ {
			valString := fmt.Sprintf("string %d", i)
			valInt := int64(i)
			val2String := fmt.Sprintf("string %d", i+1)
			val2Int := int64(i + 1)
			valArrayString := []string{valString, val2String}
			valArrayInt := []int64{valInt, val2Int}
			valArrayNilString := []*string{&valString, nil}
			valArrayNilInt := []*int64{&valInt, nil}

			colStringInsert = append(colStringInsert, valString)
			colIntInsert = append(colIntInsert, valInt)

			colString.Append(valString)
			colInt.Append(valInt)

			// example add nullable
			if i%2 == 0 {
				colNullableStringInsert = append(colNullableStringInsert, &valString)
				colNullableIntInsert = append(colNullableIntInsert, &valInt)
				colNullableString.Append(valString)
				colNullableInt.Append(valInt)
				colLCNullableStringInsert = append(colLCNullableStringInsert, &valString)
				colLCNullableIntInsert = append(colLCNullableIntInsert, &valInt)
				colLCNullableString.Append(valString)
				colLCNullableInt.Append(valInt)
			} else {
				colNullableStringInsert = append(colNullableStringInsert, nil)
				colNullableIntInsert = append(colNullableIntInsert, nil)
				colNullableString.AppendNil()
				colNullableInt.AppendNil()
				colLCNullableStringInsert = append(colLCNullableStringInsert, nil)
				colLCNullableIntInsert = append(colLCNullableIntInsert, nil)
				colLCNullableString.AppendNil()
				colLCNullableInt.AppendNil()
			}

			colArrayString.Append(valArrayString)
			colArrayInt.Append(valArrayInt)
			colArrayStringInsert = append(colArrayStringInsert, valArrayString)
			colArrayIntInsert = append(colArrayIntInsert, valArrayInt)

			colNullableArrayString.AppendP(valArrayNilString)
			colNullableArrayInt.AppendP(valArrayNilInt)
			colArrayNullableStringInsert = append(colArrayNullableStringInsert, valArrayNilString)
			colArrayNullableIntInsert = append(colArrayNullableIntInsert, valArrayNilInt)

			colLCStringInsert = append(colLCStringInsert, valString)
			colLCIntInsert = append(colLCIntInsert, valInt)
			colLCString.Append(valString)
			colLCInt.Append(valInt)

			colLCArrayStringInsert = append(colLCArrayStringInsert, valArrayString)
			colLCArrayIntInsert = append(colLCArrayIntInsert, valArrayInt)
			colArrayLCString.Append(valArrayString)
			colArrayLCInt.Append(valArrayInt)

			colLCNullableArrayStringInsert = append(colLCNullableArrayStringInsert, valArrayNilString)
			colLCNullableArrayIntInsert = append(colLCNullableArrayIntInsert, valArrayNilInt)
			colArrayLCNullableString.AppendP(valArrayNilString)
			colArrayLCNullableInt.AppendP(valArrayNilInt)
		}

		err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable,
				%[1]s_lc,
				%[1]s_nullable_lc,
				%[1]s_array_lc,
				%[1]s_array_lc_nullable
			)
		VALUES`, tableName),
			col,
			colNullable,
			colArray,
			colNullableArray,
			colLC,
			colLCNullable,
			colArrayLC,
			colArrayLCNullable,
		)
		require.NoError(t, err)
	}

	// example read all

	colStringRead := column.NewString[string]()
	colIntRead := column.New[int64]()
	colRead := column.NewTuple(colStringRead, colIntRead)

	colNullableStringRead := column.NewString[string]().Nullable()
	colNullableIntRead := column.New[int64]().Nullable()
	colNullableRead := column.NewTuple(colNullableStringRead, colNullableIntRead)

	colArrayStringRead := column.NewString[string]().Array()
	colArrayIntRead := column.New[int64]().Array()
	colArrayRead := column.NewTuple(colArrayStringRead, colArrayIntRead)

	colNullableArrayStringRead := column.NewString[string]().Nullable().Array()
	colNullableArrayIntRead := column.New[int64]().Nullable().Array()
	colNullableArrayRead := column.NewTuple(colNullableArrayStringRead, colNullableArrayIntRead)

	colLCStringRead := column.NewString[string]().LowCardinality()
	colLCIntRead := column.New[int64]().LowCardinality()
	colLCRead := column.NewTuple(colLCStringRead, colLCIntRead)

	colLCNullableStringRead := column.NewString[string]().Nullable().LowCardinality()
	colLCNullableIntRead := column.New[int64]().Nullable().LowCardinality()
	colLCNullableRead := column.NewTuple(colLCNullableStringRead, colLCNullableIntRead)

	colArrayLCStringRead := column.NewString[string]().LowCardinality().Array()
	colArrayLCIntRead := column.New[int64]().LowCardinality().Array()
	colArrayLCRead := column.NewTuple(colArrayLCStringRead, colArrayLCIntRead)

	colArrayLCNullableStringRead := column.NewString[string]().Nullable().LowCardinality().Array()
	colArrayLCNullableIntRead := column.New[int64]().Nullable().LowCardinality().Array()
	colArrayLCNullableRead := column.NewTuple(colArrayLCNullableStringRead, colArrayLCNullableIntRead)
	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
	%[1]s,
	%[1]s_nullable,
	%[1]s_array,
	%[1]s_array_nullable,
	%[1]s_lc,
	%[1]s_nullable_lc,
	%[1]s_array_lc,
	%[1]s_array_lc_nullable
	FROM test_%[1]s`, tableName),
		colRead,
		colNullableRead,
		colArrayRead,
		colNullableArrayRead,
		colLCRead,
		colLCNullableRead,
		colArrayLCRead,
		colArrayLCNullableRead)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var colStringData []string
	var colIntData []int64
	var colNullableStringData []*string
	var colNullableIntData []*int64
	var colArrayStringData [][]string
	var colArrayIntData [][]int64
	var colArrayNullableStringData [][]*string
	var colArrayNullableIntData [][]*int64
	var colLCStringData []string
	var colLCIntData []int64
	var colLCNullableStringData []*string
	var colLCNullableIntData []*int64
	var colLCArrayStringData [][]string
	var colLCArrayIntData [][]int64
	var colLCNullableArrayStringData [][]*string
	var colLCNullableArrayIntData [][]*int64

	for selectStmt.Next() {
		colStringData = colStringRead.Read(colStringData)
		colNullableStringData = colNullableStringRead.ReadP(colNullableStringData)
		colArrayStringData = colArrayStringRead.Read(colArrayStringData)
		colArrayNullableStringData = colNullableArrayStringRead.ReadP(colArrayNullableStringData)
		colLCStringData = colLCStringRead.Read(colLCStringData)
		colLCNullableStringData = colLCNullableStringRead.ReadP(colLCNullableStringData)
		colLCArrayStringData = colArrayLCStringRead.Read(colLCArrayStringData)
		colLCNullableArrayStringData = colArrayLCNullableStringRead.ReadP(colLCNullableArrayStringData)

		colIntData = colIntRead.Read(colIntData)
		colNullableIntData = colNullableIntRead.ReadP(colNullableIntData)
		colArrayIntData = colArrayIntRead.Read(colArrayIntData)
		colArrayNullableIntData = colNullableArrayIntRead.ReadP(colArrayNullableIntData)
		colLCIntData = colLCIntRead.Read(colLCIntData)
		colLCNullableIntData = colLCNullableIntRead.ReadP(colLCNullableIntData)
		colLCArrayIntData = colArrayLCIntRead.Read(colLCArrayIntData)
		colLCNullableArrayIntData = colArrayLCNullableIntRead.ReadP(colLCNullableArrayIntData)
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colStringInsert, colStringData)
	assert.Equal(t, colIntInsert, colIntData)
	assert.Equal(t, colNullableStringInsert, colNullableStringData)
	assert.Equal(t, colNullableIntInsert, colNullableIntData)
	assert.Equal(t, colArrayStringInsert, colArrayStringData)
	assert.Equal(t, colArrayIntInsert, colArrayIntData)
	assert.Equal(t, colArrayNullableStringInsert, colArrayNullableStringData)
	assert.Equal(t, colArrayNullableIntInsert, colArrayNullableIntData)
	assert.Equal(t, colLCStringInsert, colLCStringData)
	assert.Equal(t, colLCIntInsert, colLCIntData)
	assert.Equal(t, colLCNullableStringInsert, colLCNullableStringData)
	assert.Equal(t, colLCNullableIntInsert, colLCNullableIntData)
	assert.Equal(t, colLCArrayStringInsert, colLCArrayStringData)
	assert.Equal(t, colLCArrayIntInsert, colLCArrayIntData)
	assert.Equal(t, colLCNullableArrayStringInsert, colLCNullableArrayStringData)
	assert.Equal(t, colLCNullableArrayIntInsert, colLCNullableArrayIntData)

	// check dynamic column
	selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
		%[1]s,
		%[1]s_nullable,
		%[1]s_array,
		%[1]s_array_nullable,
		%[1]s_lc,
		%[1]s_nullable_lc,
		%[1]s_array_lc,
		%[1]s_array_lc_nullable
		FROM test_%[1]s`, tableName),
	)

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()

	assert.Len(t, autoColumns, 8)

	assert.IsType(t, colRead, autoColumns[0])
	assert.IsType(t, colRead.Column()[0], autoColumns[0].(*column.Tuple).Column()[0])
	assert.IsType(t, colRead.Column()[1], autoColumns[0].(*column.Tuple).Column()[1])
	assert.IsType(t, colNullableRead, autoColumns[1])
	assert.IsType(t, colNullableRead.Column()[0], autoColumns[1].(*column.Tuple).Column()[0])
	assert.IsType(t, colNullableRead.Column()[1], autoColumns[1].(*column.Tuple).Column()[1])
	assert.IsType(t, colArrayRead, autoColumns[2])
	assert.IsType(t, colArrayRead.Column()[0], autoColumns[2].(*column.Tuple).Column()[0])
	assert.IsType(t, colArrayRead.Column()[1], autoColumns[2].(*column.Tuple).Column()[1])
	assert.IsType(t, colNullableArrayRead, autoColumns[3])
	assert.IsType(t, colNullableArrayRead.Column()[0], autoColumns[3].(*column.Tuple).Column()[0])
	assert.IsType(t, colNullableArrayRead.Column()[1], autoColumns[3].(*column.Tuple).Column()[1])
	assert.IsType(t, colLCRead, autoColumns[4])
	assert.IsType(t, colLCRead.Column()[0], autoColumns[4].(*column.Tuple).Column()[0])
	assert.IsType(t, colLCRead.Column()[1], autoColumns[4].(*column.Tuple).Column()[1])
	assert.IsType(t, colLCNullableRead, autoColumns[5])
	assert.IsType(t, colLCNullableRead.Column()[0], autoColumns[5].(*column.Tuple).Column()[0])
	assert.IsType(t, colLCNullableRead.Column()[1], autoColumns[5].(*column.Tuple).Column()[1])
	assert.IsType(t, colArrayLCRead, autoColumns[6])
	assert.IsType(t, colArrayLCRead.Column()[0], autoColumns[6].(*column.Tuple).Column()[0])
	assert.IsType(t, colArrayLCRead.Column()[1], autoColumns[6].(*column.Tuple).Column()[1])
	assert.IsType(t, colArrayLCNullableRead, autoColumns[7])
	assert.IsType(t, colArrayLCNullableRead.Column()[0], autoColumns[7].(*column.Tuple).Column()[0])
	assert.IsType(t, colArrayLCNullableRead.Column()[1], autoColumns[7].(*column.Tuple).Column()[1])

	for selectStmt.Next() {
	}
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()
}

func TestTupleNoColumn(t *testing.T) {
	assert.Panics(t, func() { column.NewTuple() })
}

func TestGeo(t *testing.T) {
	tableName := "geo"

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
			Value: "1",
		},
		{
			Name:  "allow_experimental_geo_types",
			Value: "1",
		},
	}
	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
		point Point ,
		ring Ring ,
		polygon Polygon ,
		multiPolygon MultiPolygon
		) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	colPoint := column.NewTupleOf[types.Point]()
	colRing := column.NewTupleOf[types.Point]().Array()
	colPolygon := column.NewTupleOf[types.Point]().Array().Array()
	colMultiPolygon := column.NewTupleOf[types.Point]().Array().Array().Array()

	colPoint.SetWriteBuffer(20)
	colRing.SetWriteBuffer(20)
	colPolygon.SetWriteBuffer(20)
	colMultiPolygon.SetWriteBuffer(20)

	var pointInsert []types.Point
	var ringInsert [][]types.Point
	var polygonInsert [][][]types.Point
	var multiPolygonInsert [][][][]types.Point

	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		for i := 0; i < rows; i++ {
			pointValue := types.Point{
				X: float64(i),
				Y: float64(i + 1),
			}
			ringValue := []types.Point{
				{
					X: float64(i),
					Y: float64(i + 1),
				},
				{
					X: float64(i + 2),
					Y: float64(i + 3),
				},
			}
			polygonValue := [][]types.Point{
				{
					{
						X: float64(i),
						Y: float64(i + 1),
					},
					{
						X: float64(i + 2),
						Y: float64(i + 3),
					},
				},
				{
					{
						X: float64(i),
						Y: float64(i + 1),
					},
					{
						X: float64(i + 2),
						Y: float64(i + 3),
					},
				},
			}
			multiPolygonValue := [][][]types.Point{
				{
					{
						{
							X: float64(i),
							Y: float64(i + 1),
						},
						{
							X: float64(i + 2),
							Y: float64(i + 3),
						},
					},
					{
						{
							X: float64(i),
							Y: float64(i + 1),
						},
						{
							X: float64(i + 2),
							Y: float64(i + 3),
						},
					},
				},
			}
			colPoint.Append(pointValue)
			pointInsert = append(pointInsert, pointValue)
			colRing.Append(ringValue)
			ringInsert = append(ringInsert, ringValue)
			colPolygon.Append(polygonValue)
			polygonInsert = append(polygonInsert, polygonValue)
			colMultiPolygon.Append(multiPolygonValue)
			multiPolygonInsert = append(multiPolygonInsert, multiPolygonValue)
		}

		err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				point,
				ring,
				polygon,
				multiPolygon
			)
		VALUES`, tableName),
			colPoint,
			colRing,
			colPolygon,
			colMultiPolygon,
		)
		require.NoError(t, err)
	}

	// example read all

	colPointRead := column.NewTupleOf[types.Point]()
	colRingRead := column.NewTupleOf[types.Point]().Array()
	colPolygonRead := column.NewTupleOf[types.Point]().Array().Array()
	colMultiPolygonRead := column.NewTupleOf[types.Point]().Array().Array().Array()

	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
	point,
	ring,
	polygon,
	multiPolygon
	FROM test_%[1]s`, tableName),
		colPointRead,
		colRingRead,
		colPolygonRead,
		colMultiPolygonRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var pointData []types.Point
	var ringData [][]types.Point
	var polygonData [][][]types.Point
	var multiPolygonData [][][][]types.Point

	for selectStmt.Next() {
		pointData = colPointRead.Read(pointData)
		ringData = colRingRead.Read(ringData)
		polygonData = colPolygonRead.Read(polygonData)
		multiPolygonData = colMultiPolygonRead.Read(multiPolygonData)
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, pointInsert, pointData)
	assert.Equal(t, ringInsert, ringData)
	assert.Equal(t, polygonInsert, polygonData)
	assert.Equal(t, multiPolygonInsert, multiPolygonData)
}
