package column_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/format"
	"github.com/vahid-sohrabloo/chconn/v3/types"
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
		%[1]s_array_lc_nullable Tuple(Array(LowCardinality(Nullable(String))),Array(LowCardinality(Nullable(Int64)))),
		%[1]s_array_array_tuple Array(Array(Tuple(String, Int64)))
			) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	colString := column.NewString()
	colInt := column.New[int64]()
	col := column.NewTuple(colString, colInt)

	colNullableString := column.NewString().Nullable()
	colNullableInt := column.New[int64]().Nullable()
	colNullable := column.NewTuple(colNullableString, colNullableInt)

	colArrayString := column.NewString().Array()
	colArrayInt := column.New[int64]().Array()
	colArray := column.NewTuple(colArrayString, colArrayInt)

	colNullableArrayString := column.NewString().Nullable().Array()
	colNullableArrayInt := column.New[int64]().Nullable().Array()
	colNullableArray := column.NewTuple(colNullableArrayString, colNullableArrayInt)

	colLCString := column.NewString().LowCardinality()
	colLCInt := column.New[int64]().LowCardinality()
	colLC := column.NewTuple(colLCString, colLCInt)

	colLCNullableString := column.NewString().LowCardinality().Nullable()
	colLCNullableInt := column.New[int64]().LowCardinality().Nullable()
	colLCNullable := column.NewTuple(colLCNullableString, colLCNullableInt)

	colArrayLCString := column.NewString().LowCardinality().Array()
	colArrayLCInt := column.New[int64]().LowCardinality().Array()
	colArrayLC := column.NewTuple(colArrayLCString, colArrayLCInt)

	colArrayLCNullableString := column.NewString().LowCardinality().Nullable().Array()
	colArrayLCNullableInt := column.New[int64]().LowCardinality().Nullable().Array()
	colArrayLCNullable := column.NewTuple(colArrayLCNullableString, colArrayLCNullableInt)

	colArrayArrayTupleString := column.NewString()
	colArrayArrayTupleInt := column.New[int64]()
	colArrayArrayTuple := column.NewTuple(colArrayArrayTupleString, colArrayArrayTupleInt).Array().Array()

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

			colArrayArrayTuple.AppendLen(1)
			colArrayArrayTuple.Column().(*column.ArrayBase).AppendLen(2)
			colArrayArrayTupleString.AppendMulti(valString, val2String)
			colArrayArrayTupleInt.AppendMulti(valInt, val2Int)
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
				%[1]s_array_lc_nullable,
				%[1]s_array_array_tuple
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
			colArrayArrayTuple,
		)
		require.NoError(t, err)
	}

	// example read all

	colStringRead := column.NewString()
	colIntRead := column.New[int64]()
	colRead := column.NewTuple(colStringRead, colIntRead)

	colNullableStringRead := column.NewString().Nullable()
	colNullableIntRead := column.New[int64]().Nullable()
	colNullableRead := column.NewTuple(colNullableStringRead, colNullableIntRead)

	colArrayStringRead := column.NewString().Array()
	colArrayIntRead := column.New[int64]().Array()
	colArrayRead := column.NewTuple(colArrayStringRead, colArrayIntRead)

	colNullableArrayStringRead := column.NewString().Nullable().Array()
	colNullableArrayIntRead := column.New[int64]().Nullable().Array()
	colNullableArrayRead := column.NewTuple(colNullableArrayStringRead, colNullableArrayIntRead)

	colLCStringRead := column.NewString().LowCardinality()
	colLCIntRead := column.New[int64]().LowCardinality()
	colLCRead := column.NewTuple(colLCStringRead, colLCIntRead)

	colLCNullableStringRead := column.NewString().LowCardinality().Nullable()
	colLCNullableIntRead := column.New[int64]().LowCardinality().Nullable()
	colLCNullableRead := column.NewTuple(colLCNullableStringRead, colLCNullableIntRead)

	colArrayLCStringRead := column.NewString().LowCardinality().Array()
	colArrayLCIntRead := column.New[int64]().LowCardinality().Array()
	colArrayLCRead := column.NewTuple(colArrayLCStringRead, colArrayLCIntRead)

	colArrayLCNullableStringRead := column.NewString().LowCardinality().Nullable().Array()
	colArrayLCNullableIntRead := column.New[int64]().LowCardinality().Nullable().Array()
	colArrayLCNullableRead := column.NewTuple(colArrayLCNullableStringRead, colArrayLCNullableIntRead)

	selectQuery := fmt.Sprintf(`SELECT
	%[1]s,
	%[1]s_nullable,
	%[1]s_array,
	%[1]s_array_nullable,
	%[1]s_lc,
	%[1]s_nullable_lc,
	%[1]s_array_lc,
	%[1]s_array_lc_nullable
	FROM test_%[1]s`, tableName)
	selectStmt, err := conn.Select(context.Background(), selectQuery,
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
	selectStmt, err = conn.Select(context.Background(), selectQuery)

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()

	assert.Len(t, autoColumns, 8)

	assert.Equal(t, colRead.FullType(), autoColumns[0].FullType())
	assert.Equal(t, colNullableRead.FullType(), autoColumns[1].FullType())
	assert.Equal(t, colArrayRead.FullType(), autoColumns[2].FullType())
	assert.Equal(t, colNullableArrayRead.FullType(), autoColumns[3].FullType())
	assert.Equal(t, colLCRead.FullType(), autoColumns[4].FullType())
	assert.Equal(t, colLCNullableRead.FullType(), autoColumns[5].FullType())
	assert.Equal(t, colArrayLCRead.FullType(), autoColumns[6].FullType())
	assert.Equal(t, colArrayLCNullableRead.FullType(), autoColumns[7].FullType())
	rows := selectStmt.Rows()
	var colData []any
	var colNullableData []any
	var colArrayData []any
	var colArrayNullableData []any
	var colLCData []any
	var colLCNullableData []any
	var colLCArrayData []any
	var colLCNullableArrayData []any

	for rows.Next() {
		var colVal []any
		var colNullableVal []any
		var colArrayVal []any
		var colArrayNullableVal []any
		var colLCVal []any
		var colLCNullableVal []any
		var colLCArrayVal []any
		var colLCNullableArrayVal []any
		err := rows.Scan(
			&colVal,
			&colNullableVal,
			&colArrayVal,
			&colArrayNullableVal,
			&colLCVal,
			&colLCNullableVal,
			&colLCArrayVal,
			&colLCNullableArrayVal,
		)
		require.NoError(t, err)
		colData = append(colData, colVal)
		colNullableData = append(colNullableData, colNullableVal)
		colArrayData = append(colArrayData, colArrayVal)
		colArrayNullableData = append(colArrayNullableData, colArrayNullableVal)
		colLCData = append(colLCData, colLCVal)
		colLCNullableData = append(colLCNullableData, colLCNullableVal)
		colLCArrayData = append(colLCArrayData, colLCArrayVal)
		colLCNullableArrayData = append(colLCNullableArrayData, colLCNullableArrayVal)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	var colStringDataI []any
	var colNullableStringDataI []any
	var colArrayStringDataI []any
	var colArrayNullableStringDataI []any
	var colLCStringDataI []any
	var colLCNullableStringDataI []any
	var colLCArrayStringDataI []any
	var colLCNullableArrayStringDataI []any
	for i, v := range colStringData {
		colStringDataI = append(colStringDataI, []any{
			v,
			colIntData[i],
		})
	}
	for i, v := range colNullableStringData {
		colNullableStringDataI = append(colNullableStringDataI, []any{
			v,
			colNullableIntData[i],
		})
	}
	for i, v := range colArrayStringData {
		colArrayStringDataI = append(colArrayStringDataI, []any{
			v,
			colArrayIntData[i],
		})
	}
	for i, v := range colArrayNullableStringData {
		colArrayNullableStringDataI = append(colArrayNullableStringDataI, []any{
			v,
			colArrayNullableIntData[i],
		})
	}
	for i, v := range colLCStringData {
		colLCStringDataI = append(colLCStringDataI, []any{
			v,
			colLCIntData[i],
		})
	}
	for i, v := range colLCNullableStringData {
		colLCNullableStringDataI = append(colLCNullableStringDataI, []any{
			v,
			colLCNullableIntData[i],
		})
	}
	for i, v := range colLCArrayStringData {
		colLCArrayStringDataI = append(colLCArrayStringDataI, []any{
			v,
			colLCArrayIntData[i],
		})
	}
	for i, v := range colLCNullableArrayStringData {
		colLCNullableArrayStringDataI = append(colLCNullableArrayStringDataI, []any{
			v,
			colLCNullableArrayIntData[i],
		})
	}

	assert.Equal(t, colStringDataI, colData)
	assert.Equal(t, colNullableStringDataI, colNullableData)
	assert.Equal(t, colArrayStringDataI, colArrayData)
	assert.Equal(t, colArrayNullableStringDataI, colArrayNullableData)
	assert.Equal(t, colLCStringDataI, colLCData)
	assert.Equal(t, colLCNullableStringDataI, colLCNullableData)
	assert.Equal(t, colLCArrayStringDataI, colLCArrayData)
	assert.Equal(t, colLCNullableArrayStringDataI, colLCNullableArrayData)

	var chconnJSON []string
	jsonFormat := format.NewJSON(1000, func(b []byte, cb []column.ColumnBasic) {
		chconnJSON = append(chconnJSON, string(b))
	})

	// check JSON
	selectStmt, err = conn.Select(context.Background(), selectQuery)

	require.NoError(t, err)

	err = jsonFormat.ReadEachRow(selectStmt)
	require.NoError(t, err)

	jsonFromClickhouse := httpJSON(selectQuery)

	d := json.NewDecoder(strings.NewReader(strings.Join(chconnJSON, "\n")))
	var valsChconn []any
	iff := 0
	for {
		var v any
		if err := d.Decode(&v); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		iff++
		valsChconn = append(valsChconn, v)
	}

	d = json.NewDecoder(bytes.NewReader(jsonFromClickhouse))
	var valsClickhouse []any
	for {
		var v any
		if err := d.Decode(&v); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		valsClickhouse = append(valsClickhouse, v)
	}

	assert.Equal(t, valsClickhouse, valsChconn)
}

func TestTupleNoColumn(t *testing.T) {
	assert.Panics(t, func() { column.NewTuple() })
}

func TestTupleScan(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	tupleColString := column.NewString()
	tupleColVal1 := column.New[int64]()
	tupleColVal2 := column.New[int64]()
	tupleColInside := column.NewTuple(tupleColVal1, tupleColVal2)
	tupleCol := column.NewTuple(tupleColString, tupleColInside)

	stmt, err := conn.Select(context.Background(), `SELECT tuple('test',tuple(toInt64(1),toInt64(2)))`, tupleCol)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())
	type tupleData struct {
		Col1 string           `db:"0"`
		Col2 map[string]int64 `db:"1"`
	}
	mapData := make(map[string]any)
	structData := tupleData{}
	for stmt.Next() {
		for i := 0; i < stmt.RowsInBlock(); i++ {
			require.NoError(t, tupleCol.Scan(i, &mapData))
			require.NoError(t, tupleCol.Scan(i, &structData))
		}
	}

	assert.Equal(t, map[string]any{
		"0": "test",
		"1": []any{
			int64(1),
			int64(2),
		},
	}, mapData)

	assert.Equal(t, tupleData{
		Col1: "test",
		Col2: map[string]int64{
			"0": int64(1),
			"1": int64(2),
		},
	}, structData)
}

func TestTupleArrayScan(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT [tuple(toInt64(1),toInt64(2)),tuple(toInt64(3),toInt64(4))] AS arr")
	require.NoError(t, err)
	defer rows.Close()
	assert.True(t, rows.Next())
	require.NoError(t, rows.Err())
	var arr []map[string]int64
	err = rows.Scan(&arr)
	require.NoError(t, err)
	assert.Equal(t, []map[string]int64{
		{
			"0": int64(1),
			"1": int64(2),
		},
		{
			"0": int64(3),
			"1": int64(4),
		},
	}, arr)

	var invalidArr int64
	err = rows.Scan(&invalidArr)
	assert.Equal(t, "can't scan into dest[0]: dest must be a pointer to slice", err.Error())

	var invalidArrInside []int64
	err = rows.Scan(&invalidArrInside)
	assert.Equal(t, "can't scan into dest[0]: cannot scan array item 0: tuple: scan: unsupported type int64", err.Error())
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

	colPoint := column.NewPoint()
	colRing := column.NewPoint().Array()
	colPolygon := column.NewPoint().Array().Array()
	colMultiPolygon := column.NewPoint().Array().Array().Array()

	colPoint.SetWriteBufferSize(20)
	colRing.SetWriteBufferSize(20)
	colPolygon.SetWriteBufferSize(20)
	colMultiPolygon.SetWriteBufferSize(20)

	var pointInsert []types.Point
	var ringInsert [][]types.Point
	var polygonInsert [][][]types.Point
	var multiPolygonInsert [][][][]types.Point

	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		for i := 0; i < rows; i++ {
			pointValue := types.Point{
				Col1: float64(i),
				Col2: float64(i + 1),
			}
			ringValue := []types.Point{
				{
					Col1: float64(i),
					Col2: float64(i + 1),
				},
				{
					Col1: float64(i + 2),
					Col2: float64(i + 3),
				},
			}
			polygonValue := [][]types.Point{
				{
					{
						Col1: float64(i),
						Col2: float64(i + 1),
					},
					{
						Col1: float64(i + 2),
						Col2: float64(i + 3),
					},
				},
				{
					{
						Col1: float64(i),
						Col2: float64(i + 1),
					},
					{
						Col1: float64(i + 2),
						Col2: float64(i + 3),
					},
				},
			}
			multiPolygonValue := [][][]types.Point{
				{
					{
						{
							Col1: float64(i),
							Col2: float64(i + 1),
						},
						{
							Col1: float64(i + 2),
							Col2: float64(i + 3),
						},
					},
					{
						{
							Col1: float64(i),
							Col2: float64(i + 1),
						},
						{
							Col1: float64(i + 2),
							Col2: float64(i + 3),
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

	colPointRead := column.NewPoint()
	colRingRead := column.NewPoint().Array()
	colPolygonRead := column.NewPoint().Array().Array()
	colMultiPolygonRead := column.NewPoint().Array().Array().Array()

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
