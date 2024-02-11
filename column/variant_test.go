package column_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/format"
)

func TestVariant(t *testing.T) {
	tableName := "variant"
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
			Name:  "allow_experimental_variant_type",
			Value: "1",
		},
	}

	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
		%[1]s Variant(String, Int64),
		%[1]s_array Variant(Array(String),Array(Int64)),
		%[1]s_array_nullable Variant(Array(Nullable(String)),Array(Nullable(Int64))),
		%[1]s_lc Variant(LowCardinality(String),LowCardinality(Int64)),
		%[1]s_array_lc Variant(Array(LowCardinality(String)),Array(LowCardinality(Int64))),
		%[1]s_array_lc_nullable Variant(Array(LowCardinality(Nullable(String))),Array(LowCardinality(Nullable(Int64)))),
		%[1]s_array_array_variant Array(Array(Variant(String, Int64)))
			) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	colString := column.NewString()
	colInt := column.New[int64]()
	col := column.NewVariant(colString, colInt)

	colArrayString := column.NewString().Array()
	colArrayInt := column.New[int64]().Array()
	colArray := column.NewVariant(colArrayString, colArrayInt)

	colNullableArrayString := column.NewString().Nullable().Array()
	colNullableArrayInt := column.New[int64]().Nullable().Array()
	colNullableArray := column.NewVariant(colNullableArrayString, colNullableArrayInt)

	colLCString := column.NewString().LowCardinality()
	colLCInt := column.New[int64]().LowCardinality()
	colLC := column.NewVariant(colLCString, colLCInt)

	colArrayLCString := column.NewString().LowCardinality().Array()
	colArrayLCInt := column.New[int64]().LowCardinality().Array()
	colArrayLC := column.NewVariant(colArrayLCString, colArrayLCInt)

	colArrayLCNullableString := column.NewString().LowCardinality().Nullable().Array()
	colArrayLCNullableInt := column.New[int64]().LowCardinality().Nullable().Array()
	colArrayLCNullable := column.NewVariant(colArrayLCNullableString, colArrayLCNullableInt)

	colArrayArrayVariantString := column.NewString()
	colArrayArrayVariantInt := column.New[int64]()
	colArrayArrayVariantMain := column.NewVariant(colArrayArrayVariantString, colArrayArrayVariantInt)
	colArrayArrayVariant := colArrayArrayVariantMain.Array().Array()

	var colInsert []any
	var colArrayInsert []any
	var colArrayNullableInsert []any
	var colLCInsert []any
	var colLCArrayInsert []any
	var colLCNullableArrayInsert []any

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
			colArrayArrayVariant.AppendLen(1)
			colArrayArrayVariant.Column().(*column.Array[any]).AppendLen(2)
			switch i % 3 {
			case 0:
				colString.Append(valString)
				colInsert = append(colInsert, valString)

				colArrayString.Append(valArrayString)
				colArrayInsert = append(colArrayInsert, valArrayString)

				colNullableArrayString.AppendP(valArrayNilString)
				colArrayNullableInsert = append(colArrayNullableInsert, valArrayNilString)

				colLCInsert = append(colLCInsert, valString)
				colLCString.Append(valString)

				colArrayLCString.Append(valArrayString)
				colLCArrayInsert = append(colLCArrayInsert, valArrayString)

				colArrayLCNullableString.AppendP(valArrayNilString)
				colArrayArrayVariantString.AppendMulti(valString, val2String)
				colLCNullableArrayInsert = append(colLCNullableArrayInsert, valArrayNilString)

			case 1:
				colInt.Append(valInt)
				colInsert = append(colInsert, valInt)

				colArrayInt.Append(valArrayInt)
				colArrayInsert = append(colArrayInsert, valArrayInt)

				colNullableArrayInt.AppendP(valArrayNilInt)
				colArrayNullableInsert = append(colArrayNullableInsert, valArrayNilInt)

				colLCInsert = append(colLCInsert, valInt)
				colLCInt.Append(valInt)

				colArrayLCInt.Append(valArrayInt)
				colLCArrayInsert = append(colLCArrayInsert, valArrayInt)

				colArrayLCNullableInt.AppendP(valArrayNilInt)
				colArrayArrayVariantInt.AppendMulti(valInt, val2Int)
				colLCNullableArrayInsert = append(colLCNullableArrayInsert, valArrayNilInt)
			case 2:
				col.AppendNil()
				colInsert = append(colInsert, nil)

				colArray.AppendNil()
				colArrayInsert = append(colArrayInsert, nil)

				colNullableArray.AppendNil()
				colArrayNullableInsert = append(colArrayNullableInsert, nil)

				colLC.AppendNil()
				colLCInsert = append(colLCInsert, nil)

				colArrayLC.AppendNil()
				colLCArrayInsert = append(colLCArrayInsert, nil)

				colArrayLCNullable.AppendNil()
				colLCNullableArrayInsert = append(colLCNullableArrayInsert, nil)

				colArrayArrayVariantMain.AppendNil()
				colArrayArrayVariantMain.AppendNil()
			}
		}

		err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				%[1]s,
				%[1]s_array,
				%[1]s_array_nullable,
				%[1]s_lc,
				%[1]s_array_lc,
				%[1]s_array_lc_nullable,
				%[1]s_array_array_variant
			)
		VALUES`, tableName),
			col,
			colArray,
			colNullableArray,
			colLC,
			colArrayLC,
			colArrayLCNullable,
			colArrayArrayVariant,
		)
		require.NoError(t, err)
	}

	// example read all

	colStringRead := column.NewString()
	colIntRead := column.New[int64]()
	colRead := column.NewVariant(colStringRead, colIntRead)

	colArrayStringRead := column.NewString().Array()
	colArrayIntRead := column.New[int64]().Array()
	colArrayRead := column.NewVariant(colArrayStringRead, colArrayIntRead)

	colNullableArrayStringRead := column.NewString().Nullable().Array()
	colNullableArrayIntRead := column.New[int64]().Nullable().Array()
	colNullableArrayRead := column.NewVariant(colNullableArrayStringRead, colNullableArrayIntRead)

	colLCStringRead := column.NewString().LowCardinality()
	colLCIntRead := column.New[int64]().LowCardinality()
	colLCRead := column.NewVariant(colLCStringRead, colLCIntRead)

	colArrayLCStringRead := column.NewString().LowCardinality().Array()
	colArrayLCIntRead := column.New[int64]().LowCardinality().Array()
	colArrayLCRead := column.NewVariant(colArrayLCStringRead, colArrayLCIntRead)

	colArrayLCNullableStringRead := column.NewString().LowCardinality().Nullable().Array()
	colArrayLCNullableIntRead := column.New[int64]().LowCardinality().Nullable().Array()
	colArrayLCNullableRead := column.NewVariant(colArrayLCNullableStringRead, colArrayLCNullableIntRead)

	selectQuery := fmt.Sprintf(`SELECT
	%[1]s,
	%[1]s_array,
	%[1]s_array_nullable,
	%[1]s_lc,
	%[1]s_array_lc,
	%[1]s_array_lc_nullable
	FROM test_%[1]s`, tableName)
	selectStmt, err := conn.Select(context.Background(), selectQuery,
		colRead,
		colArrayRead,
		colNullableArrayRead,
		colLCRead,
		colArrayLCRead,
		colArrayLCNullableRead)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var colData []any
	var colArrayData []any
	var colArrayNullableData []any
	var colLCData []any
	var colLCArrayData []any
	var colLCNullableArrayData []any

	for selectStmt.Next() {
		colData = colRead.Read(colData)
		colArrayData = colArrayRead.Read(colArrayData)
		colArrayNullableData = colNullableArrayRead.Read(colArrayNullableData)
		colLCData = colLCRead.Read(colLCData)
		colLCArrayData = colArrayLCRead.Read(colLCArrayData)
		colLCNullableArrayData = colArrayLCNullableRead.Read(colLCNullableArrayData)
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
	assert.Equal(t, colLCInsert, colLCData)
	assert.Equal(t, colLCArrayInsert, colLCArrayData)
	assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)

	// check dynamic column
	selectStmt, err = conn.Select(context.Background(), selectQuery)

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()

	assert.Len(t, autoColumns, 6)

	assert.Equal(t, colRead.FullType(), autoColumns[0].FullType())
	assert.Equal(t, colArrayRead.FullType(), autoColumns[1].FullType())
	assert.Equal(t, colNullableArrayRead.FullType(), autoColumns[2].FullType())
	assert.Equal(t, colLCRead.FullType(), autoColumns[3].FullType())
	assert.Equal(t, colArrayLCRead.FullType(), autoColumns[4].FullType())
	assert.Equal(t, colArrayLCNullableRead.FullType(), autoColumns[5].FullType())
	rows := selectStmt.Rows()

	colData = colData[:0]
	colArrayData = colArrayData[:0]
	colArrayNullableData = colArrayNullableData[:0]
	colLCData = colLCData[:0]
	colLCArrayData = colLCArrayData[:0]
	colLCNullableArrayData = colLCNullableArrayData[:0]
	for rows.Next() {
		var colVal any
		var colArrayVal any
		var colArrayNullableVal any
		var colLCVal any
		var colLCArrayVal any
		var colLCNullableArrayVal any
		err := rows.Scan(
			&colVal,
			&colArrayVal,
			&colArrayNullableVal,
			&colLCVal,
			&colLCArrayVal,
			&colLCNullableArrayVal,
		)
		require.NoError(t, err)
		colData = append(colData, colVal)
		colArrayData = append(colArrayData, colArrayVal)
		colArrayNullableData = append(colArrayNullableData, colArrayNullableVal)
		colLCData = append(colLCData, colLCVal)
		colLCArrayData = append(colLCArrayData, colLCArrayVal)
		colLCNullableArrayData = append(colLCNullableArrayData, colLCNullableArrayVal)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
	assert.Equal(t, colLCInsert, colLCData)
	assert.Equal(t, colLCArrayInsert, colLCArrayData)
	assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)

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

	var valsChconn []any
	iff := 0
	for i, jsonData := range chconnJSON {
		var v any
		if err := json.Unmarshal([]byte(jsonData), &v); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("error unmarshal json %d: %s", i, err.Error())
		}
		iff++
		valsChconn = append(valsChconn, v)
	}

	d := json.NewDecoder(bytes.NewReader(jsonFromClickhouse))
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

func TestVariantNoColumn(t *testing.T) {
	assert.Panics(t, func() { column.NewVariant() })
}
