package column_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/format"
)

func deepConvertToInterfaceSlice(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		return v
	}
	result := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		result[i] = deepConvertToInterfaceSlice(rv.Index(i).Interface())
	}
	return result
}

func TestDynamic(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	tableName := "dynamic"

	err = conn.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_%s`, tableName),
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:  "allow_experimental_dynamic_type",
			Value: "true",
		},
	}
	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
				block_id UInt8,
				col Dynamic(),
				col_array Array(Dynamic())
			) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)
	blockID := column.New[uint8]()
	col1 := column.New[int64]()
	col2 := column.NewString()
	col3 := column.New[uint32]()
	col4 := column.NewString()
	dynamicCol := column.NewDynamic(col1, col2)
	dynamicArrayCol := column.NewArray(column.NewDynamic(col3, col4))
	var colInsert []any
	var colArrayInsert [][]any

	// SetWriteBufferSize is not necessary. this just to show how to set write buffer
	dynamicCol.SetWriteBufferSize(10)
	for insertN := 0; insertN < 2; insertN++ {
		rows := 2
		for i := 0; i < rows; i++ {
			blockID.Append(uint8(insertN))
			if i%2 == 0 {
				col1.Append(int64(i))
				colInsert = append(colInsert, int64(i))
			} else {
				col2.Append(fmt.Sprintf("row %d", i))
				colInsert = append(colInsert, fmt.Sprintf("row %d", i))
			}
			col3.Append(uint32(i))
			col4.Append(fmt.Sprintf("row %d", i))
			dynamicArrayCol.AppendLen(2)
			colArrayInsert = append(colArrayInsert, []any{uint32(i), fmt.Sprintf("row %d", i)})
		}

		err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				col,
				col_array
				)
			VALUES`, tableName),
			dynamicCol,
			dynamicArrayCol,
		)

		require.NoError(t, err)
	}

	dynamicCol = column.NewDynamic()
	blockID = column.New[uint8]()
	dynamicArrayCol = column.NewArray(column.NewDynamic())
	// test insert any with different types
	for insertN := 0; insertN < 2; insertN++ {
		rows := 2
		for i := 0; i < rows; i++ {
			blockID.Append(uint8(insertN + 2))
			if insertN == 0 {
				if i%2 == 0 {
					dynamicCol.Append(uint32(i * 4))
					colInsert = append(colInsert, uint32(i*4))
				} else {
					dynamicCol.Append([]uint32{1 * 4, 2 * 4})
					colInsert = append(colInsert, []uint32{1 * 4, 2 * 4})
				}
			} else {
				if i%2 == 0 {
					dynamicCol.Append(uint16(i * 4))
					colInsert = append(colInsert, uint16(i*4))
				} else {
					dynamicCol.Append([][]uint16{{1 * 4, 2 * 4}})
					colInsert = append(colInsert, [][]uint16{{1 * 4, 2 * 4}})
				}
			}
			dynamicArrayCol.Append([]any{uint32(i * 4), fmt.Sprintf("row %d", i)})
			colArrayInsert = append(colArrayInsert, []any{uint32(i * 4), fmt.Sprintf("row %d", i)})
		}
		blockID.Append(uint8(insertN + 2))
		dynamicCol.Append(nil)
		colInsert = append(colInsert, nil)
		dynamicArrayCol.Append([]any{nil})
		colArrayInsert = append(colArrayInsert, []any{nil})
		err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				block_id,
				col,
				col_array
				)
			VALUES`, tableName),
			blockID,
			dynamicCol,
			dynamicArrayCol,
		)
		require.NoError(t, err)
	}

	colRead := column.NewDynamic()
	colArrayRead := column.NewArray(column.NewDynamic())

	selectStmt, err := conn.Select(context.Background(), fmt.Sprintf(`SELECT
		col,
		col_array
		FROM test_%[1]s order by block_id`, tableName),
		colRead,
		colArrayRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())
	var colData []any
	var colArrayData [][]any
	for selectStmt.Next() {
		colData = colRead.Read(colData)
		colArrayData = colArrayRead.Read(colArrayData)
	}
	require.NoError(t, selectStmt.Err())

	assert.Equal(t, deepConvertToInterfaceSlice(colInsert), deepConvertToInterfaceSlice(colData))
	assert.Equal(t, colArrayInsert, colArrayData)
	// test read Row
	colRead = column.NewDynamic()
	colArrayRead = column.NewArray(column.NewDynamic())

	selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
		col,
		col_array
		FROM test_%[1]s order by block_id`, tableName),
		colRead,
		colArrayRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())
	colData = colData[:0]
	colArrayData = colArrayData[:0]
	for selectStmt.Next() {
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			colData = append(colData, colRead.Row(i))
			colArrayData = append(colArrayData, colArrayRead.Row(i))
		}
	}
	require.NoError(t, selectStmt.Err())

	assert.Equal(t, deepConvertToInterfaceSlice(colInsert), deepConvertToInterfaceSlice(colData))
	assert.Equal(t, colArrayInsert, colArrayData)

	selectQuery := fmt.Sprintf(`SELECT
		 col,
		 col_array
		 FROM test_%[1]s order by block_id`, tableName)
	// check dynamic column
	selectStmt, err = conn.Select(context.Background(), selectQuery)

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()

	assert.Len(t, autoColumns, 2)

	assert.Equal(t, colRead.FullType(), autoColumns[0].FullType())
	colData = colData[:0]
	colArrayData = colArrayData[:0]

	rows := selectStmt.Rows()
	for rows.Next() {
		var colVal any
		var colValArray []any

		err := rows.Scan(
			&colVal,
			&colValArray,
		)
		require.NoError(t, err)
		colData = append(colData, colVal)
		colArrayData = append(colArrayData, colValArray)
	}
	require.NoError(t, selectStmt.Err())

	assert.Equal(t, deepConvertToInterfaceSlice(colInsert), deepConvertToInterfaceSlice(colData))
	assert.Equal(t, colArrayInsert, colArrayData)

	selectStmt.Close()

	var chconnJSON []string
	jsonFormat := format.NewJSON(1000, func(b []byte, cb []column.ColumnCore) {
		chconnJSON = append(chconnJSON, string(b))
	})

	// check JSON
	selectStmt, err = conn.Select(context.Background(), selectQuery)

	require.NoError(t, err)

	err = jsonFormat.ReadEachRow(selectStmt)
	require.NoError(t, err)

	jsonFromClickhouse := httpJSON(selectQuery)

	var valsChconn []any
	for index, j := range chconnJSON {
		var v any
		if err := json.Unmarshal([]byte(j), &v); err == io.EOF {
			break
		} else if err != nil {
			require.NoError(t, err, "index %d", index)
		}
		//nolint:staticcheck
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
		//nolint:staticcheck
		valsClickhouse = append(valsClickhouse, v)
	}
	// shared variant is not supported finished for json yet
	// assert.Equal(t, valsClickhouse, valsChconn)
}
