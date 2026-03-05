package column_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/format"
)

func TestJSON(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	tableName := "json_str"

	err = conn.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_%s`, tableName),
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:  "allow_experimental_json_type",
			Value: "true",
		},
		{
			Name:  "allow_experimental_dynamic_type",
			Value: "true",
		},
		{
			Name:  "output_format_native_write_json_as_string",
			Value: "1",
		},
	}
	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
				block_id UInt8,
				col JSON
			) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	blockID := column.New[uint8]()
	col := column.NewJSON()
	var colInsert []string

	// SetWriteBufferSize is not necessary. this just to show how to set write buffer
	col.SetWriteBufferSize(10)
	for insertN := 0; insertN < 2; insertN++ {
		rows := 5
		for i := 0; i < rows; i++ {
			blockID.Append(uint8(insertN))
			val := fmt.Sprintf(`{"name":"user_%d","batch":%d,"score":%d}`, i, insertN, i*10+insertN)
			col.Append(val)
			colInsert = append(colInsert, val)
		}

		err = conn.InsertWithOption(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				block_id,
				col
				)
			VALUES`, tableName),
			&chconn.QueryOptions{Settings: set},
			blockID,
			col,
		)

		require.NoError(t, err)
	}

	// test read all with Read()
	colRead := column.NewJSON()

	selectQuery := fmt.Sprintf(`SELECT
		col
		FROM test_%[1]s order by block_id`, tableName)
	selectStmt, err := conn.SelectWithOption(context.Background(), selectQuery,
		&chconn.QueryOptions{Settings: set},
		colRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())
	var colData []any
	for selectStmt.Next() {
		colData = colRead.Read(colData)
	}
	require.NoError(t, selectStmt.Err())

	require.Len(t, colData, len(colInsert))
	for i, expected := range colInsert {
		actual, ok := colData[i].(string)
		require.True(t, ok, "row %d: expected string, got %T", i, colData[i])
		assert.JSONEq(t, expected, actual)
	}

	// test read Row
	colRead = column.NewJSON()

	selectStmt, err = conn.SelectWithOption(context.Background(), selectQuery,
		&chconn.QueryOptions{Settings: set},
		colRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())
	colData = colData[:0]
	for selectStmt.Next() {
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			colData = append(colData, colRead.Row(i))
		}
	}
	require.NoError(t, selectStmt.Err())

	require.Len(t, colData, len(colInsert))
	for i, expected := range colInsert {
		actual, ok := colData[i].(string)
		require.True(t, ok, "row %d: expected string, got %T", i, colData[i])
		assert.JSONEq(t, expected, actual)
	}

	// check auto-column detection
	selectStmt, err = conn.SelectWithOption(context.Background(), selectQuery,
		&chconn.QueryOptions{Settings: set},
	)

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()

	assert.Len(t, autoColumns, 1)

	colData = colData[:0]

	rows := selectStmt.Rows()
	for rows.Next() {
		var colVal string

		err := rows.Scan(
			&colVal,
		)
		require.NoError(t, err)
		colData = append(colData, colVal)
	}
	require.NoError(t, selectStmt.Err())

	require.Len(t, colData, len(colInsert))
	for i, expected := range colInsert {
		actual, ok := colData[i].(string)
		require.True(t, ok, "row %d: expected string, got %T", i, colData[i])
		assert.JSONEq(t, expected, actual)
	}

	selectStmt.Close()

	var chconnJSON []string
	jsonFormat := format.NewJSON(1000, func(b []byte, cb []column.ColumnCore) {
		chconnJSON = append(chconnJSON, string(b))
	})

	// check JSON format
	selectStmt, err = conn.SelectWithOption(context.Background(), selectQuery,
		&chconn.QueryOptions{Settings: set},
	)

	require.NoError(t, err)

	err = jsonFormat.ReadEachRow(selectStmt)
	require.NoError(t, err)

	// Use output_format_json_quote_64bit_integers=0 so ClickHouse HTTP output
	// matches our raw JSON (string mode preserves original number types).
	jsonFromClickhouse := httpJSONWithSettings(selectQuery,
		"allow_experimental_json_type=1",
		"allow_experimental_dynamic_type=1",
		"output_format_native_write_json_as_string=1",
		"output_format_json_quote_64bit_integers=0",
		"output_format_json_quote_decimals=1",
	)

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
	assert.Equal(t, valsClickhouse, valsChconn)
}

func TestJSONObject(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	tableName := "json_object"

	err = conn.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_%s`, tableName),
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:  "allow_experimental_json_type",
			Value: "true",
		},
		{
			Name:  "allow_experimental_dynamic_type",
			Value: "true",
		},
	}
	insertSet := chconn.Settings{
		{
			Name:  "allow_experimental_json_type",
			Value: "true",
		},
		{
			Name:  "allow_experimental_dynamic_type",
			Value: "true",
		},
		{
			Name:  "output_format_native_write_json_as_string",
			Value: "1",
		},
	}
	err = conn.ExecWithOption(context.Background(), fmt.Sprintf(`CREATE TABLE test_%[1]s (
				block_id UInt8,
				col JSON
			) Engine=Memory`, tableName), &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	blockID := column.New[uint8]()
	col := column.NewJSON()
	var colInsert []string

	col.SetWriteBufferSize(10)
	for insertN := 0; insertN < 2; insertN++ {
		rows := 5
		for i := 0; i < rows; i++ {
			blockID.Append(uint8(insertN))
			val := fmt.Sprintf(`{"name":"user_%d","batch":%d,"score":%d}`, i, insertN, i*10+insertN)
			col.Append(val)
			colInsert = append(colInsert, val)
		}

		err = conn.InsertWithOption(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				block_id,
				col
				)
			VALUES`, tableName),
			&chconn.QueryOptions{Settings: insertSet},
			blockID,
			col,
		)

		require.NoError(t, err)
	}

	// Read in object mode (no output_format_native_write_json_as_string)
	// test read all with Read()
	colRead := column.NewJSON()

	selectQuery := fmt.Sprintf(`SELECT
		col
		FROM test_%[1]s order by block_id`, tableName)
	selectStmt, err := conn.SelectWithOption(context.Background(), selectQuery,
		&chconn.QueryOptions{Settings: set},
		colRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())
	var colData []any
	for selectStmt.Next() {
		colData = colRead.Read(colData)
	}
	require.NoError(t, selectStmt.Err())

	require.Len(t, colData, len(colInsert))
	for i, expected := range colInsert {
		obj, ok := colData[i].(map[string]any)
		require.True(t, ok, "row %d: expected map[string]any, got %T", i, colData[i])
		// convert both to JSON for comparison
		actualJSON, err := json.Marshal(obj)
		require.NoError(t, err)
		assert.JSONEq(t, expected, string(actualJSON))
	}

	// test read Row
	colRead = column.NewJSON()

	selectStmt, err = conn.SelectWithOption(context.Background(), selectQuery,
		&chconn.QueryOptions{Settings: set},
		colRead,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())
	colData = colData[:0]
	for selectStmt.Next() {
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			colData = append(colData, colRead.Row(i))
		}
	}
	require.NoError(t, selectStmt.Err())

	require.Len(t, colData, len(colInsert))
	for i, expected := range colInsert {
		obj, ok := colData[i].(map[string]any)
		require.True(t, ok, "row %d: expected map[string]any, got %T", i, colData[i])
		actualJSON, err := json.Marshal(obj)
		require.NoError(t, err)
		assert.JSONEq(t, expected, string(actualJSON))
	}

	// check auto-column and scan to string
	selectStmt, err = conn.SelectWithOption(context.Background(), selectQuery,
		&chconn.QueryOptions{Settings: set},
	)

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()

	assert.Len(t, autoColumns, 1)

	colData = colData[:0]

	rows := selectStmt.Rows()
	for rows.Next() {
		var colVal string

		err := rows.Scan(
			&colVal,
		)
		require.NoError(t, err)
		colData = append(colData, colVal)
	}
	require.NoError(t, selectStmt.Err())

	require.Len(t, colData, len(colInsert))
	for i, expected := range colInsert {
		actual, ok := colData[i].(string)
		require.True(t, ok, "row %d: expected string, got %T", i, colData[i])
		// ClickHouse quotes Int64/UInt64 in JSON output, so compare
		// parsed values by their string representation
		assertJSONValuesEqual(t, expected, actual, "scan-to-string row %d", i)
	}

	selectStmt.Close()

	// scan to map
	colRead = column.NewJSON()
	selectStmt, err = conn.SelectWithOption(context.Background(), selectQuery,
		&chconn.QueryOptions{Settings: set},
		colRead,
	)
	require.NoError(t, err)
	colData = colData[:0]
	for selectStmt.Next() {
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			var m map[string]any
			err := colRead.Scan(i, &m)
			require.NoError(t, err)
			colData = append(colData, m)
		}
	}
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()

	require.Len(t, colData, len(colInsert))
	for i, expected := range colInsert {
		obj, ok := colData[i].(map[string]any)
		require.True(t, ok, "row %d: expected map[string]any, got %T", i, colData[i])
		actualJSON, err := json.Marshal(obj)
		require.NoError(t, err)
		assert.JSONEq(t, expected, string(actualJSON))
	}

	// scan to []byte
	colRead = column.NewJSON()
	selectStmt, err = conn.SelectWithOption(context.Background(), selectQuery,
		&chconn.QueryOptions{Settings: set},
		colRead,
	)
	require.NoError(t, err)
	colData = colData[:0]
	for selectStmt.Next() {
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			var b []byte
			err := colRead.Scan(i, &b)
			require.NoError(t, err)
			colData = append(colData, string(b))
		}
	}
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()

	require.Len(t, colData, len(colInsert))
	for i, expected := range colInsert {
		actual, ok := colData[i].(string)
		require.True(t, ok, "row %d: expected string, got %T", i, colData[i])
		assertJSONValuesEqual(t, expected, actual, "scan-to-bytes row %d", i)
	}

	// check ToJSON
	colRead = column.NewJSON()
	selectStmt, err = conn.SelectWithOption(context.Background(), selectQuery,
		&chconn.QueryOptions{Settings: set},
		colRead,
	)
	require.NoError(t, err)
	for selectStmt.Next() {
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			jsonBytes := colRead.ToJSON(i, false, nil)
			var parsed map[string]any
			require.NoError(t, json.Unmarshal(jsonBytes, &parsed), "ToJSON produced invalid JSON: %s", string(jsonBytes))
		}
	}
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()
}

func TestJSONValueAppendAndValidate(t *testing.T) {
	t.Parallel()

	jv1 := column.NewJSONValue()
	jv1.SetValueAtPath("name", "Alice")
	jv1.SetValueAtPath("age", int64(30))

	jv2 := column.NewJSONValue()
	jv2.SetValueAtPath("name", "Bob")
	jv2.SetValueAtPath("score", float64(99.5))

	col := column.NewJSON()
	col.Append(jv1)
	col.Append(jv2)

	require.NoError(t, col.ValidateInsert())
	assert.Equal(t, 2, col.NumRow())
}

func TestJSONValueNestedMap(t *testing.T) {
	t.Parallel()

	jv := column.NewJSONValue()
	jv.SetValueAtPath("a.b.c", "deep")
	jv.SetValueAtPath("a.x", int64(42))

	nested := jv.NestedMap()
	aMap, ok := nested["a"].(map[string]any)
	require.True(t, ok)
	bMap, ok := aMap["b"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "deep", bMap["c"])
	assert.Equal(t, int64(42), aMap["x"])
}

func TestJSONMapAppend(t *testing.T) {
	t.Parallel()

	col := column.NewJSON()
	col.Append(map[string]any{
		"name": "Alice",
		"meta": map[string]any{
			"score": int64(100),
		},
	})
	col.Append(map[string]any{
		"name": "Bob",
	})

	require.NoError(t, col.ValidateInsert())
	assert.Equal(t, 2, col.NumRow())
}

func TestJSONNilDynamicPathHandling(t *testing.T) {
	t.Parallel()

	col := column.NewJSON()

	// First row has "a" and "b"
	jv1 := column.NewJSONValue()
	jv1.SetValueAtPath("a", "hello")
	jv1.SetValueAtPath("b", int64(1))
	col.Append(jv1)

	// Second row has only "a" — "b" should get backfilled with nil
	jv2 := column.NewJSONValue()
	jv2.SetValueAtPath("a", "world")
	col.Append(jv2)

	// Third row introduces "c" — "c" should be backfilled for rows 1 and 2
	jv3 := column.NewJSONValue()
	jv3.SetValueAtPath("a", "!")
	jv3.SetValueAtPath("c", float64(3.14))
	col.Append(jv3)

	require.NoError(t, col.ValidateInsert())
	assert.Equal(t, 3, col.NumRow())
}

// assertJSONValuesEqual compares two JSON strings by parsing them and comparing
// each value via fmt.Sprint. This handles ClickHouse's convention of quoting
// Int64/UInt64 values in JSON output (e.g., "0" vs 0).
func assertJSONValuesEqual(t *testing.T, expected, actual string, msgAndArgs ...any) {
	t.Helper()
	var expectedMap, actualMap map[string]any
	require.NoError(t, json.Unmarshal([]byte(expected), &expectedMap), msgAndArgs...)
	require.NoError(t, json.Unmarshal([]byte(actual), &actualMap), msgAndArgs...)
	require.Equal(t, len(expectedMap), len(actualMap), msgAndArgs...)
	for k, ev := range expectedMap {
		av, ok := actualMap[k]
		require.True(t, ok, "missing key %q", k)
		assert.Equal(t, fmt.Sprint(ev), fmt.Sprint(av), "key %q", k)
	}
}

// httpJSONWithSettings is like httpJSON but allows custom ClickHouse settings.
func httpJSONWithSettings(query string, settings ...string) []byte {
	url := os.Getenv("CHX_TEST_HTTP_CONN_STRING")
	if url == "" {
		url = "http://localhost:8123"
	}
	url += "?" + strings.Join(settings, "&")

	query += " FORMAT JSONEachRow"

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(query))
	if err != nil {
		panic(err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return body
}

func BenchmarkJSONObjectAppend(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col := column.NewJSON()
		for j := 0; j < 10000; j++ {
			jv := column.NewJSONValue()
			jv.SetValueAtPath("name", "test")
			jv.SetValueAtPath("value", int64(j))
			col.Append(jv)
		}
	}
}

func BenchmarkJSONStringAppend(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col := column.NewJSON()
		for j := 0; j < 10000; j++ {
			col.Append(fmt.Sprintf(`{"name":"test","value":%d}`, j))
		}
	}
}
