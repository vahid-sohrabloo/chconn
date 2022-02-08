package export

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
)

type ColumnTest struct {
	Name   string
	ChType string
}

var mainDataTypes = []ColumnTest{
	{"int8", "Int8"},
	{"int16", "Int16"},
	{"int32", "Int32"},
	{"int64", "Int64"},
	{"uint8", "UInt8"},
	{"uint16", "UInt16"},
	{"uint32", "UInt32"},
	{"uint64", "UInt64"},
	{"float32", "Float32"},
	{"float64", "Float64"},
	{"string", "String"},
	{"fixedString", "FixedString(10)"},
	{"decimal32", "Decimal32(3)"},
	{"decimal64", "Decimal64(3)"},
	{"date", "Date"},
	{"date32", "Date32"},
	{"datetime", "DateTime('CET')"},
	{"datetime64", "DateTime64(3, 'CET')"},
	{"enum8", "Enum8('hello' = 1, 'world' = 2)"},
	{"enum16", "Enum16('hello' = 1, 'world' = 2)"},
	{"uuid", "UUID"},
	{"ipv4", "IPv4"},
	{"ipv6", "IPv6"},
}

func TestJSON(t *testing.T) {
	var columnTypes []string
	for _, dataType := range mainDataTypes {
		columnTypes = append(columnTypes, dataType.Name+" "+dataType.ChType,
			dataType.Name+"_nullable Nullable("+dataType.ChType+")",
			dataType.Name+"_array Array("+dataType.ChType+")",
			dataType.Name+"_array_nullable Array(Nullable("+dataType.ChType+"))")
	}
	columnTypes = append(columnTypes, "tuple Tuple(String,Nullable(String))")
	createTable := "CREATE TABLE chconn_json_export_example (" + strings.Join(columnTypes, ",\n") + ") ENGINE = GenerateRandom(1, 5, 3)"

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS chconn_json_export_example`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), createTable)
	require.NoError(t, err)
	require.Nil(t, res)

	selectStmt, err := conn.Select(context.Background(), `SELECT *,
				toLowCardinality(string),toLowCardinality(string_nullable),
				toLowCardinality(fixedString),toLowCardinality(fixedString_nullable)
				 FROM chconn_json_export_example limit 200`)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	data := []byte{'['}
	jsonExport := NewJSON(2, func(b []byte, c []column.Column) {
		data = append(data, b...)
	})
	jsonExport.Read(selectStmt)
	data = append(data, ']')
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()
	var exportData interface{}
	err = json.Unmarshal(data, &exportData)
	assert.NoError(t, err)
}

func TestJSONCompact(t *testing.T) {
	var columnTypes []string
	for _, dataType := range mainDataTypes {
		columnTypes = append(columnTypes, dataType.Name+" "+dataType.ChType,
			dataType.Name+"_nullable Nullable("+dataType.ChType+")",
			dataType.Name+"_array Array("+dataType.ChType+")",
			dataType.Name+"_array_nullable Array(Nullable("+dataType.ChType+"))")
	}
	columnTypes = append(columnTypes, "tuple Tuple(String,Nullable(String))")
	createTable := `CREATE TABLE
				chconn_json_compact_export_example
				(` + strings.Join(columnTypes, ",\n") + `)
				ENGINE = GenerateRandom(1, 5, 3)`

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS chconn_json_compact_export_example`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), createTable)
	require.NoError(t, err)
	require.Nil(t, res)

	selectStmt, err := conn.Select(context.Background(), `SELECT *,
				toLowCardinality(string),toLowCardinality(string_nullable),
				toLowCardinality(fixedString),toLowCardinality(fixedString_nullable)
				 FROM chconn_json_compact_export_example limit 200`)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())
	data := []byte{'['}
	jsonExport := NewJSON(2, func(b []byte, c []column.Column) {
		data = append(data, b...)
	})
	jsonExport.ReadCompact(selectStmt)
	data = append(data, ']')
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()
	var exportData interface{}
	err = json.Unmarshal(data, &exportData)
	assert.NoError(t, err)
}

func TestJSONEachRow(t *testing.T) {
	var columnTypes []string
	for _, dataType := range mainDataTypes {
		columnTypes = append(columnTypes, dataType.Name+" "+dataType.ChType,
			dataType.Name+"_nullable Nullable("+dataType.ChType+")",
			dataType.Name+"_array Array("+dataType.ChType+")",
			dataType.Name+"_array_nullable Array(Nullable("+dataType.ChType+"))")
	}
	columnTypes = append(columnTypes, "tuple Tuple(String,Nullable(String))")
	createTable := `CREATE TABLE 
						chconn_json_compact_export_example
						(` + strings.Join(columnTypes, ",\n") + `)
						ENGINE = GenerateRandom(1, 5, 3)`

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS chconn_json_compact_export_example`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), createTable)
	require.NoError(t, err)
	require.Nil(t, res)

	selectStmt, err := conn.Select(context.Background(), `SELECT *,
				toLowCardinality(string),toLowCardinality(string_nullable),
				toLowCardinality(fixedString),toLowCardinality(fixedString_nullable)
				 FROM chconn_json_compact_export_example limit 200`)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())
	jsonExport := NewJSON(2, func(b []byte, c []column.Column) {
		var exportData interface{}
		err = json.Unmarshal(b, &exportData)
		assert.NoError(t, err)
	})
	jsonExport.ReadEachRow(selectStmt)
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()
}
