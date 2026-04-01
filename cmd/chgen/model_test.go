package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToGoName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"entity_id", "EntityId"},
		{"entityId", "EntityId"},
		{"ad_slotId", "AdSlotId"},
		{"id", "Id"},
		{"created_at", "CreatedAt"},
		{"some_field_name", "SomeFieldName"},
		{"myFieldName", "MyFieldName"},
		{"name", "Name"},
		{"source", "Source"},
		{"some.field.name", "SomeFieldName"},
		{"some-field-name", "SomeFieldName"},
		{"AdSlotId", "AdSlotId"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := toGoName(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestExtractTableName(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    string
		wantErr bool
	}{
		{
			name: "simple create table",
			sql:  "CREATE TABLE events (id UInt64) ENGINE = MergeTree()",
			want: "events",
		},
		{
			name: "with IF NOT EXISTS",
			sql:  "CREATE TABLE IF NOT EXISTS events (id UInt64) ENGINE = MergeTree()",
			want: "events",
		},
		{
			name: "with database prefix",
			sql:  "CREATE TABLE mydb.events (id UInt64) ENGINE = MergeTree()",
			want: "events",
		},
		{
			name: "lowercase create table",
			sql:  "create table my_table (col1 String) ENGINE = Memory()",
			want: "my_table",
		},
		{
			name:    "no create table",
			sql:     "SELECT 1",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractTableName(tt.sql)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGenerateModel_Snapshot(t *testing.T) {
	columns := []columnSchema{
		{Name: "id", Type: "UInt64"},
		{Name: "name", Type: "LowCardinality(String)"},
		{Name: "score", Type: "Nullable(Int64)"},
		{Name: "source", Type: "Enum8('prebid' = 1, 'server' = 2, 'client' = 3)"},
		{Name: "revenue", Type: "SimpleAggregateFunction(sum, Nullable(Int64))"},
		{Name: "created_at", Type: "DateTime"},
		{Name: "updated_at", Type: "Date32"},
		{Name: "tags", Type: "Array(LowCardinality(String))"},
		{Name: "country_code", Type: "LowCardinality(FixedString(2))"},
		{Name: "metadata", Type: "Map(String, String)"},
		{Name: "ip_addr", Type: "IPv4"},
		{Name: "col_uuid", Type: "UUID"},
		{Name: "col_float", Type: "Float64"},
		{Name: "col_bool", Type: "Bool"},
		{Name: "viewed_at", Type: "DateTime64(3)"},
	}

	outFile := filepath.Join(t.TempDir(), "model_output.go")
	cfg := modelConfig{
		out:   outFile,
		pkg:   "testpkg",
		table: "events",
	}

	err := generateModel(cfg, columns)
	require.NoError(t, err)

	got, err := os.ReadFile(outFile)
	require.NoError(t, err)

	testSnapshot(t, "testdata/model_output.go.golden", string(got))
}

func TestGenerateModel_TimeAsUint(t *testing.T) {
	columns := []columnSchema{
		{Name: "created_at", Type: "DateTime"},
		{Name: "updated_at", Type: "Date32"},
		{Name: "viewed_at", Type: "DateTime64(3)"},
		{Name: "birth_date", Type: "Date"},
	}

	outFile := filepath.Join(t.TempDir(), "model_uint.go")
	cfg := modelConfig{
		out:        outFile,
		pkg:        "testpkg",
		table:      "time_test",
		timeAsUint: true,
	}

	err := generateModel(cfg, columns)
	require.NoError(t, err)

	got, err := os.ReadFile(outFile)
	require.NoError(t, err)

	testSnapshot(t, "testdata/model_time_as_uint.go.golden", string(got))
}
