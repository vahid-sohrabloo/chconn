package chgentest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	chconn "github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/types"
)

func getTestConnection(t testing.TB) chconn.Conn {
	t.Helper()
	connStr := os.Getenv("CHX_TEST_TCP_CONN_STRING")
	if connStr == "" {
		t.Skip("CHX_TEST_TCP_CONN_STRING not set")
	}
	config, err := chconn.ParseConfig(connStr)
	require.NoError(t, err)
	c, err := chconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	return c
}

func ptr[T any](v T) *T { return &v }

const createTableSQL = `CREATE TABLE IF NOT EXISTS test_chgen (
	id UInt64,
	name String,
	score Float64,
	active Bool,
	small_num Int8,
	category LowCardinality(String),
	null_score Nullable(Int64),
	null_name Nullable(String),
	created_at DateTime,
	updated_at DateTime,
	country_code FixedString(2),
	lang_code LowCardinality(FixedString(2)),
	tags Array(String),
	metadata Map(String, String),
	status Enum8('active' = 1, 'inactive' = 2),
	uuid UUID,
	location Point,
	deleted_at Nullable(DateTime),
	optional_scores Array(Nullable(Int64)),
	tag_groups Array(Array(String)),
	null_category LowCardinality(Nullable(String)),
	optional_meta Map(String, Nullable(Int64))
) ENGINE = Memory`

func TestIntegration_InsertAndSelect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := getTestConnection(t)

	err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_chgen")
	require.NoError(t, err)
	err = conn.Exec(ctx, createTableSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_chgen")
	})

	now := time.Now().UTC().Truncate(time.Second)
	testUUID := types.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	deletedAt := now.Add(-time.Hour)
	inputFull := TestModel{
		ID:             42,
		Name:           "hello",
		Score:          3.14,
		Active:         true,
		SmallNum:       -7,
		Category:       "test",
		NullScore:      ptr(int64(123)),
		NullName:       ptr("world"),
		CreatedAt:      now,
		UpdatedAt:      uint32(now.Unix()),
		CountryCode:    [2]byte{'U', 'S'},
		LangCode:       [2]byte{'e', 'n'},
		Tags:           []string{"go", "clickhouse"},
		Metadata:       map[string]string{"key": "value"},
		Status:         TestModelStatusActive,
		UUID:           testUUID,
		Location:       types.Point{Col1: 1.5, Col2: 2.5},
		DeletedAt:      &deletedAt,
		OptionalScores: []*int64{ptr(int64(10)), nil, ptr(int64(30))},
		TagGroups:      [][]string{{"a", "b"}, {"c"}},
		NullCategory:   ptr("special"),
		OptionalMeta:   map[string]*int64{"x": ptr(int64(1)), "y": nil},
	}

	inputNullNil := TestModel{
		ID:             43,
		Name:           "nil_test",
		Score:          0,
		Active:         false,
		SmallNum:       0,
		Category:       "",
		NullScore:      nil,
		NullName:       nil,
		CreatedAt:      now,
		UpdatedAt:      uint32(now.Unix()),
		CountryCode:    [2]byte{0, 0},
		LangCode:       [2]byte{0, 0},
		Tags:           nil,
		Metadata:       nil,
		Status:         TestModelStatusInactive,
		UUID:           types.UUID{},
		Location:       types.Point{},
		DeletedAt:      nil,
		OptionalScores: nil,
		TagGroups:      nil,
		NullCategory:   nil,
		OptionalMeta:   nil,
	}

	// INSERT
	cols := NewTestModelColumns()
	cols.Write(&inputFull)
	cols.Write(&inputNullNil)
	err = conn.Insert(ctx, cols.InsertQuery("test_chgen"), cols.Columns()...)
	require.NoError(t, err)

	// SELECT
	readCols := NewTestModelColumns()
	stmt, err := conn.Select(ctx, "SELECT * FROM test_chgen ORDER BY id", readCols.Columns()...)
	require.NoError(t, err)

	var results []TestModel
	for i, err := range stmt.RowIter() {
		require.NoError(t, err)
		results = append(results, readCols.Read(i))
	}

	require.Len(t, results, 2)

	// Verify first row (non-null values)
	r := results[0]
	assert.Equal(t, uint64(42), r.ID)
	assert.Equal(t, "hello", r.Name)
	assert.InDelta(t, 3.14, r.Score, 0.001)
	assert.True(t, r.Active)
	assert.Equal(t, int8(-7), r.SmallNum)
	assert.Equal(t, "test", r.Category)
	assert.Equal(t, ptr(int64(123)), r.NullScore)
	assert.Equal(t, ptr("world"), r.NullName)
	assert.Equal(t, now, r.CreatedAt.UTC())
	assert.Equal(t, uint32(now.Unix()), r.UpdatedAt)
	assert.Equal(t, [2]byte{'U', 'S'}, r.CountryCode)
	assert.Equal(t, [2]byte{'e', 'n'}, r.LangCode)
	assert.Equal(t, []string{"go", "clickhouse"}, r.Tags)
	assert.Equal(t, map[string]string{"key": "value"}, r.Metadata)
	assert.Equal(t, TestModelStatusActive, r.Status)
	assert.Equal(t, testUUID, r.UUID)
	assert.Equal(t, types.Point{Col1: 1.5, Col2: 2.5}, r.Location)
	assert.Equal(t, deletedAt.Unix(), r.DeletedAt.Unix())
	assert.Equal(t, []*int64{ptr(int64(10)), nil, ptr(int64(30))}, r.OptionalScores)
	assert.Equal(t, [][]string{{"a", "b"}, {"c"}}, r.TagGroups)
	assert.Equal(t, ptr("special"), r.NullCategory)
	assert.Equal(t, map[string]*int64{"x": ptr(int64(1)), "y": nil}, r.OptionalMeta)

	// Verify second row (null values)
	r2 := results[1]
	assert.Equal(t, uint64(43), r2.ID)
	assert.Equal(t, "nil_test", r2.Name)
	assert.Nil(t, r2.NullScore)
	assert.Nil(t, r2.NullName)
	assert.Equal(t, TestModelStatusInactive, r2.Status)
	assert.Equal(t, types.Point{}, r2.Location)
	assert.Nil(t, r2.DeletedAt)
	assert.Nil(t, r2.NullCategory)
	// Empty arrays/maps may come back as nil from ClickHouse
	assert.True(t, len(r2.Tags) == 0, "expected empty or nil tags")
	assert.True(t, len(r2.Metadata) == 0, "expected empty or nil metadata")
	assert.True(t, len(r2.OptionalScores) == 0, "expected empty or nil optional_scores")
	assert.True(t, len(r2.TagGroups) == 0, "expected empty or nil tag_groups")
	assert.True(t, len(r2.OptionalMeta) == 0, "expected empty or nil optional_meta")
}

const createBlocksTableSQL = `CREATE TABLE IF NOT EXISTS test_chgen_blocks (
	id UInt64,
	name String,
	score Float64,
	active Bool,
	small_num Int8,
	category LowCardinality(String),
	null_score Nullable(Int64),
	null_name Nullable(String),
	created_at DateTime,
	updated_at DateTime,
	country_code FixedString(2),
	lang_code LowCardinality(FixedString(2)),
	tags Array(String),
	metadata Map(String, String),
	status Enum8('active' = 1, 'inactive' = 2),
	uuid UUID,
	location Point,
	deleted_at Nullable(DateTime),
	optional_scores Array(Nullable(Int64)),
	tag_groups Array(Array(String)),
	null_category LowCardinality(Nullable(String)),
	optional_meta Map(String, Nullable(Int64))
) ENGINE = Memory`

func TestIntegration_SelectMultipleBlocks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := getTestConnection(t)

	err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_chgen_blocks")
	require.NoError(t, err)
	err = conn.Exec(ctx, createBlocksTableSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_chgen_blocks")
	})

	const rowCount = 1000
	now := time.Now().UTC().Truncate(time.Second)

	// Insert many rows to trigger multiple blocks
	cols := NewTestModelColumns()
	cols.SetWriteBufferSize(rowCount)
	for i := range rowCount {
		var nullCat *string
		if i%3 == 0 {
			nullCat = ptr("cat")
		}
		cols.Write(&TestModel{
			ID:             uint64(i),
			Name:           "row",
			Score:          float64(i),
			Active:         i%2 == 0,
			SmallNum:       int8(i % 100),
			Category:       "bulk",
			NullScore:      ptr(int64(i)),
			NullName:       nil,
			CreatedAt:      now,
			UpdatedAt:      uint32(now.Unix()),
			CountryCode:    [2]byte{'U', 'S'},
			LangCode:       [2]byte{'e', 'n'},
			Tags:           nil,
			Metadata:       nil,
			Status:         TestModelStatusActive,
			UUID:           types.UUID{},
			Location:       types.Point{},
			DeletedAt:      nil,
			OptionalScores: nil,
			TagGroups:      nil,
			NullCategory:   nullCat,
			OptionalMeta:   nil,
		})
	}
	err = conn.Insert(ctx, cols.InsertQuery("test_chgen_blocks"), cols.Columns()...)
	require.NoError(t, err)

	// Read back and count
	readCols := NewTestModelColumns()
	stmt, err := conn.Select(ctx, "SELECT * FROM test_chgen_blocks ORDER BY id", readCols.Columns()...)
	require.NoError(t, err)

	var count int
	for _, err := range stmt.RowIter() {
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, rowCount, count)
}
