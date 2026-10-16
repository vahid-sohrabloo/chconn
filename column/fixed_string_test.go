package column_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func pad4(s string) string {
	return s + strings.Repeat("\x00", 4-len(s))
}

func TestFixedStringColumn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn, err := chconn.Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Exec(ctx, `DROP TABLE IF EXISTS test_fixed_string_column`))
	require.NoError(t, conn.ExecWithOption(ctx, `CREATE TABLE test_fixed_string_column (
		v FixedString(4),
		v_nullable Nullable(FixedString(4)),
		v_array Array(FixedString(4)),
		v_array_nullable Array(Nullable(FixedString(4))),
		v_lc LowCardinality(FixedString(4)),
		v_lc_nullable LowCardinality(Nullable(FixedString(4))),
		v_array_lc Array(LowCardinality(FixedString(4)))
	) Engine=Memory`, &chconn.QueryOptions{
		Settings: chconn.Settings{{Name: "allow_suspicious_low_cardinality_types", Value: "true", Important: true}},
	}))

	col := column.NewFixedString(4)
	colNullable := column.NewFixedString(4).Nullable()
	colArray := column.NewFixedString(4).Array()
	colArrayNullable := column.NewFixedString(4).Nullable().Array()
	colLC := column.NewFixedString(4).LowCardinality()
	colLCNullable := column.NewFixedString(4).LowCardinality().Nullable()
	colArrayLC := column.NewFixedString(4).LowCardinality().Array()

	values := []string{"ab", "abcd", "", "wxyz", "a"}
	for i, v := range values {
		col.Append(v)
		if i%2 == 0 {
			colNullable.AppendNil()
			colLCNullable.AppendNil()
		} else {
			colNullable.Append(v)
			colLCNullable.Append(v)
		}
		colArray.Append([]string{v, "zz"})
		colArrayNullable.AppendP([]*string{&v, nil})
		colLC.Append(v)
		colArrayLC.Append([]string{v, v})
	}
	require.NoError(t, conn.Insert(ctx, `INSERT INTO test_fixed_string_column VALUES`,
		col, colNullable, colArray, colArrayNullable, colLC, colLCNullable, colArrayLC))

	t.Run("typed select", func(t *testing.T) {
		readCol := column.NewFixedString(4)
		readNullable := column.NewFixedString(4).Nullable()
		readArray := column.NewFixedString(4).Array()
		readArrayNullable := column.NewFixedString(4).Nullable().Array()
		readLC := column.NewFixedString(4).LowCardinality()
		readLCNullable := column.NewFixedString(4).LowCardinality().Nullable()
		readArrayLC := column.NewFixedString(4).LowCardinality().Array()
		stmt, err := conn.Select(ctx, `SELECT v, v_nullable, v_array, v_array_nullable, v_lc, v_lc_nullable, v_array_lc
			FROM test_fixed_string_column`,
			readCol, readNullable, readArray, readArrayNullable, readLC, readLCNullable, readArrayLC)
		require.NoError(t, err)
		var got []string
		var gotNullable, gotLCNullable []*string
		var gotArray, gotArrayLC [][]string
		var gotArrayNullable [][]*string
		var gotLC []string
		for stmt.Next() {
			got = readCol.Read(got)
			gotNullable = readNullable.ReadP(gotNullable)
			gotArray = readArray.Read(gotArray)
			gotArrayNullable = readArrayNullable.ReadP(gotArrayNullable)
			gotLC = readLC.Read(gotLC)
			gotLCNullable = readLCNullable.ReadP(gotLCNullable)
			gotArrayLC = readArrayLC.Read(gotArrayLC)
		}
		require.NoError(t, stmt.Err())

		require.Len(t, got, len(values))
		for i, v := range values {
			want := pad4(v)
			assert.Equal(t, want, got[i])
			assert.Equal(t, want, gotLC[i])
			assert.Equal(t, []string{want, pad4("zz")}, gotArray[i])
			assert.Equal(t, []string{want, want}, gotArrayLC[i])
			require.Len(t, gotArrayNullable[i], 2)
			assert.Equal(t, want, *gotArrayNullable[i][0])
			assert.Nil(t, gotArrayNullable[i][1])
			if i%2 == 0 {
				assert.Nil(t, gotNullable[i])
				assert.Nil(t, gotLCNullable[i])
			} else {
				require.NotNil(t, gotNullable[i])
				assert.Equal(t, want, *gotNullable[i])
				require.NotNil(t, gotLCNullable[i])
				assert.Equal(t, want, *gotLCNullable[i])
			}
		}
	})

	t.Run("dynamic scan", func(t *testing.T) {
		rows, err := conn.Query(ctx, `SELECT v, v, v, v_nullable FROM test_fixed_string_column`)
		require.NoError(t, err)
		i := 0
		for rows.Next() {
			var s string
			var arr [4]byte
			var b []byte
			var p *string
			require.NoError(t, rows.Scan(&s, &arr, &b, &p))
			want := pad4(values[i])
			assert.Equal(t, want, s)
			assert.Equal(t, want, string(arr[:]))
			assert.Equal(t, []byte(want), b)
			if i%2 == 0 {
				assert.Nil(t, p)
			} else {
				require.NotNil(t, p)
				assert.Equal(t, want, *p)
			}
			i++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, len(values), i)
	})
}

func TestFixedStringTooLong(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn, err := chconn.Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Exec(ctx, `DROP TABLE IF EXISTS test_fixed_string_too_long`))
	require.NoError(t, conn.Exec(ctx, `CREATE TABLE test_fixed_string_too_long (v FixedString(2)) Engine=Memory`))

	col := column.NewFixedString(2)
	col.Append("ok")
	col.Append("toolong")
	err = conn.Insert(ctx, `INSERT INTO test_fixed_string_too_long VALUES`, col)
	require.ErrorContains(t, err, "value of 7 bytes does not fit FixedString(2) at row 1")

	// A failed insert leaves the connection unusable, so count on a new one.
	conn2, err := chconn.Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer conn2.Close()
	var n uint64
	require.NoError(t, conn2.QueryRow(ctx, `SELECT count() FROM test_fixed_string_too_long`).Scan(&n))
	assert.Zero(t, n)
}

func TestFixedStringSparse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn, err := chconn.Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Exec(ctx, `DROP TABLE IF EXISTS test_fixed_string_sparse`))
	require.NoError(t, conn.Exec(ctx, `CREATE TABLE test_fixed_string_sparse (id UInt32, v FixedString(3))
		Engine=MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5`))

	const rows = 1000
	id := column.New[uint32]()
	v := column.NewFixedString(3)
	for i := range rows {
		id.Append(uint32(i))
		if i%100 == 0 {
			v.Append(fmt.Sprintf("%03d", i/100))
		} else {
			v.Append("")
		}
	}
	require.NoError(t, conn.Insert(ctx, `INSERT INTO test_fixed_string_sparse VALUES`, id, v))

	var kind string
	require.NoError(t, conn.QueryRow(ctx, `SELECT serialization_kind FROM system.parts_columns
		WHERE database = currentDatabase() AND table = 'test_fixed_string_sparse' AND column = 'v' AND active
		LIMIT 1`).Scan(&kind))
	require.Equal(t, "Sparse", kind, "the test needs a sparse column")

	readID := column.New[uint32]()
	readV := column.NewFixedString(3)
	stmt, err := conn.Select(ctx, `SELECT id, v FROM test_fixed_string_sparse ORDER BY id`, readID, readV)
	require.NoError(t, err)
	var gotID []uint32
	var gotV []string
	for stmt.Next() {
		gotID = readID.Read(gotID)
		gotV = readV.Read(gotV)
	}
	require.NoError(t, stmt.Err())
	require.Len(t, gotV, rows)
	for i := range rows {
		require.Equal(t, uint32(i), gotID[i])
		if i%100 == 0 {
			assert.Equal(t, fmt.Sprintf("%03d", i/100), gotV[i])
		} else {
			assert.Equal(t, "\x00\x00\x00", gotV[i])
		}
	}
}
