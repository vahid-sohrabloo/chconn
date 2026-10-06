package chgentest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvModel_NumRowsFromExtraTemplate(t *testing.T) {
	t.Parallel()
	cols := NewConvModelColumns()
	cols.Write(&ConvModel{ID: 1})
	cols.Write(&ConvModel{ID: 2})
	assert.Equal(t, 2, cols.NumRows())
}

func TestIntegration_ConvModel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := getTestConnection(t)

	require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_chgen_conv"))
	require.NoError(t, conn.Exec(ctx, `CREATE TABLE test_chgen_conv (
	id UInt64,
	flag Int8,
	code LowCardinality(String),
	uid String,
	created DateTime
) ENGINE = Memory`))
	t.Cleanup(func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_chgen_conv")
	})

	cols := NewConvModelColumns()
	cols.Write(&ConvModel{ID: 1, Flag: -1, Code: "us", UID: 7, Created: 1700000000})
	require.NoError(t, conn.Insert(ctx, cols.InsertQuery("test_chgen_conv"), cols.Columns()...))

	readCols := NewConvModelColumns()
	stmt, err := conn.Select(ctx, "SELECT id, flag, code, uid, created FROM test_chgen_conv", readCols.Columns()...)
	require.NoError(t, err)
	var got []ConvModel
	var uids []string
	var created []int64
	for i, err := range stmt.RowIter() {
		require.NoError(t, err)
		got = append(got, readCols.Read(i))
		uids = append(uids, readCols.UID.Row(i))
		created = append(created, readCols.Created.Row(i).Unix())
	}
	require.Len(t, got, 1)
	assert.Equal(t, ConvModel{ID: 1, Flag: -1, Code: "us"}, got[0])
	assert.Equal(t, []string{"uid-7"}, uids)
	assert.Equal(t, []int64{1700000000}, created)
}

func TestCastModel_Write(t *testing.T) {
	t.Parallel()
	cols := NewCastModelColumns()
	cols.Write(&CastModel{Byte: 1, Alias: 2, Opt: 3, Code: 4})
	assert.Equal(t, uint8(1), cols.Byte.Row(0))
	assert.Equal(t, uint16(2), cols.Alias.Row(0))
	assert.Equal(t, int8(3), cols.Opt.Row(0))
	assert.Equal(t, 1, cols.Code.NumRow())
}
