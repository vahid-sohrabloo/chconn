package chgentest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestStrictModel_SetWriteBufferSizePresizesArray(t *testing.T) {
	t.Parallel()
	cols := NewStrictModelColumns()
	cols.SetWriteBufferSize(10)
	assert.GreaterOrEqual(t, cap(cols.Nums.Column().(*column.Base[uint16]).Data()), 20)
}

func TestIntegration_StrictModel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := getTestConnection(t)

	require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_chgen_strict"))
	require.NoError(t, conn.Exec(ctx, `CREATE TABLE test_chgen_strict (
	flag UInt8,
	null_flag Nullable(UInt8),
	flags Array(UInt8),
	null_at Nullable(DateTime),
	at DateTime,
	day Date,
	nums Array(UInt16)
) ENGINE = Memory`))
	t.Cleanup(func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_chgen_strict")
	})

	full := StrictModel{
		Flag:     true,
		NullFlag: new(true),
		Flags:    []bool{true, false, true},
		NullAt:   new(uint32(1700000000)),
		At:       1700000001,
		Day:      19700,
		Nums:     []uint16{1, 2},
	}
	cols := NewStrictModelColumns()
	cols.Write(&full)
	cols.Write(&StrictModel{At: 1, Day: 1})
	require.NoError(t, conn.Insert(ctx, cols.InsertQuery("test_chgen_strict"), cols.Columns()...))

	readCols := NewStrictModelColumns()
	stmt, err := conn.Select(ctx, "SELECT "+readCols.ColumnNames()+" FROM test_chgen_strict ORDER BY at DESC", readCols.Columns()...)
	require.NoError(t, err)
	var got []StrictModel
	for i, err := range stmt.RowIter() {
		require.NoError(t, err)
		got = append(got, readCols.Read(i))
	}
	require.Len(t, got, 2)
	assert.Equal(t, full, got[0])
	assert.Equal(t, StrictModel{At: 1, Day: 1}, StrictModel{At: got[1].At, Day: got[1].Day})
	assert.False(t, got[1].Flag)
	assert.Nil(t, got[1].NullFlag)
	assert.Nil(t, got[1].NullAt)
	assert.Empty(t, got[1].Flags)
	assert.Empty(t, got[1].Nums)
}
