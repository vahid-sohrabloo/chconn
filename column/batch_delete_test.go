package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVariantBatchDeleteKeepsMemberRows(t *testing.T) {
	col, err := ColumnByType([]byte("Array(Variant(FixedString(2), Int64))"), 0, false, false, "UTC")
	require.NoError(t, err)
	a := col.(*Array[any])
	require.NoError(t, a.AppendAny([]any{[2]byte{'a', 'a'}, int64(1)}))
	require.NoError(t, a.AppendAny([]any{[2]byte{'b', 'b'}, nil}))
	require.NoError(t, a.AppendAny([]any{int64(3)}))

	a.DeleteFunc(func(row int) bool { return row == 0 })
	require.NoError(t, a.ValidateInsert())
	assert.Equal(t, []any{[]any{"bb", nil}, []any{int64(3)}}, []any{a.RowAny(0), a.RowAny(1)})

	a.DeleteFunc(func(int) bool { return true })
	assert.Zero(t, a.NumRow())
	require.NoError(t, a.ValidateInsert())
}

func TestArrayDeleteAllEmptiesData(t *testing.T) {
	ints := New[int64]().Array()
	ints.Append([]int64{1, 2})
	ints.DeleteFunc(func(int) bool { return true })
	assert.Zero(t, ints.NumRow())
	require.NoError(t, ints.ValidateInsert())

	strs := NewString().Nullable().Array()
	strs.Append([]string{"a"})
	strs.DeleteFunc(func(int) bool { return true })
	require.NoError(t, strs.ValidateInsert())

	lc := NewString().LowCardinality().Array()
	lc.Append([]string{"a", "b"})
	lc.DeleteFunc(func(int) bool { return true })
	require.NoError(t, lc.ValidateInsert())
}

func TestArrayAppendAnySliceOfAnyRollsBack(t *testing.T) {
	a := NewString().Array()
	require.NoError(t, a.AppendAny([]any{"x", "y"}))
	require.Error(t, a.AppendAny([]any{"x", 5}))
	require.Equal(t, 1, a.NumRow())
	require.NoError(t, a.ValidateInsert())
	assert.Equal(t, []string{"x", "y"}, a.Row(0))
}
