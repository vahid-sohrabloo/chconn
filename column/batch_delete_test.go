package column

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3/types"
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

func TestArrayAppendAnyRollbackKeepsNestedOffset(t *testing.T) {
	a := NewString().Array().Array()
	require.Error(t, a.AppendAny([]any{[]string{"x"}, 42}))
	a.Append([][]string{{"y"}})
	require.NoError(t, a.ValidateInsert())
	assert.Equal(t, [][][]string{{{"y"}}}, a.Data())
}

func TestArrayAndMapRemoveDeleteKeepOffset(t *testing.T) {
	for name, change := range map[string]func(c ColumnCore){
		"Remove": func(c ColumnCore) { c.Remove(1) },
		"Delete": func(c ColumnCore) { c.Delete(1, 2) },
	} {
		t.Run(name, func(t *testing.T) {
			a := NewString().Array()
			a.Append([]string{"a"})
			a.Append([]string{"b", "c"})
			change(a)
			a.Append([]string{"d"})
			require.NoError(t, a.ValidateInsert())
			assert.Equal(t, [][]string{{"a"}, {"d"}}, a.Data())

			m := NewString().Map(New[int64]())
			m.Append(map[string]int64{"a": 1})
			m.Append(map[string]int64{"b": 2, "c": 3})
			change(m)
			m.Append(map[string]int64{"d": 4})
			require.NoError(t, m.ValidateInsert())
			assert.Equal(t, []map[string]int64{{"a": 1}, {"d": 4}}, m.Data())
		})
	}
}

func TestNullableDeleteFuncCallsPredicateOncePerRow(t *testing.T) {
	type nullableCol interface {
		NumRow() int
		RowIsNil(int) bool
		DeleteFunc(func(int) bool)
		AppendNil()
	}
	fs := NewFixedString(1).Nullable()
	fs.Append("a")
	fs.AppendNil()
	fs.Append("b")
	base := New[int32]().Nullable()
	base.Append(1)
	base.AppendNil()
	base.Append(2)
	str := NewString().Nullable()
	str.Append("a")
	str.AppendNil()
	str.Append("b")
	date := NewDate[types.Date]().Nullable()
	d1, d2 := time.Unix(86400, 0).UTC(), time.Unix(2*86400, 0).UTC()
	date.Append(d1)
	date.AppendNil()
	date.Append(d2)

	for name, c := range map[string]nullableCol{"FixedString": fs, "Base": base, "String": str, "Date": date} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			c.DeleteFunc(func(row int) bool {
				calls++
				return c.RowIsNil(row)
			})
			assert.Equal(t, 3, calls)
			require.Equal(t, 2, c.NumRow())
			assert.False(t, c.RowIsNil(0))
			assert.False(t, c.RowIsNil(1))
		})
	}
	assert.Equal(t, []string{"a", "b"}, fs.Data())
	assert.Equal(t, []int32{1, 2}, base.Data())
	assert.Equal(t, []string{"a", "b"}, str.Data())
	assert.True(t, date.Row(0).Equal(d1))
	assert.True(t, date.Row(1).Equal(d2))
}

func TestTupleAndJSONDeleteFuncCallsPredicateOncePerRow(t *testing.T) {
	strs := NewString()
	tup := NewTuple(strs, New[int64]())
	for i, v := range []string{"a", "x", "b", "c"} {
		require.NoError(t, tup.AppendAny([]any{v, int64(i)}))
	}
	calls := 0
	tup.DeleteFunc(func(row int) bool {
		calls++
		return strs.Row(row) == "x"
	})
	assert.Equal(t, 4, calls)
	require.Equal(t, 3, tup.NumRow())
	assert.Equal(t, []any{[]any{"a", int64(0)}, []any{"b", int64(2)}, []any{"c", int64(3)}},
		[]any{tup.RowAny(0), tup.RowAny(1), tup.RowAny(2)})

	js := NewJSON()
	js.Append(`{"a":1}`)
	js.Append(`{"a":2}`)
	js.Append(`{"a":3}`)
	calls = 0
	js.DeleteFunc(func(row int) bool {
		calls++
		return row == 1
	})
	assert.Equal(t, 3, calls)
	require.Equal(t, 2, js.NumRow())
}

func TestMapCanAppendHasNoSideEffect(t *testing.T) {
	col, err := ColumnByType([]byte("Variant(Map(String, Int64), String)"), 0, false, false, "UTC")
	require.NoError(t, err)
	v := col.(*Variant)
	require.NoError(t, v.AppendAny(map[string]int64{"a": 1}))
	require.Equal(t, 1, v.NumRow())
	for _, m := range v.Columns() {
		if m.FullType() == "Map(String, Int64)" {
			assert.Equal(t, 1, m.NumRow())
		}
	}
	require.NoError(t, v.ValidateInsert())
}

func TestMapAppendAnyRollsBackOnError(t *testing.T) {
	m := NewMapBase(NewString(), New[int64]())
	require.NoError(t, m.AppendAny(map[string]int64{"a": 1}))
	require.Error(t, m.AppendAny(map[string]any{"b": "not an int"}))
	require.Equal(t, 1, m.NumRow())
	require.NoError(t, m.ValidateInsert())

	typed := NewString().Map(New[int64]())
	require.NoError(t, typed.AppendAny(map[string]any{"a": int64(1)}))
	require.Error(t, typed.AppendAny(map[any]any{"b": "not an int"}))
	require.NoError(t, typed.AppendAny(map[any]any{"c": int64(3)}))
	require.NoError(t, typed.ValidateInsert())
	assert.Equal(t, []map[string]int64{{"a": 1}, {"c": 3}}, typed.Data())
}

func TestArrayOfMapCanAppendInVariant(t *testing.T) {
	arr := NewString().Map(New[int64]()).Array()
	assert.False(t, arr.canAppend([]map[string]int32{{"a": 1}}))
	v := NewVariant(arr, NewString())
	require.NoError(t, v.AppendAny([]map[string]any{{"a": int64(1)}}))
	require.NoError(t, v.ValidateInsert())
	assert.Equal(t, []map[string]int64{{"a": 1}}, arr.Row(0))
}

func TestDynamicMembersDeleteFuncKeepRowsInSync(t *testing.T) {
	j := NewJSON()
	for i := range 3 {
		require.NoError(t, j.AppendAny(map[string]any{"a": int64(i)}))
	}
	j.DeleteFunc(func(r int) bool { return r == 1 })
	require.Equal(t, 2, j.NumRow())
	require.NoError(t, j.ValidateInsert())

	empty := NewJSON()
	for range 3 {
		require.NoError(t, empty.AppendAny(map[string]any{}))
	}
	empty.DeleteFunc(func(int) bool { return true })
	assert.Zero(t, empty.NumRow())

	dyn := NewDynamic()
	tup := NewTuple(NewString(), dyn)
	require.NoError(t, tup.AppendAny([]any{"a", int64(1)}))
	require.NoError(t, tup.AppendAny([]any{"b", "x"}))
	tup.DeleteFunc(func(r int) bool { return r == 0 })
	require.Equal(t, 1, tup.NumRow())
	assert.Equal(t, 1, dyn.NumRow())
	require.NoError(t, tup.ValidateInsert())

	arr := NewDynamic().Array()
	require.NoError(t, arr.AppendAny([]any{int64(1), "x"}))
	require.NoError(t, arr.AppendAny([]any{int64(2)}))
	arr.DeleteFunc(func(r int) bool { return r == 0 })
	require.Equal(t, 1, arr.NumRow())
	require.NoError(t, arr.ValidateInsert())
}

func TestArrayCanAppendEmptySlices(t *testing.T) {
	fs4Array := func() ColumnCore { return NewFixedString(4).Array() }
	for _, tc := range []struct {
		name  string
		col   func() ColumnCore
		value any
		want  bool
	}{
		{"Array(Tuple) [][]any{}", func() ColumnCore { return NewTuple(NewString(), New[int64]()).Array() }, [][]any{}, true},
		{"Array(Array(Tuple)) [][][]any{}", func() ColumnCore { return NewTuple(NewString(), New[int64]()).Array().Array() }, [][][]any{}, true},
		{"Array(Tuple) [][]string{}", func() ColumnCore { return NewTuple(NewString(), NewString()).Array() }, [][]string{}, true},
		{"Array(Nothing) []any{}", func() ColumnCore { return NewArrayBase(NewNothing()) }, []any{}, true},
		{"Array(Array(String)) [][]any{}", func() ColumnCore { return NewString().Array().Array() }, [][]any{}, true},
		{"Array(String) []any{}", func() ColumnCore { return NewString().Array() }, []any{}, true},
		{"Array(FixedString(4)) []string{}", fs4Array, []string{}, false},
		{"Array(FixedString(4)) []int{}", fs4Array, []int{}, false},
		{"Array(FixedString(4)) [][4]byte{}", fs4Array, [][4]byte{}, true},
		{"Array(Array(FixedString(4))) [][]string{}", func() ColumnCore { return NewFixedString(4).Array().Array() }, [][]string{}, false},
	} {
		assert.Equal(t, tc.want, tc.col().canAppend(tc.value), tc.name)
	}

	v := NewVariant(NewTuple(NewString(), New[int64]()).Array(), NewString())
	require.NoError(t, v.AppendAny([][]any{}))
	require.NoError(t, v.ValidateInsert())
}

type lcInt32 int32

func TestLowCardinalityNullableAppendAnyDereferencesPointer(t *testing.T) {
	c := New[int32]().LowCardinality().Nullable()
	i := lcInt32(5)
	require.True(t, c.canAppend(&i))
	require.NoError(t, c.AppendAny(&i))
	require.NoError(t, c.AppendAny((*lcInt32)(nil)))
	require.Equal(t, 2, c.NumRow())
	assert.False(t, c.RowIsNil(0))
	assert.True(t, c.RowIsNil(1))
}

func TestMapCanAppendChecksMapAnyAny(t *testing.T) {
	ints := NewString().Map(New[int64]())
	strs := NewString().Map(NewString())
	v := NewVariant(ints, strs)
	require.NoError(t, v.AppendAny(map[any]any{"a": "x"}))
	assert.Zero(t, ints.NumRow())
	assert.Equal(t, 1, strs.NumRow())
	require.NoError(t, v.ValidateInsert())
}
