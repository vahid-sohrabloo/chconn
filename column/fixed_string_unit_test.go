package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFixedStringRowBookkeeping(t *testing.T) {
	c := NewFixedString(3)
	c.AppendMulti("a", "bb", "ccc", "d", "ee")
	assert.Equal(t, []string{"a\x00\x00", "bb\x00", "ccc", "d\x00\x00", "ee\x00"}, c.Data())

	c.Remove(4)
	assert.Equal(t, []string{"a\x00\x00", "bb\x00", "ccc", "d\x00\x00"}, c.Data())

	c.Delete(1, 2)
	assert.Equal(t, []string{"a\x00\x00", "ccc", "d\x00\x00"}, c.Data())

	c.DeleteFunc(func(row int) bool { return row == 0 })
	assert.Equal(t, []string{"ccc", "d\x00\x00"}, c.Data())

	c.AppendMulti("x", "y")
	c.startBatchDelete()
	c.batchDeleteKeep(0, 1)
	c.batchDeleteKeep(3, 4)
	c.endBatchDelete()
	assert.Equal(t, []string{"ccc", "y\x00\x00"}, c.Data())

	c.startBatchDelete()
	c.endBatchDelete()
	assert.Zero(t, c.NumRow())

	n := NewFixedString(2).Nullable()
	n.Append("a")
	n.AppendNil()
	n.Append("bc")
	n.DeleteFunc(func(row int) bool { return row == 0 })
	require.Equal(t, 2, n.NumRow())
	assert.True(t, n.RowIsNil(0))
	assert.Equal(t, "bc", *n.RowP(1))
}

func TestFixedStringTooLongInsideWrappers(t *testing.T) {
	plain := NewFixedString(2)
	plain.Append("toolong")
	require.ErrorContains(t, plain.ValidateInsert(), "does not fit FixedString(2)")
	assert.Equal(t, "to", plain.Row(0))
	plain.Reset()
	require.NoError(t, plain.ValidateInsert())

	array := NewFixedString(2).Array()
	require.NoError(t, array.AppendAny([]string{"toolong"}))
	require.ErrorContains(t, array.ValidateInsert(), "does not fit FixedString(2)")

	arrayNullable := NewFixedString(2).Nullable().Array()
	arrayNullable.Append([]string{"toolong"})
	require.ErrorContains(t, arrayNullable.ValidateInsert(), "does not fit FixedString(2)")

	lc := NewFixedString(2).LowCardinality()
	lc.Append("toolong")
	require.ErrorContains(t, lc.ValidateInsert(), "does not fit FixedString(2)")

	arrayLC := NewFixedString(2).LowCardinality().Array()
	arrayLC.Append([]string{"toolong"})
	require.ErrorContains(t, arrayLC.ValidateInsert(), "does not fit FixedString(2)")
}

func TestFixedStringReadFromBytes(t *testing.T) {
	data := []byte("aaaabbbb")
	c := NewFixedString(4)
	n, err := c.ReadFromBytes(2, data)
	require.NoError(t, err)
	assert.Equal(t, 8, n)
	assert.Equal(t, []string{"aaaa", "bbbb"}, c.Data())

	// Writing after Reset must not touch the caller's buffer.
	c.Reset()
	c.Append("zzzz")
	assert.Equal(t, "aaaabbbb", string(data))

	_, err = c.ReadFromBytes(3, data)
	require.Error(t, err)
	_, err = NewFixedString(1<<40).ReadFromBytes(1<<40, data)
	require.Error(t, err)
	_, err = c.ReadFromBytes(-1, data)
	require.Error(t, err)
}

func TestFixedStringAppendAny(t *testing.T) {
	c := NewFixedString(4)
	assert.True(t, c.canAppend([4]byte{}))
	assert.False(t, c.canAppend([2]byte{}), "a different size must not be accepted")
	assert.False(t, c.canAppend("ab"), "Go strings go to a String member of a Variant")
	assert.False(t, c.canAppend(&[4]byte{}))

	s := "ab"
	require.NoError(t, c.AppendAny("ab"))
	require.NoError(t, c.AppendAny([]byte("abcd")))
	require.NoError(t, c.AppendAny(&s))
	require.NoError(t, c.AppendAny([4]byte{'w', 'x', 'y', 'z'}))
	require.NoError(t, c.AppendAny(&[4]byte{'1', '2', '3', '4'}))
	assert.Equal(t, []string{"ab\x00\x00", "abcd", "ab\x00\x00", "wxyz", "1234"}, c.Data())

	require.Error(t, c.AppendAny([2]byte{'a', 'b'}))
	require.Error(t, c.AppendAny((*string)(nil)))
	require.Error(t, c.AppendAny((*[4]byte)(nil)))
	require.Error(t, c.AppendAny(42))
	assert.Equal(t, 5, c.NumRow())

	n := NewFixedString(4).Nullable()
	assert.True(t, n.canAppend(nil))
	assert.True(t, n.canAppend((*[4]byte)(nil)))
	assert.True(t, n.canAppend(&[4]byte{}))
	assert.False(t, n.canAppend("ab"))
	require.NoError(t, n.AppendAny((*[4]byte)(nil)))
	require.NoError(t, n.AppendAny(&[4]byte{'a'}))
	require.Error(t, n.AppendAny([2]byte{}))
	require.Equal(t, 2, n.NumRow())
	assert.True(t, n.RowIsNil(0))
	assert.Equal(t, "a\x00\x00\x00", *n.RowP(1))

	var got any
	require.NoError(t, n.Scan(0, &got))
	assert.Equal(t, "\x00\x00\x00\x00", got)
}

func TestFixedStringVariantSelection(t *testing.T) {
	col, err := ColumnByType([]byte("Variant(FixedString(4), String)"), 0, false, false, "UTC")
	require.NoError(t, err)
	v := col.(*Variant)
	require.NoError(t, v.AppendAny("hello world"))
	require.NoError(t, v.AppendAny([4]byte{'a', 'b', 'c', 'd'}))

	rows := map[string]int{}
	for _, m := range v.Columns() {
		rows[m.FullType()] = m.NumRow()
	}
	assert.Equal(t, 1, rows["String"], "a Go string goes to the String member")
	assert.Equal(t, 1, rows["FixedString(4)"], "a [4]byte goes to the FixedString(4) member")
}

func TestArrayAppendAnyRollsBackOnError(t *testing.T) {
	a := NewString().Array()
	s := "x"
	require.Error(t, a.AppendAny([]*string{&s, nil}))
	assert.Zero(t, a.NumRow())
	require.NoError(t, a.ValidateInsert())

	b := New[[2]byte]().Array()
	assert.False(t, New[[2]byte]().canAppend([3]byte{}))
	require.Error(t, b.AppendAny([][3]byte{{1, 2, 3}}), "must return an error, not panic")
	require.NoError(t, b.ValidateInsert())
}

func TestNewFixedStringRejectsBadSize(t *testing.T) {
	assert.Panics(t, func() { NewFixedString(0) })
}

func TestFixedStringWrappersDoNotTakeGoStringsInVariant(t *testing.T) {
	for _, tc := range []struct {
		chType string
		value  any
		want   string
	}{
		{"Variant(Array(Nullable(FixedString(4))), Array(Nullable(String)))", []string{"hello"}, "Array(Nullable(String))"},
		{"Variant(Array(Nullable(FixedString(4))), Array(Nullable(String)))", []*string{nil}, "Array(Nullable(String))"},
		{"Variant(LowCardinality(FixedString(4)), String)", "hello", "String"},
		{"Variant(Array(LowCardinality(FixedString(4))), Array(String))", []string{"hello"}, "Array(String)"},
		{
			"Variant(Array(Array(Nullable(FixedString(4)))), Array(Array(Nullable(String))))",
			[][]string{{"hello"}}, "Array(Array(Nullable(String)))",
		},
		{"Variant(Array(FixedString(4)), Array(String))", []string{}, "Array(String)"},
	} {
		t.Run(tc.chType, func(t *testing.T) {
			col, err := ColumnByType([]byte(tc.chType), 0, false, false, "UTC")
			require.NoError(t, err)
			v := col.(*Variant)
			require.NoError(t, v.AppendAny(tc.value))
			for _, m := range v.Columns() {
				want := 0
				if m.FullType() == tc.want {
					want = 1
				}
				assert.Equal(t, want, m.NumRow(), m.FullType())
			}
			require.NoError(t, v.ValidateInsert())
		})
	}
}

func TestFixedStringNeverWritesIntoReadFromBytesBuffer(t *testing.T) {
	for name, change := range map[string]func(c *FixedString){
		"DeleteFunc":                func(c *FixedString) { c.DeleteFunc(func(row int) bool { return row == 0 }) },
		"Delete":                    func(c *FixedString) { c.Delete(0, 1) },
		"Remove+Append":             func(c *FixedString) { c.Remove(0); c.Append("zzzz") },
		"SetWriteBufferSize+Append": func(c *FixedString) { c.SetWriteBufferSize(1); c.Remove(0); c.Append("zzzz") },
		"batch delete":              func(c *FixedString) { c.startBatchDelete(); c.batchDeleteKeep(1, 2); c.endBatchDelete() },
	} {
		t.Run(name, func(t *testing.T) {
			data := []byte("aaaabbbb")
			c := NewFixedString(4)
			_, err := c.ReadFromBytes(2, data)
			require.NoError(t, err)
			change(c)
			assert.Equal(t, "aaaabbbb", string(data))
		})
	}
}
