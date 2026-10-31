package column

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/types"
)

func issue128Roundtrip(t *testing.T, src, dst ColumnCore) {
	t.Helper()
	header := readerwriter.NewWriter()
	src.HeaderWriter(header)
	var buf bytes.Buffer
	_, err := header.WriteTo(&buf)
	require.NoError(t, err)
	_, err = src.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, dst.ReadHeader(readerwriter.NewReader(&buf), nil))
	require.NoError(t, dst.ReadRaw(src.NumRow()))
}

func TestIssue128LCReadAppendRoundtrip(t *testing.T) {
	src, dst := NewLC(NewString()), NewLC(NewString())
	src.AppendMulti("old", "a", "old")
	issue128Roundtrip(t, src, dst)
	dst.AppendMulti("a", "new", "new")
	require.NoError(t, dst.AppendAny([]byte("bytes")))
	assert.Equal(t, []string{"old", "a", "old", "a", "new", "new", "bytes"}, dst.Data())
	assert.Equal(t, []string{"old", "a", "new", "bytes"}, dst.Dicts())
	issue128Roundtrip(t, dst, NewLC(NewString()))

	src.Reset()
	src.AppendMulti("next", "next")
	issue128Roundtrip(t, src, dst)
	dst.AppendMulti("old", "next", "last")
	assert.Equal(t, []string{"next", "next", "old", "next", "last"}, dst.Data())
	assert.Equal(t, []string{"next", "old", "last"}, dst.Dicts())
	assert.Len(t, dst.dict, 3)
	again := NewLC(NewString())
	issue128Roundtrip(t, dst, again)
	assert.Equal(t, dst.Data(), again.Data())

	require.NoError(t, dst.ReadRaw(0))
	dst.Append("old")
	assert.Equal(t, []string{"old"}, dst.Data())
	assert.Len(t, dst.dict, 1)
}

func TestIssue128LCNullableLocalAndReadAppend(t *testing.T) {
	src, dst := NewLCNullable(NewString()), NewLCNullable(NewString())
	value := "pointer"
	src.AppendNil()
	src.Append("")
	src.AppendMulti("a", "")
	src.AppendP(&value)
	src.AppendMultiP(nil, &value)
	require.NoError(t, src.AppendAny([]byte("bytes")))
	want := []string{"", "", "a", "", value, "", value, "bytes"}
	assert.Equal(t, want, src.Data())
	for i, v := range want {
		assert.Equal(t, v, src.Row(i))
		if i == 0 || i == 5 {
			assert.Nil(t, src.RowP(i))
			assert.Equal(t, (*string)(nil), src.RowAny(i))
		} else {
			require.NotNil(t, src.RowP(i))
			assert.Equal(t, v, *src.RowP(i))
			assert.Equal(t, &v, src.RowAny(i))
		}
	}
	issue128Roundtrip(t, src, dst)
	dst.AppendMulti("", "a", "new", "new")
	dst.AppendNil()
	assert.Equal(t, append(want, "", "a", "new", "new", ""), dst.Data())
	assert.Equal(t, []string{"", "", "a", value, "bytes", "new"}, dst.Dicts())
	assert.Equal(t, uint32(1), dst.Keys()[8], "empty string must not use the NULL key")
	again := NewLCNullable(NewString())
	issue128Roundtrip(t, dst, again)
	assert.Equal(t, dst.DataP(), again.DataP())

	src.Reset()
	src.Append("next")
	issue128Roundtrip(t, src, dst)
	dst.AppendMulti("", "a", "next")
	assert.Equal(t, []string{"next", "", "a", "next"}, dst.Data())
	assert.Len(t, dst.dict, 3)
	issue128Roundtrip(t, dst, again)
	assert.Equal(t, dst.DataP(), again.DataP())

	require.NoError(t, dst.ReadRaw(0))
	dst.AppendNil()
	dst.Append("")
	assert.Equal(t, []uint32{0, 1}, dst.Keys())
	assert.Equal(t, "", dst.Row(0))
	assert.Equal(t, "", *dst.RowP(1))
	issue128Roundtrip(t, dst, again)
	assert.Equal(t, dst.DataP(), again.DataP())
}

func TestIssue128LCNullableOnlyNullRoundtrip(t *testing.T) {
	src := NewLCNullable(NewString())
	src.AppendMultiP(nil, nil)
	dst := NewLCNullable(NewString())
	issue128Roundtrip(t, src, dst)
	assert.Equal(t, []*string{nil, nil}, dst.DataP())
	dst.Append("")
	assert.Equal(t, uint32(1), dst.Keys()[2])
	issue128Roundtrip(t, dst, src)
	assert.Equal(t, dst.DataP(), src.DataP())
}

func TestIssue128LCNullableDefersDateLocation(t *testing.T) {
	date := NewDate[types.DateTime]()
	c := NewLCNullable[time.Time](date)
	assert.Nil(t, date.loc, "reserved NULL must not resolve location before the header")
	c.SetWriteBufferSize(2)
	assert.Nil(t, date.loc)
	c.Reset()
	assert.Nil(t, date.loc)
	require.NoError(t, c.SetColumnHeader(ColumnHeader{
		ChType: []byte("LowCardinality(Nullable(DateTime('America/New_York')))"),
	}))
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	value := time.Date(2020, time.January, 1, 0, 0, 0, 0, loc)
	assert.Nil(t, date.loc)
	src := NewLCNullable[time.Time](NewDate[types.DateTime]().SetLocation(loc))
	src.Append(value)
	src.AppendNil()
	issue128Roundtrip(t, src, c)
	assert.Equal(t, "America/New_York", c.Row(0).Location().String())
	assert.True(t, c.Row(0).Equal(value))
	assert.Nil(t, c.RowP(1))
	c.Append(value)
	assert.True(t, c.Row(2).Equal(value))
}

func TestIssue128LCReadEmptyThenWideIndices(t *testing.T) {
	src, dst := NewLC(New[int64]()), NewLC(New[int64]())
	for i := range 260 {
		src.Append(int64(i))
	}
	issue128Roundtrip(t, src, dst)
	require.NoError(t, dst.ReadRaw(0))
	issue128Roundtrip(t, src, dst)
	assert.Equal(t, src.Data(), dst.Data())
	dst.Append(259)
	dst.Append(260)
	assert.Len(t, dst.Dicts(), 261)
	assert.Equal(t, int64(259), dst.Row(260))
	assert.Equal(t, int64(260), dst.Row(261))
}

func TestIssue128VariantExplicitDiscriminators(t *testing.T) {
	str := NewString()
	str.AppendMulti("a", "b")
	v := NewVariant(str)
	v.AppendDiscriminatorsMulti(0, 255, 0)
	assert.Equal(t, []any{"a", nil, "b"}, v.Data())
	require.NoError(t, v.ValidateInsert())
	dst := NewVariant(NewString())
	issue128Roundtrip(t, v, dst)
	assert.Equal(t, v.Data(), dst.Data())
	require.NoError(t, dst.ValidateInsert())
}

func TestIssue128VariantMemberReorderRoundtrip(t *testing.T) {
	str := NewString()
	v := NewVariant(str)
	str.AppendMulti("a", "b")
	v.AppendNil()
	oldIndex := str.getLocationInParent()
	v.columns = append(v.columns, New[int64]())
	v.reorderColumn()
	for i, disc := range v.discriminators.values {
		if disc == oldIndex {
			v.discriminators.values[i] = str.getLocationInParent()
		}
	}
	require.NoError(t, v.AppendAny(int64(3)))
	require.NoError(t, v.AppendAny("c"))
	v.AppendNil()
	assert.Equal(t, []any{"a", "b", nil, int64(3), "c", nil}, v.Data())
	require.NoError(t, v.ValidateInsert())
	dst := NewVariant(NewString(), New[int64]())
	issue128Roundtrip(t, v, dst)
	assert.Equal(t, v.Data(), dst.Data())
	require.NoError(t, dst.ValidateInsert())
}

func TestIssue128VariantAppendPositionsRoundtrip(t *testing.T) {
	str, ints := NewString(), New[int64]()
	src := NewVariant(str, ints)
	str.AppendMulti("a", "b")
	ints.AppendMulti(1, 2)
	src.AppendNil()
	src.AppendNil()
	assert.Equal(t, []any{"a", "b", int64(1), int64(2), nil, nil}, src.Data())
	require.NoError(t, src.ValidateInsert())

	dst := NewVariant(NewString(), New[int64]())
	issue128Roundtrip(t, src, dst)
	assert.Equal(t, src.Data(), dst.Data())
	assert.Equal(t, 2, dst.totalNils)
	require.NoError(t, dst.ValidateInsert())
	require.NoError(t, dst.AppendAny("c"))
	dst.AppendNil()
	require.NoError(t, dst.AppendAny(int64(3)))
	assert.Equal(t, []any{"a", "b", int64(1), int64(2), nil, nil, "c", nil, int64(3)}, dst.Data())
	require.NoError(t, dst.ValidateInsert())

	dst.Delete(1, 3)
	dst.DeleteFunc(func(row int) bool { return dst.RowIsNil(row) })
	require.NoError(t, dst.AppendAny("d"))
	dst.AppendNil()
	assert.Equal(t, []any{"a", int64(2), "c", int64(3), "d", nil}, dst.Data())
	require.NoError(t, dst.ValidateInsert())
	dst.Remove(3)
	require.NoError(t, dst.AppendAny(int64(4)))
	dst.AppendNil()
	assert.Equal(t, []any{"a", int64(2), "c", int64(4), nil}, dst.Data())
	require.NoError(t, dst.ValidateInsert())
	again := NewVariant(NewString(), New[int64]())
	issue128Roundtrip(t, dst, again)
	assert.Equal(t, dst.Data(), again.Data())
	require.NoError(t, again.ValidateInsert())

	again.startBatchDelete()
	again.batchDeleteKeep(1, 2)
	again.batchDeleteKeep(4, 5)
	again.endBatchDelete()
	require.NoError(t, again.AppendAny("after batch"))
	assert.Equal(t, []any{int64(2), nil, "after batch"}, again.Data())
	require.NoError(t, again.ValidateInsert())

	issue128Roundtrip(t, src, dst)
	assert.Equal(t, 2, dst.totalNils, "repeated reads must replace the nil count")
	dst.Reset()
	require.NoError(t, dst.AppendAny("reset"))
	assert.Equal(t, []any{"reset"}, dst.Data())
	require.NoError(t, dst.ValidateInsert())
	require.NoError(t, dst.ReadRaw(0))
	require.NoError(t, dst.AppendAny(int64(9)))
	assert.Equal(t, []any{int64(9)}, dst.Data())
	require.NoError(t, dst.ValidateInsert())
}

func TestIssue128StringCanAppendNilPointers(t *testing.T) {
	c := NewString()
	for _, value := range []any{(*string)(nil), (*[]byte)(nil), nil} {
		assert.False(t, c.canAppend(value))
		require.Error(t, c.AppendAny(value))
		assert.Zero(t, c.NumRow())
	}
	text, data := "a", []byte("b")
	assert.True(t, c.canAppend(&text))
	assert.True(t, c.canAppend(&data))
	require.NoError(t, c.AppendAny(&text))
	require.NoError(t, c.AppendAny(&data))
	dst := NewString()
	issue128Roundtrip(t, c, dst)
	assert.Equal(t, []string{"a", "b"}, dst.Data())

	v := NewVariant(NewString())
	require.Error(t, v.AppendAny((*string)(nil)))
	require.Error(t, v.AppendAny((*[]byte)(nil)))
	assert.Zero(t, v.NumRow())
}

func TestIssue128StringDeleteEmptyRanges(t *testing.T) {
	c := NewString()
	c.Delete(0, 0)
	c.Delete(0, 1)
	assert.Zero(t, c.NumRow())

	c.AppendMulti("", "middle", "last")
	for _, bounds := range [][2]int{{0, 0}, {1, 1}, {2, 1}, {3, 3}, {3, 10}, {5, 6}} {
		c.Delete(bounds[0], bounds[1])
		assert.Equal(t, []string{"", "middle", "last"}, c.Data())
	}
	c.Delete(1, 100)
	assert.Equal(t, []string{""}, c.Data())
	c.Append("after")
	assert.Equal(t, []string{"", "after"}, c.Data())
	dst := NewString()
	issue128Roundtrip(t, c, dst)
	assert.Equal(t, c.Data(), dst.Data())
	c.Delete(0, 100)
	c.Delete(0, 0)
	assert.Zero(t, c.NumRow())
}
