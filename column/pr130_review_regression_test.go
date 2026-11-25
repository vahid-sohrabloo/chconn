package column

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

func TestLCNullableSetWriteBufferSizeAfterRead(t *testing.T) {
	src, dst := NewLCNullable(NewString()), NewLCNullable(NewString())
	src.AppendNil()
	src.AppendMulti("", "a")
	issue128Roundtrip(t, src, dst)

	dst.SetWriteBufferSize(cap(dst.keys) + 8)
	dst.Append("a")
	dst.AppendNil()
	assert.Equal(t, []string{"", "a"}, dst.Dicts())
	assert.Equal(t, "a", dst.Row(0))
	require.NotNil(t, dst.RowP(0))
	assert.Equal(t, "a", *dst.RowP(0))
	assert.Nil(t, dst.RowP(1))
}

func TestEmptyStringPointerContainersCanAppend(t *testing.T) {
	assert.True(t, NewString().Array().canAppend([]*string{}))
	assert.True(t, NewString().Array().Array().canAppend([][]*string{}))
	assert.True(t, NewMap[string, string](NewString(), NewString()).canAppend(map[string]*string{}))
	assert.False(t, NewString().canAppend((*string)(nil)))

	v := NewVariant(NewString().Array(), New[int64]())
	require.NoError(t, v.AppendAny([]*string{}))
	assert.Equal(t, 1, v.NumRow())
}

func TestDynamicReadFailedAppendKeepsTypes(t *testing.T) {
	src := NewDynamic()
	src.AppendMulti(int64(1), "two")
	dst := NewDynamic()
	roundTripDynamicJSON(t, src, dst)
	types := func() []string {
		var out []string
		for _, col := range dst.variant.columns {
			out = append(out, col.FullType())
		}
		return out
	}
	before := types()

	require.Error(t, dst.AppendAny((*float64)(nil)))
	assert.Equal(t, before, types())
	assert.Equal(t, 2, dst.NumRow())

	require.NoError(t, dst.AppendAny(float64(2.5)))
	assert.Equal(t, []any{int64(1), "two", float64(2.5)}, dst.Data())
	out := NewDynamic()
	roundTripDynamicJSON(t, dst, out)
	assert.Equal(t, []any{int64(1), "two", float64(2.5)}, out.Data())
}

func TestDynamicFailedReadLeavesReadMode(t *testing.T) {
	src := NewDynamic()
	src.AppendMulti(int64(1), "two")
	dst := NewDynamic()
	roundTripDynamicJSON(t, src, dst)
	require.True(t, dst.readMode)

	header := readerwriter.NewWriter()
	src.HeaderWriter(header)
	var buf bytes.Buffer
	_, err := header.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, dst.ReadHeader(readerwriter.NewReader(&buf), &shared.ServerInfo{Timezone: "UTC"}))
	require.Error(t, dst.ReadRaw(src.NumRow()))
	assert.False(t, dst.readMode)
}

func TestArrayNullableReadAppendCache(t *testing.T) {
	a, b, c := "a", "b", "c"
	src, dst := NewString().Nullable().Array(), NewString().Nullable().Array()
	src.AppendP([]*string{&a, nil})
	issue128Roundtrip(t, src, dst)
	assert.Equal(t, [][]*string{{&a, nil}}, dst.DataP())
	dst.AppendP([]*string{&b})
	assert.Equal(t, [][]*string{{&a, nil}, {&b}}, dst.DataP())
	assert.Equal(t, []*string{&b}, dst.RowP(1))

	src2, dst2 := NewString().Nullable().Array().Array(), NewString().Nullable().Array().Array()
	src2.AppendP([][]*string{{&a}, {nil}})
	issue128Roundtrip(t, src2, dst2)
	assert.Equal(t, [][][]*string{{{&a}, {nil}}}, dst2.DataP())
	dst2.AppendP([][]*string{{&c}})
	assert.Equal(t, [][][]*string{{{&a}, {nil}}, {{&c}}}, dst2.DataP())
	assert.Equal(t, [][]*string{{&c}}, dst2.RowP(1))

	src3, dst3 := NewString().Nullable().Array().Array().Array(), NewString().Nullable().Array().Array().Array()
	src3.AppendP([][][]*string{{{&a}}})
	issue128Roundtrip(t, src3, dst3)
	assert.Equal(t, [][][][]*string{{{{&a}}}}, dst3.DataP())
	dst3.AppendP([][][]*string{{{&b, nil}}})
	assert.Equal(t, [][][][]*string{{{{&a}}}, {{{&b, nil}}}}, dst3.DataP())
	assert.Equal(t, [][][]*string{{{&b, nil}}}, dst3.RowP(1))
}

func TestStringNullableReadAppendDataP(t *testing.T) {
	a, b := "a", "b"
	src, dst := NewString().Nullable(), NewString().Nullable()
	src.AppendP(&a)
	src.AppendP(nil)
	issue128Roundtrip(t, src, dst)
	dst.AppendP(&b)
	assert.Equal(t, []*string{&a, nil, &b}, dst.DataP())
	assert.Equal(t, []*string{&a, nil, &b}, dst.ReadP(nil))
}

func TestDynamicReadAppendNoDiscriminatorLeft(t *testing.T) {
	src := NewDynamic()
	src.AppendMulti(int64(1), "two")
	dst := NewDynamic()
	roundTripDynamicJSON(t, src, dst)
	for len(dst.variant.columns) < 255 {
		dst.variant.columns = append(dst.variant.columns, New[int8]())
	}

	require.ErrorContains(t, dst.AppendAny(float64(2.5)), "no discriminator left")
	assert.Len(t, dst.variant.columns, 255)
	assert.Equal(t, []any{int64(1), "two"}, dst.Data())
}

func TestJSONTypedHeaderSettingsChange(t *testing.T) {
	col := NewJSON()
	require.NoError(t, col.SetColumnHeader(ColumnHeader{ChType: []byte("JSON(a Int64, b String, max_dynamic_paths=1)")}))
	require.NoError(t, col.SetColumnHeader(ColumnHeader{ChType: []byte("JSON(b String, a Int64, max_dynamic_paths=2)")}))
	assert.Equal(t, uint64(2), col.maxDynamicPaths)
	require.Len(t, col.typedPaths, 2)
	require.ErrorContains(t,
		col.SetColumnHeader(ColumnHeader{ChType: []byte("JSON(a Int64, max_dynamic_paths=2)")}), "cannot change type")
}

func TestVariantRemoveKeepsPositions(t *testing.T) {
	v := NewVariant(New[int64](), NewString())
	for i, val := range []any{int64(1), "a", nil, int64(2), "b", nil, int64(3), "c"} {
		require.NoError(t, v.AppendAny(val), i)
	}
	for _, n := range []int{7, 5, 3, 0} {
		v.Remove(n)
		pos := slices.Clone(v.discriminatorsIndexPos)
		counts, nils := v.discriminatorCounts, v.totalNils
		v.rebuildPositions()
		assert.Equal(t, v.discriminatorsIndexPos, pos, "Remove(%d)", n)
		assert.Equal(t, v.discriminatorCounts, counts, "Remove(%d)", n)
		assert.Equal(t, v.totalNils, nils, "Remove(%d)", n)
	}
	require.NoError(t, v.AppendAny("d"))
	assert.Equal(t, []any{"d"}, v.Data())
}
