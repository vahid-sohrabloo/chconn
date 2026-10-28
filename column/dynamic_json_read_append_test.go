package column

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

func roundTripDynamicJSON(t *testing.T, src, dst ColumnCore) {
	t.Helper()
	chType := src.Type()
	if len(chType) == 0 {
		chType = []byte(src.FullType())
	}
	require.NoError(t, src.SetColumnHeader(ColumnHeader{ChType: chType}))
	require.NoError(t, src.ValidateInsert())
	header := readerwriter.NewWriter()
	src.HeaderWriter(header)
	var buf bytes.Buffer
	_, err := header.WriteTo(&buf)
	require.NoError(t, err)
	_, err = src.WriteTo(&buf)
	require.NoError(t, err)
	r := readerwriter.NewReader(&buf)
	require.NoError(t, dst.SetColumnHeader(ColumnHeader{ChType: chType}))
	require.NoError(t, dst.ReadHeader(r, &shared.ServerInfo{Timezone: "UTC"}))
	require.NoError(t, dst.ReadRaw(src.NumRow()))
	assert.Zero(t, buf.Len(), "all serialized data should have been consumed")
}

func TestDynamicReadAppend(t *testing.T) {
	src := NewDynamic()
	src.AppendMulti(int64(1), "two", nil, int64(3))
	dst := NewDynamic()
	roundTripDynamicJSON(t, src, dst)
	require.Equal(t, 4, dst.NumRow())
	require.NoError(t, dst.variant.ValidateInsert())
	assert.Equal(t, []any{int64(1), "two", nil, int64(3)}, dst.Data())

	require.NoError(t, dst.AppendAny(int64(9)))
	require.NoError(t, dst.AppendAny(uint8(7)))
	require.NoError(t, dst.AppendAny(float64(2.5)))
	require.NoError(t, dst.AppendAny("last"))
	require.NoError(t, dst.AppendAny(nil))
	want := []any{int64(1), "two", nil, int64(3), int64(9), uint8(7), float64(2.5), "last", nil}
	assert.Equal(t, want, dst.Data())
	require.NoError(t, dst.variant.ValidateInsert())
	out := NewDynamic()
	roundTripDynamicJSON(t, dst, out)
	assert.Equal(t, want, out.Data())

	// Writing the same read column again must not consume or rebuild its rows.
	out = NewDynamic()
	roundTripDynamicJSON(t, dst, out)
	assert.Equal(t, want, out.Data())
}

func TestDynamicReadMutations(t *testing.T) {
	for _, mutation := range []string{"delete", "deleteFunc", "remove", "reset"} {
		t.Run(mutation, func(t *testing.T) {
			src := NewDynamic()
			src.AppendMulti(int64(1), "two", nil, int64(3))
			dst := NewDynamic()
			roundTripDynamicJSON(t, src, dst)
			var want []any
			switch mutation {
			case "delete":
				dst.Delete(0, 2)
				want = []any{nil, int64(3)}
			case "deleteFunc":
				dst.DeleteFunc(func(row int) bool { return row%2 == 0 })
				want = []any{"two", int64(3)}
			case "remove":
				dst.Remove(2)
				want = []any{int64(1), "two"}
			case "reset":
				dst.Reset()
			}
			require.NoError(t, dst.AppendAny("new"))
			want = append(want, "new")
			out := NewDynamic()
			roundTripDynamicJSON(t, dst, out)
			assert.Equal(t, want, out.Data())
			require.NoError(t, out.variant.ValidateInsert())
		})
	}
}

func TestDynamicReadNestedAppend(t *testing.T) {
	t.Run("array", func(t *testing.T) {
		src := NewArray(NewDynamic())
		src.Append([]any{int64(1), "two"})
		dst := NewArray(NewDynamic())
		roundTripDynamicJSON(t, src, dst)
		assert.Equal(t, []any{int64(1), "two"}, dst.Row(0))
		dst.Delete(0, 1)
		dst.Append([]any{"new", uint8(9)})
		require.NoError(t, dst.ValidateInsert())
		out := NewArray(NewDynamic())
		roundTripDynamicJSON(t, dst, out)
		assert.Equal(t, [][]any{{"new", uint8(9)}}, out.Data())
	})
	t.Run("map", func(t *testing.T) {
		src := NewMap(NewString(), NewDynamic())
		src.Append(map[string]any{"a": int64(1)})
		dst := NewMap(NewString(), NewDynamic())
		roundTripDynamicJSON(t, src, dst)
		assert.Equal(t, map[string]any{"a": int64(1)}, dst.Row(0))
		dst.Delete(0, 1)
		dst.Append(map[string]any{"b": "new"})
		require.NoError(t, dst.ValidateInsert())
		out := NewMap(NewString(), NewDynamic())
		roundTripDynamicJSON(t, dst, out)
		assert.Equal(t, []map[string]any{{"b": "new"}}, out.Data())
	})
}

func TestJSONReadAppend(t *testing.T) {
	for _, mutation := range []string{"append", "delete", "deleteFunc", "remove", "reset"} {
		t.Run(mutation, func(t *testing.T) {
			src := NewJSON()
			src.AppendMulti(map[string]any{"a": int64(1)}, map[string]any{"a": int64(2)})
			dst := NewJSON()
			roundTripDynamicJSON(t, src, dst)
			want := []any{map[string]any{"a": int64(1)}, map[string]any{"a": int64(2)}}
			switch mutation {
			case "delete":
				dst.Delete(0, 1)
				want = want[1:]
			case "deleteFunc":
				dst.DeleteFunc(func(row int) bool { return row == 0 })
				want = want[1:]
			case "remove":
				dst.Remove(1)
				want = want[:1]
			case "reset":
				dst.Reset()
				want = nil
			}
			require.NoError(t, dst.AppendAny(map[string]any{"a": int64(9), "b": "new"}))
			want = append(want, map[string]any{"a": int64(9), "b": "new"})
			out := NewJSON()
			roundTripDynamicJSON(t, dst, out)
			assert.Equal(t, want, out.Data())
		})
	}
}

func TestJSONReadSharedDataMutations(t *testing.T) {
	for _, version := range []uint64{jsonObjectV2SerializationVersion, jsonObjectV3SerializationVersion} {
		for _, mutation := range []string{"append", "delete", "deleteFunc", "remove", "reset"} {
			t.Run(fmt.Sprintf("v%d/%s", version, mutation), func(t *testing.T) {
				src := NewJSON()
				src.AppendMulti(map[string]any{"a": int64(1)}, map[string]any{"a": int64(2)})
				src.serializationVersion = version
				data := NewMap(NewString(), NewString())
				data.AppendMulti(map[string]string{"x": "first"}, map[string]string{"y": "second"})
				src.sharedData = data
				dst := NewJSON()
				roundTripDynamicJSON(t, src, dst)
				want := []any{map[string]any{"a": int64(1)}, map[string]any{"a": int64(2)}}
				wantShared := []map[any]any{{"x": "first"}, {"y": "second"}}
				switch mutation {
				case "delete":
					dst.Delete(0, 1)
					want, wantShared = want[1:], wantShared[1:]
				case "deleteFunc":
					dst.DeleteFunc(func(row int) bool { return row == 0 })
					want, wantShared = want[1:], wantShared[1:]
				case "remove":
					dst.Remove(1)
					want, wantShared = want[:1], wantShared[:1]
				case "reset":
					dst.Reset()
					want, wantShared = nil, nil
				}
				require.NoError(t, dst.AppendAny(map[string]any{"a": int64(9)}))
				want = append(want, map[string]any{"a": int64(9)})
				wantShared = append(wantShared, nil)
				out := NewJSON()
				roundTripDynamicJSON(t, dst, out)
				assert.Equal(t, want, out.Data())
				for i, expected := range wantShared {
					assert.Equal(t, expected, out.sharedData.RowAny(i))
				}
			})
		}
	}
}

func TestDynamicReadReusedColumn(t *testing.T) {
	dst := NewDynamic()
	for _, values := range [][]any{{int64(1), "first"}, {[]uint64{2}, nil}, {}, {"last"}} {
		src := NewDynamic()
		src.AppendMulti(values...)
		roundTripDynamicJSON(t, src, dst)
		require.Equal(t, len(values), dst.NumRow())
		for i, expected := range values {
			assert.Equal(t, expected, dst.Row(i))
		}
		require.NoError(t, dst.AppendAny(int64(9)))
		out := NewDynamic()
		roundTripDynamicJSON(t, dst, out)
		assert.Equal(t, append(values, int64(9)), out.Data())
	}
}

func TestDynamicReadSharedVariantAppend(t *testing.T) {
	sharedCol := NewSharedVariant()
	src := NewDynamic(New[int64](), sharedCol)
	sharedCol.StringBase.Append(string([]byte{byte(helper.BinaryTypeIndexString)}) + "shared")
	src.Append(nil)
	dst := NewDynamic()
	roundTripDynamicJSON(t, src, dst)
	assert.Equal(t, []any{"shared", nil}, dst.Data())
	require.NoError(t, dst.AppendAny(float64(1.5)))
	out := NewDynamic()
	roundTripDynamicJSON(t, dst, out)
	assert.Equal(t, []any{"shared", nil, float64(1.5)}, out.Data())
}

func TestJSONReadTypedPathAppend(t *testing.T) {
	newTypedJSON := func() *JSON {
		col := NewJSON()
		col.typedPaths = []string{"a"}
		col.typedPathIndex["a"] = 0
		col.typedColumns = []ColumnCore{New[int64]()}
		return col
	}
	src := newTypedJSON()
	src.AppendMulti(
		map[string]any{"a": int64(1), "b": "first"},
		map[string]any{"a": int64(2), "b": "second"},
	)
	dst := newTypedJSON()
	roundTripDynamicJSON(t, src, dst)
	dst.Delete(0, 1)
	require.NoError(t, dst.AppendAny(map[string]any{"a": int64(9), "b": "last"}))
	out := newTypedJSON()
	roundTripDynamicJSON(t, dst, out)
	assert.Equal(t, []any{
		map[string]any{"a": int64(2), "b": "second"},
		map[string]any{"a": int64(9), "b": "last"},
	}, out.Data())
}
