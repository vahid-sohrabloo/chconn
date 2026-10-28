package column

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

func TestArrayCacheMutationRegression(t *testing.T) {
	t.Run("numbers", func(t *testing.T) {
		testArrayCacheMutations(t, func() Column[uint64] { return New[uint64]() }, 1, 2, 3)
	})
	t.Run("strings", func(t *testing.T) {
		testArrayCacheMutations(t, func() Column[string] { return NewString() }, "one", "two", "three")
	})
}

func testArrayCacheMutations[T any](t *testing.T, newColumn func() Column[T], one, two, three T) {
	t.Helper()
	tests := []struct {
		name   string
		mutate func(*Array[T])
		want   [][]T
	}{
		{"append", func(a *Array[T]) { a.Append([]T{three}) }, [][]T{{one}, {two, three}, {}, {three}}},
		{"append-multi", func(a *Array[T]) { a.AppendMulti([]T{three}, []T{one}) }, [][]T{{one}, {two, three}, {}, {three}, {one}}},
		{"append-any", func(a *Array[T]) { require.NoError(t, a.AppendAny([]T{three})) }, [][]T{{one}, {two, three}, {}, {three}}},
		{"base-append-any", func(a *Array[T]) { require.NoError(t, a.ArrayBase.AppendAny([]any{three})) }, [][]T{{one}, {two, three}, {}, {three}}},
		{"append-item", func(a *Array[T]) {
			a.AppendLen(1)
			a.AppendItem(three)
		}, [][]T{{one}, {two, three}, {}, {three}}},
		{"direct-column", func(a *Array[T]) {
			data := a.Column().(Column[T])
			data.Remove(0)
			data.AppendMulti(three, one, two)
		}, [][]T{{three}, {one, two}, {}}},
		{"delete", func(a *Array[T]) { a.Delete(0, 1) }, [][]T{{two, three}, {}}},
		{"delete-func", func(a *Array[T]) { a.DeleteFunc(func(row int) bool { return row == 0 }) }, [][]T{{two, three}, {}}},
		{"remove-append", func(a *Array[T]) {
			a.Remove(1)
			a.Append([]T{three})
		}, [][]T{{one}, {three}}},
		{"reset-append", func(a *Array[T]) {
			a.Reset()
			a.Append([]T{three})
		}, [][]T{{three}}},
		{"delete-all", func(a *Array[T]) { a.Delete(0, a.NumRow()) }, [][]T{}},
		{"delete-func-all", func(a *Array[T]) { a.DeleteFunc(func(int) bool { return true }) }, [][]T{}},
		{"remove-all", func(a *Array[T]) { a.Remove(0) }, [][]T{}},
		{"reset", func(a *Array[T]) { a.Reset() }, [][]T{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewArray(newColumn())
			seed := [][]T{{one}, {two, three}, {}}
			a.AppendMulti(seed...)
			checkArrayCacheValues(t, a, seed)
			tt.mutate(a)
			checkArrayCacheValues(t, a, tt.want)
			require.NoError(t, a.ValidateInsert())
		})
	}

	t.Run("read-raw", func(t *testing.T) {
		a := NewArray(newColumn())
		a.Append([]T{one})
		checkArrayCacheValues(t, a, [][]T{{one}})
		for _, rows := range [][][]T{{{two, three}, {}}, {{three}}, {}} {
			source := NewArray(newColumn())
			source.AppendMulti(rows...)
			var buf bytes.Buffer
			_, err := source.WriteTo(&buf)
			require.NoError(t, err)
			require.NoError(t, a.ReadHeader(readerwriter.NewReader(&buf), nil))
			require.NoError(t, a.ReadRaw(len(rows)))
			checkArrayCacheValues(t, a, rows)
		}
		a.Append([]T{two})
		checkArrayCacheValues(t, a, [][]T{{two}})
		require.NoError(t, a.ValidateInsert())
	})
}

func checkArrayCacheValues[T any](t *testing.T, a *Array[T], want [][]T) {
	t.Helper()
	require.Equal(t, want, a.Data())
	require.Equal(t, want, a.Read(make([][]T, 0)))
	require.Equal(t, len(want), a.NumRow())
	for row, value := range want {
		if len(value) == 0 {
			require.Empty(t, a.Row(row))
			require.Empty(t, a.RowAny(row))
		} else {
			require.Equal(t, value, a.Row(row))
			require.Equal(t, value, a.RowAny(row))
		}
	}
}

func TestMapCacheMutationRegression(t *testing.T) {
	t.Run("numbers", func(t *testing.T) {
		testMapCacheMutations(t, func() Column[uint64] { return New[uint64]() }, uint64(1), uint64(2), uint64(3))
	})
	t.Run("strings", func(t *testing.T) {
		testMapCacheMutations(t, func() Column[string] { return NewString() }, "one", "two", "three")
	})
}

func testMapCacheMutations[T comparable](t *testing.T, newColumn func() Column[T], one, two, three T) {
	t.Helper()
	tests := []struct {
		name   string
		mutate func(*Map[T, T])
		want   []map[T]T
	}{
		{"append", func(m *Map[T, T]) { m.Append(map[T]T{three: one}) }, []map[T]T{{one: two}, {two: three}, {}, {three: one}}},
		{"append-multi", func(m *Map[T, T]) { m.AppendMulti(map[T]T{three: one}, map[T]T{one: three}) }, []map[T]T{{one: two}, {two: three}, {}, {three: one}, {one: three}}},
		{"append-any", func(m *Map[T, T]) { require.NoError(t, m.AppendAny(map[T]T{three: one})) }, []map[T]T{{one: two}, {two: three}, {}, {three: one}}},
		{"base-append-any", func(m *Map[T, T]) { require.NoError(t, m.MapBase.AppendAny(map[T]T{three: one})) }, []map[T]T{{one: two}, {two: three}, {}, {three: one}}},
		{"append-len", func(m *Map[T, T]) {
			m.AppendLen(1)
			m.KeyColumn().Append(three)
			m.ValueColumn().Append(one)
		}, []map[T]T{{one: two}, {two: three}, {}, {three: one}}},
		{"direct-columns", func(m *Map[T, T]) {
			m.KeyColumn().Remove(0)
			m.KeyColumn().AppendMulti(three, one)
			m.ValueColumn().Remove(0)
			m.ValueColumn().AppendMulti(one, two)
		}, []map[T]T{{three: one}, {one: two}, {}}},
		{"delete", func(m *Map[T, T]) { m.Delete(0, 1) }, []map[T]T{{two: three}, {}}},
		{"delete-func", func(m *Map[T, T]) { m.DeleteFunc(func(row int) bool { return row == 0 }) }, []map[T]T{{two: three}, {}}},
		{"remove-append", func(m *Map[T, T]) {
			m.Remove(1)
			m.Append(map[T]T{three: one})
		}, []map[T]T{{one: two}, {three: one}}},
		{"reset-append", func(m *Map[T, T]) {
			m.Reset()
			m.Append(map[T]T{three: one})
		}, []map[T]T{{three: one}}},
		{"delete-all", func(m *Map[T, T]) { m.Delete(0, m.NumRow()) }, []map[T]T{}},
		{"delete-func-all", func(m *Map[T, T]) { m.DeleteFunc(func(int) bool { return true }) }, []map[T]T{}},
		{"remove-all", func(m *Map[T, T]) { m.Remove(0) }, []map[T]T{}},
		{"reset", func(m *Map[T, T]) { m.Reset() }, []map[T]T{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMap(newColumn(), newColumn())
			seed := []map[T]T{{one: two}, {two: three}, {}}
			m.AppendMulti(seed...)
			checkMapCacheValues(t, m, seed)
			tt.mutate(m)
			checkMapCacheValues(t, m, tt.want)
			require.NoError(t, m.ValidateInsert())
		})
	}

	t.Run("read-raw", func(t *testing.T) {
		m := NewMap(newColumn(), newColumn())
		m.Append(map[T]T{one: two})
		checkMapCacheValues(t, m, []map[T]T{{one: two}})
		for _, rows := range [][]map[T]T{{{two: three, three: one}, {}}, {{three: two}}, {}} {
			source := NewMap(newColumn(), newColumn())
			source.AppendMulti(rows...)
			var buf bytes.Buffer
			_, err := source.WriteTo(&buf)
			require.NoError(t, err)
			require.NoError(t, m.ReadHeader(readerwriter.NewReader(&buf), nil))
			require.NoError(t, m.ReadRaw(len(rows)))
			checkMapCacheValues(t, m, rows)
		}
		m.Append(map[T]T{two: three})
		checkMapCacheValues(t, m, []map[T]T{{two: three}})
		require.NoError(t, m.ValidateInsert())
	})
}

func checkMapCacheValues[T comparable](t *testing.T, m *Map[T, T], want []map[T]T) {
	t.Helper()
	require.Equal(t, want, m.Data())
	require.Equal(t, want, m.Read(make([]map[T]T, 0)))
	require.Equal(t, len(want), m.NumRow())
	for row, value := range want {
		require.Equal(t, value, m.Row(row))
		require.Equal(t, value, m.RowAny(row))
	}
}
