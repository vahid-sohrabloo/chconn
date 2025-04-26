// file: column/array_base_test.go
package column_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestArrayBase_DeleteFunc_Middle(t *testing.T) {
	arr := column.New[uint32]().Array()

	// append three rows: [1,2], [3,4], [5,6]
	arr.Append([]uint32{1, 2})
	arr.Append([]uint32{3, 4})
	arr.Append([]uint32{5, 6})

	// delete only the middle row (index==1)
	arr.DeleteFunc(func(i int) bool { return i == 1 })

	// now we should have 2 rows: [1,2] and [5,6]
	assert.Equal(t, 2, arr.NumRow())
	// offsets should be [2,4]
	assert.EqualValues(t, []uint64{2, 4}, arr.Offsets())

	// check content via Row
	first := arr.Row(0)
	assert.Equal(t, []uint32{1, 2}, first)
	second := arr.Row(1)
	assert.Equal(t, []uint32{5, 6}, second)
}

func TestArrayBase_DeleteFunc_None(t *testing.T) {
	arr := column.New[uint32]().Array()
	slices := [][]uint32{{10}, {20}, {30}}
	for _, s := range slices {
		arr.Append(s)
	}
	// predicate that never deletes
	arr.DeleteFunc(func(i int) bool { return false })
	// should be unchanged
	assert.Equal(t, 3, arr.NumRow())
	assert.EqualValues(t, []uint64{1, 2, 3}, arr.Offsets())
	for i, want := range slices {
		got := arr.Row(i)
		assert.Equal(t, len(want), len(got))
		for j, v := range want {
			assert.Equal(t, v, got[j])
		}
	}
}

func TestArrayBase_DeleteFunc_All(t *testing.T) {
	arr := column.New[uint32]().Array()
	for i := 0; i < 5; i++ {
		arr.Append([]uint32{uint32(i)})
	}
	// predicate that deletes everything
	arr.DeleteFunc(func(i int) bool { return true })
	assert.Equal(t, 0, arr.NumRow())
	assert.Empty(t, arr.Offsets())
}

func BenchmarkArrayBase_DeleteFunc_1D(b *testing.B) {
	const n = 20_000_000
	// build a 1D array of n rows each with one element

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		arr := column.New[uint32]().Array()
		for i := 0; i < n; i++ {
			arr.Append([]uint32{uint32(i)})
		}
		b.StartTimer()
		// delete even indices
		arr.DeleteFunc(func(i int) bool { return i%2 == 0 })
	}
}

func BenchmarkArrayBase_DeleteFunc_2D(b *testing.B) {
	const n = 10_000_000
	// build a 2D array of n rows each with one-element inner slice
	data := []string{"a"}
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		arr := column.NewString().LowCardinality().Array()
		arr.SetWriteBufferSize(n)
		for i := 0; i < n; i++ {
			arr.Append(data)
		}
		b.StartTimer()

		// delete even indices
		arr.DeleteFunc(func(i int) bool { return i%2 == 0 })
	}
}
