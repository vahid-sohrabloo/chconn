package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildVariantForRead creates a Variant and populates discriminatorsIndexPos
// to simulate the read path, allowing Row() to work.
func buildVariantForRead(colStr *String, colInt *Base[int64], entries []any) *Variant {
	v := NewVariant(colStr, colInt)
	for _, e := range entries {
		switch val := e.(type) {
		case string:
			colStr.Append(val)
		case int64:
			colInt.Append(val)
		case nil:
			v.AppendNil()
		}
	}

	// Build discriminatorsIndexPos like ReadRaw does
	num := v.NumRow()
	v.discriminatorsIndexPos = make([]int, num)
	var dataLen [256]int
	for i, n := range v.discriminators.values {
		v.discriminatorsIndexPos[i] = dataLen[n]
		dataLen[n]++
	}
	return v
}

func TestVariantDelete(t *testing.T) {
	t.Run("Delete range", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		v := buildVariantForRead(colStr, colInt, []any{"a", int64(1), nil, "b", int64(2)})

		assert.Equal(t, 5, v.NumRow())
		assert.Equal(t, "a", v.Row(0))
		assert.Equal(t, int64(1), v.Row(1))
		assert.Nil(t, v.Row(2))
		assert.Equal(t, "b", v.Row(3))
		assert.Equal(t, int64(2), v.Row(4))

		// Delete rows [1, 3) → removes row 1 (int64=1) and row 2 (nil)
		v.Delete(1, 3)

		assert.Equal(t, 3, v.NumRow())
		assert.Equal(t, "a", v.Row(0))
		assert.Equal(t, "b", v.Row(1))
		assert.Equal(t, int64(2), v.Row(2))
	})

	t.Run("Delete all", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		v := buildVariantForRead(colStr, colInt, []any{"a", int64(1), nil})

		v.Delete(0, 3)
		assert.Equal(t, 0, v.NumRow())
	})

	t.Run("Delete out of range", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		v := buildVariantForRead(colStr, colInt, []any{"a", int64(1)})

		// start >= NumRow, should be no-op
		v.Delete(5, 10)
		assert.Equal(t, 2, v.NumRow())
	})

	t.Run("Delete end clamped", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		v := buildVariantForRead(colStr, colInt, []any{"a", int64(1), "b"})

		// end > NumRow should be clamped
		v.Delete(1, 100)
		assert.Equal(t, 1, v.NumRow())
		assert.Equal(t, "a", v.Row(0))
	})

	t.Run("DeleteFunc", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		v := buildVariantForRead(colStr, colInt, []any{"a", int64(1), nil, "b", int64(2), nil})

		assert.Equal(t, 6, v.NumRow())

		// Delete even rows (0, 2, 4) → keep odd rows (1, 3, 5)
		v.DeleteFunc(func(row int) bool {
			return row%2 == 0
		})

		assert.Equal(t, 3, v.NumRow())
		assert.Equal(t, int64(1), v.Row(0))
		assert.Equal(t, "b", v.Row(1))
		assert.Nil(t, v.Row(2))
	})

	t.Run("DeleteFunc empty", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		v := NewVariant(colStr, colInt)

		// Should not panic on empty
		v.DeleteFunc(func(row int) bool { return true })
		assert.Equal(t, 0, v.NumRow())
	})

	t.Run("Delete only nils", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		v := buildVariantForRead(colStr, colInt, []any{"a", nil, nil, int64(1)})

		v.DeleteFunc(func(row int) bool {
			return v.RowIsNil(row)
		})

		assert.Equal(t, 2, v.NumRow())
		assert.Equal(t, "a", v.Row(0))
		assert.Equal(t, int64(1), v.Row(1))
	})

	t.Run("Delete keeps correct sub-column data", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		v := buildVariantForRead(colStr, colInt, []any{"x", "y", int64(10), "z", int64(20)})

		// Delete row 1 ("y") and row 3 ("z")
		v.DeleteFunc(func(row int) bool {
			return row == 1 || row == 3
		})

		assert.Equal(t, 3, v.NumRow())
		assert.Equal(t, "x", v.Row(0))
		assert.Equal(t, int64(10), v.Row(1))
		assert.Equal(t, int64(20), v.Row(2))
	})
}

func TestVariantRead(t *testing.T) {
	colStr := NewString()
	colInt := New[int64]()
	v := buildVariantForRead(colStr, colInt, []any{"a", int64(1), nil})

	var result []any
	result = v.Read(result)

	require.Len(t, result, 3)
	assert.Equal(t, "a", result[0])
	assert.Equal(t, int64(1), result[1])
	assert.Nil(t, result[2])

	// Verify capacity was pre-allocated
	result2 := make([]any, 0, 1)
	result2 = v.Read(result2)
	assert.True(t, cap(result2) >= 3)
}

func TestDynamicDelete(t *testing.T) {
	t.Run("withDynamicColumn Delete", func(t *testing.T) {
		d := NewDynamic()

		require.NoError(t, d.AppendAny(int64(1)))
		require.NoError(t, d.AppendAny("hello"))
		require.NoError(t, d.AppendAny(nil))
		require.NoError(t, d.AppendAny(int64(2)))
		require.NoError(t, d.AppendAny("world"))

		assert.Equal(t, 5, d.NumRow())

		// Delete rows [1, 3) → removes "hello" and nil
		d.Delete(1, 3)

		assert.Equal(t, 3, d.NumRow())
	})

	t.Run("withDynamicColumn DeleteFunc", func(t *testing.T) {
		d := NewDynamic()

		require.NoError(t, d.AppendAny(int64(1)))
		require.NoError(t, d.AppendAny("hello"))
		require.NoError(t, d.AppendAny(nil))
		require.NoError(t, d.AppendAny(int64(2)))

		// Delete even rows (0: int64(1), 2: nil)
		d.DeleteFunc(func(row int) bool {
			return row%2 == 0
		})

		assert.Equal(t, 2, d.NumRow())
	})

	t.Run("withDynamicColumn Remove", func(t *testing.T) {
		d := NewDynamic()

		require.NoError(t, d.AppendAny(int64(1)))
		require.NoError(t, d.AppendAny("hello"))
		require.NoError(t, d.AppendAny(int64(2)))
		require.NoError(t, d.AppendAny("world"))

		assert.Equal(t, 4, d.NumRow())

		d.Remove(2)
		assert.Equal(t, 2, d.NumRow())
	})

	t.Run("with static columns Delete", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		d := NewDynamic(colStr, colInt)

		colStr.Append("a")
		colInt.Append(1)
		d.variant.AppendNil()
		colStr.Append("b")

		assert.Equal(t, 4, d.NumRow())

		// Delete rows [1, 3)
		d.Delete(1, 3)

		assert.Equal(t, 2, d.NumRow())
	})

	t.Run("Delete out of range", func(t *testing.T) {
		d := NewDynamic()
		require.NoError(t, d.AppendAny(int64(1)))

		d.Delete(5, 10)
		assert.Equal(t, 1, d.NumRow())
	})

	t.Run("DeleteFunc empty", func(t *testing.T) {
		d := NewDynamic()
		d.DeleteFunc(func(row int) bool { return true })
		assert.Equal(t, 0, d.NumRow())
	})
}

func TestVariantBatchDelete(t *testing.T) {
	t.Run("batch delete keeps selected rows", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		v := buildVariantForRead(colStr, colInt, []any{"a", int64(1), nil, "b", int64(2)})

		assert.Equal(t, 5, v.NumRow())

		v.startBatchDelete()
		// Keep rows 0 and 3-4
		v.batchDeleteKeep(0, 1)
		v.batchDeleteKeep(3, 5)
		v.endBatchDelete()

		assert.Equal(t, 3, v.discriminators.NumRow())
	})

	t.Run("batch delete empty", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		v := NewVariant(colStr, colInt)

		// Should not panic
		v.startBatchDelete()
		v.endBatchDelete()
		assert.Equal(t, 0, v.NumRow())
	})
}

func TestDynamicBatchDelete(t *testing.T) {
	t.Run("static columns delegates to variant", func(t *testing.T) {
		colStr := NewString()
		colInt := New[int64]()
		d := NewDynamic(colStr, colInt)

		colStr.Append("a")
		colInt.Append(1)
		colStr.Append("b")

		assert.Equal(t, 3, d.NumRow())

		d.startBatchDelete()
		d.batchDeleteKeep(0, 1) // keep row 0
		d.batchDeleteKeep(2, 3) // keep row 2
		d.endBatchDelete()

		assert.Equal(t, 2, d.NumRow())
	})

	t.Run("dynamic columns batch delete is no-op", func(t *testing.T) {
		d := NewDynamic()
		require.NoError(t, d.AppendAny(int64(1)))
		require.NoError(t, d.AppendAny("hello"))

		// Should not panic — batch delete is a no-op for dynamic columns
		d.startBatchDelete()
		d.batchDeleteKeep(0, 1)
		d.endBatchDelete()

		assert.Equal(t, 2, d.NumRow())
	})
}

func TestDynamicRemoveSubColumnCleanup(t *testing.T) {
	d := NewDynamic()

	require.NoError(t, d.AppendAny(int64(1)))
	require.NoError(t, d.AppendAny(int64(2)))
	require.NoError(t, d.AppendAny("hello"))
	require.NoError(t, d.AppendAny(int64(3)))

	assert.Equal(t, 4, d.NumRow())

	// The int64 sub-column should have 3 rows, string should have 1
	var intCol ColumnCore
	var strCol ColumnCore
	for _, col := range d.columnsAppend {
		if col.NumRow() == 3 {
			intCol = col
		} else {
			strCol = col
		}
	}
	require.NotNil(t, intCol)
	require.NotNil(t, strCol)
	assert.Equal(t, 3, intCol.NumRow())
	assert.Equal(t, 1, strCol.NumRow())

	// Remove after index 2 → removes int64(3) and "hello" is at index 2 so kept via truncation
	d.Remove(2)

	assert.Equal(t, 2, d.NumRow())
	// int64 sub-column should now have 2 rows (int64(3) removed)
	assert.Equal(t, 2, intCol.NumRow())
	// string sub-column should have 0 rows ("hello" was at discriminator index 2, removed)
	assert.Equal(t, 0, strCol.NumRow())
}

func TestDynamicDuplicateColumnPanic(t *testing.T) {
	col1 := New[int64]()
	col2 := New[int64]()

	assert.Panics(t, func() {
		NewDynamic(col1, col2)
	})
}
