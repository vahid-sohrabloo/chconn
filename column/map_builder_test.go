package column_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// TestMapBuilder checks that the generic Map builder infers both type
// parameters and produces the same column as NewMap.
func TestMapBuilder(t *testing.T) {
	viaMethod := column.NewString().Map(column.New[uint64]())
	viaFunc := column.NewMap[string, uint64](column.NewString(), column.New[uint64]())
	require.Equal(t, reflect.TypeOf(viaFunc), reflect.TypeOf(viaMethod))

	in := map[string]uint64{"a": 1, "b": 2}
	viaMethod.Append(in)
	viaFunc.Append(in)
	assert.Equal(t, viaFunc.Row(0), viaMethod.Row(0))
	assert.Equal(t, 1, viaMethod.NumRow())

	// numeric key
	assert.Equal(t,
		reflect.TypeOf(column.NewMap[uint64, string](column.New[uint64](), column.NewString())),
		reflect.TypeOf(column.New[uint64]().Map(column.NewString())))

	// LowCardinality key: Map(LowCardinality(String), UInt64)
	assert.Equal(t,
		reflect.TypeOf(column.NewMap[string, uint64](column.NewString().LC(), column.New[uint64]())),
		reflect.TypeOf(column.NewString().LC().Map(column.New[uint64]())))
}
