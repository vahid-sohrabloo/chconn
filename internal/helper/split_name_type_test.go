package helper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypesInParenthesesNames(t *testing.T) {
	cols, err := TypesInParentheses([]byte("a Int64, b.c String, `x y` UInt8, Tuple(d Int8), e Tuple(f Int8)"))
	require.NoError(t, err)
	var names, chTypes []string
	for _, c := range cols {
		names = append(names, string(c.Name))
		chTypes = append(chTypes, string(c.ChType))
	}
	assert.Equal(t, []string{"a", "b.c", "x y", "", "e"}, names)
	assert.Equal(t, []string{"Int64", "String", "UInt8", "Tuple(d Int8)", "Tuple(f Int8)"}, chTypes)
}
