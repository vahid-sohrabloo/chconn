package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecimal(t *testing.T) {
	d32 := Decimal32(12_234)
	assert.Equal(t, d32.Float64(3), float64(12.234))
	d64 := Decimal64(12_234)
	assert.Equal(t, d64.Float64(3), float64(12.234))
	assert.Equal(t, Decimal32FromFloat64(12.2334, 3), Decimal32(12233))
	assert.Equal(t, Decimal64FromFloat64(12.2334, 3), Decimal64(12233))

}
