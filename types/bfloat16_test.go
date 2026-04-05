package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBFloat16(t *testing.T) {
	// Test round-trip
	f := float32(3.14)
	b := BFloat16FromFloat32(f)
	result := b.Float32()
	// BFloat16 loses precision — just check it's close
	assert.InDelta(t, f, result, 0.02)

	// Test zero
	assert.Equal(t, float32(0), BFloat16(0).Float32())

	// Test negative
	neg := BFloat16FromFloat32(-1.5)
	assert.InDelta(t, -1.5, neg.Float32(), 0.01)
}
