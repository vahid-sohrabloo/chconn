package types

import "math"

// BFloat16 represents a 16-bit brain floating-point number.
// It is stored as a uint16 but represents a truncated float32.
type BFloat16 uint16

// Float32 converts the BFloat16 to a float32.
func (b BFloat16) Float32() float32 {
	return math.Float32frombits(uint32(b) << 16)
}

// BFloat16FromFloat32 converts a float32 to BFloat16 by truncating.
func BFloat16FromFloat32(f float32) BFloat16 {
	return BFloat16(math.Float32bits(f) >> 16)
}
