package types

// Decimal32 represents a 32-bit decimal number.
type Decimal32 int32

// Decimal64 represents a 64-bit decimal number.
type Decimal64 int64

// Decimal128 represents a 128-bit decimal number.
type Decimal128 Int128

// Decimal256 represents a 256-bit decimal number.
type Decimal256 Int256

// Table of powers of 10 for fast casting from floating types to decimal type
// representations.
var factors10 = []float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13,
	1e14, 1e15, 1e16, 1e17, 1e18,
}

// Float64 converts decimal number to float64.
func (d Decimal32) Float64(scale int) float64 {
	return float64(d) / factors10[scale]
}

// Float64 converts decimal number to float64.
func (d Decimal64) Float64(scale int) float64 {
	return float64(d) / factors10[scale]
}

// Decimal32FromFloat64 converts float64 to decimal32 number.
func Decimal32FromFloat64(f float64, scale int) Decimal32 {
	return Decimal32(f * factors10[scale])
}

// Decimal64FromFloat64 converts float64 to decimal64 number.
func Decimal64FromFloat64(f float64, scale int) Decimal64 {
	return Decimal64(f * factors10[scale])
}
