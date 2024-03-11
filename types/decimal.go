package types

import (
	"math/big"
	"strconv"
)

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

// Float64 converts decimal number to float64.
func (d Decimal128) Float64(scale int) float64 {
	return float64(d.Lo) / factors10[scale]
}

// Float64 converts decimal number to float64.
func (d Decimal256) Float64(scale int) float64 {
	return float64(d.Lo.Lo) / factors10[scale]
}

// Decimal32FromFloat64 converts float64 to decimal32 number.
func Decimal32FromFloat64(f float64, scale int) Decimal32 {
	return Decimal32(f * factors10[scale])
}

// Decimal64FromFloat64 converts float64 to decimal64 number.
func Decimal64FromFloat64(f float64, scale int) Decimal64 {
	return Decimal64(f * factors10[scale])
}

func (d Decimal32) String(scale int) string {
	if d == 0 {
		return "0"
	}
	return string(d.Append(scale, []byte{}))
}

func (d Decimal32) Append(scale int, b []byte) []byte {
	if d == 0 {
		return append(b, '0')
	}

	initialLen := len(b)

	// Reduce scale and d while the least significant digit is zero
	for d%10 == 0 && scale > 0 {
		scale--
		d /= 10
	}

	// Append the integer part of d to b
	b = strconv.AppendInt(b, int64(d), 10)

	return fixDecimalPoint(b, initialLen, scale, d < 0)
}

func (d Decimal64) String(scale int) string {
	if d == 0 {
		return "0"
	}
	return string(d.Append(scale, []byte{}))
}

// Append .

var zeroStringBytes = []byte("00000000000000000000000000000000000000000000000000000000000000000000")

func (d Decimal64) Append(scale int, b []byte) []byte {
	if d == 0 {
		return append(b, '0')
	}
	initialLen := len(b)

	// Reduce scale and d while the least significant digit is zero
	for d%10 == 0 && scale > 0 {
		scale--
		d /= 10
	}

	// Append the integer part of d to b
	b = strconv.AppendInt(b, int64(d), 10)
	return fixDecimalPoint(b, initialLen, scale, d < 0)
}

func growSlice(b []byte, n int) []byte {
	if cap(b) >= n {
		return b[:n]
	}
	b = append(b, make([]byte, n-len(b))...)
	return b
}

func fixDecimalPoint(b []byte, initialLen, scale int, isNegative bool) []byte {
	// No decimal places required
	if scale == 0 {
		return b
	}

	// Calculate the length of the number (ignoring what was previously in b)
	numberLen := len(b) - initialLen
	if isNegative {
		numberLen-- // Adjust for negative sign
	}

	// Check if zeros need to be prepended
	if scale > numberLen {
		zeroesNeeded := scale - numberLen + 1
		initialLenCopy := initialLen
		if isNegative {
			initialLenCopy++
		}
		b = growSlice(b, len(b)+zeroesNeeded)
		copy(b[initialLenCopy+zeroesNeeded:], b[initialLenCopy:])
		copy(b[initialLenCopy:], zeroStringBytes[:zeroesNeeded])
	}

	// Recalculate the length of the number part
	numberLen = len(b) - initialLen

	// Calculate the insertion point for the decimal
	decimalInsertionPoint := initialLen + numberLen - scale

	// Make room for the decimal point by shifting bytes to the right
	b = append(b, 0) // Extend slice by 1
	copy(b[decimalInsertionPoint+1:], b[decimalInsertionPoint:])

	// Insert the decimal point
	b[decimalInsertionPoint] = '.'

	return b
}

func (d Decimal128) ToInt128(scale int) Int128 {
	bigInt := Int128(d).Big()
	// If scale is positive, divide the number by 10^scale
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return Int128FromBig(bigInt.Div(bigInt, divisor))
}

// Float64 converts decimal number to string.
func (d Decimal128) String(scale int) string {
	if Int128(d).Zero() {
		return "0"
	}
	return string(d.Append(scale, []byte{}))
}

// Float64 converts decimal number to float64.
func (d Decimal128) Append(scale int, b []byte) []byte {
	if Int128(d).Zero() {
		return append(b, '0')
	}

	initialLen := len(b)

	bigInt := Int128(d).Big()
	ten := big.NewInt(10) // Define a constant for 10

	// Reduce scale and d while the least significant digit is zero
	checkMod := new(big.Int).Set(bigInt)
	for scale > 0 && len(checkMod.Mod(checkMod, ten).Bits()) == 0 {
		scale--
		bigInt.Div(bigInt, ten)
		checkMod = new(big.Int).Set(bigInt)
	}

	// Append the integer part of d to b
	b = bigInt.Append(b, 10)

	return fixDecimalPoint(b, initialLen, scale, bigInt.Sign() == -1)
}

func (d Decimal256) ToInt256(scale int) Int256 {
	bigInt := Int256(d).Big()
	// If scale is positive, divide the number by 10^scale
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return Int256FromBig(bigInt.Div(bigInt, divisor))
}

// Float64 converts decimal number to string.
func (d Decimal256) String(scale int) string {
	if Int256(d).Zero() {
		return "0"
	}
	return string(d.Append(scale, []byte{}))
}

// Float64 converts decimal number to float64.
func (d Decimal256) Append(scale int, b []byte) []byte {
	if Int256(d).Zero() {
		return append(b, '0')
	}
	initialLen := len(b)

	bigInt := Int256(d).Big()
	ten := big.NewInt(10) // Define a constant for 10

	// Reduce scale and d while the least significant digit is zero
	checkMod := new(big.Int).Set(bigInt)
	for scale > 0 && len(checkMod.Mod(checkMod, ten).Bits()) == 0 {
		scale--
		bigInt.Div(bigInt, ten)
		checkMod = new(big.Int).Set(bigInt)
	}

	// Append the integer part of d to b
	b = bigInt.Append(b, 10)

	// Append the integer part of d to b
	return fixDecimalPoint(b, initialLen, scale, bigInt.Sign() == -1)
}
