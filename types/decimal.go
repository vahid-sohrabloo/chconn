package types

import (
	"fmt"
	"math/big"
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
	// Convert to string
	strNum := fmt.Sprintf("%d", d)

	// Ensure that the string is long enough to insert a decimal point
	for len(strNum) <= scale {
		if d < 0 {
			// For negative numbers, pad after the negative sign
			strNum = strNum[:1] + "0" + strNum[1:]
		} else {
			strNum = "0" + strNum
		}
	}

	// Calculate the insertion point for the decimal
	decimalInsertionPoint := len(strNum) - scale

	// Insert the decimal point
	result := strNum[:decimalInsertionPoint] + "." + strNum[decimalInsertionPoint:]

	return result
}

func (d Decimal64) String(scale int) string {
	// Convert to string
	strNum := fmt.Sprintf("%d", d)

	// Ensure that the string is long enough to insert a decimal point
	for len(strNum) <= scale {
		if d < 0 {
			// For negative numbers, pad after the negative sign
			strNum = strNum[:1] + "0" + strNum[1:]
		} else {
			strNum = "0" + strNum
		}
	}

	// Calculate the insertion point for the decimal
	decimalInsertionPoint := len(strNum) - scale

	// Insert the decimal point
	result := strNum[:decimalInsertionPoint] + "." + strNum[decimalInsertionPoint:]

	return result
}

func (d Decimal128) ToInt128(scale int) Int128 {
	bigInt := Int128(d).Big()
	// If scale is positive, divide the number by 10^scale
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return Int128FromBig(bigInt.Div(bigInt, divisor))
}

// Float64 converts decimal number to float64.
func (d Decimal128) String(scale int) string {
	// Convert to string
	strNum := Int128(d).String()

	// Ensure that the string is long enough to insert a decimal point
	for len(strNum) <= scale {
		if d.Hi < 0 {
			// For negative numbers, pad after the negative sign
			strNum = strNum[:1] + "0" + strNum[1:]
		} else {
			strNum = "0" + strNum
		}
	}

	// Calculate the insertion point for the decimal
	decimalInsertionPoint := len(strNum) - scale

	// Insert the decimal point
	result := strNum[:decimalInsertionPoint] + "." + strNum[decimalInsertionPoint:]

	return result
}

func (d Decimal256) ToInt256(scale int) Int256 {
	bigInt := Int256(d).Big()
	// If scale is positive, divide the number by 10^scale
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return Int256FromBig(bigInt.Div(bigInt, divisor))
}

// Float64 converts decimal number to float64.
func (d Decimal256) String(scale int) string {
	// Convert to string
	strNum := Int256(d).String()

	// Ensure that the string is long enough to insert a decimal point
	for len(strNum) <= scale {
		if d.Hi.Hi < 0 {
			// For negative numbers, pad after the negative sign
			strNum = strNum[:1] + "0" + strNum[1:]
		} else {
			strNum = "0" + strNum
		}
	}

	// Calculate the insertion point for the decimal
	decimalInsertionPoint := len(strNum) - scale

	// Insert the decimal point
	result := strNum[:decimalInsertionPoint] + "." + strNum[decimalInsertionPoint:]

	return result
}
