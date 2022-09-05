package types

import (
	"math/big"
)

// Note, Zero and Max are functions just to make read-only values.
// We cannot define constants for structures, and global variables
// are unacceptable because it will be possible to change them.

// Zero is the lowest possible Uint256 value.
func Uint256Zero() Uint256 {
	return Uint256From64(0)
}

// Max is the largest possible Uint256 value.
func Uint256Max() Uint256 {
	return Uint256{
		Lo: Uint128Max(),
		Hi: Uint128Max(),
	}
}

// Uint256 is an unsigned 256-bit number.
// All methods are immutable, works just like standard uint64.
type Uint256 struct {
	Lo Uint128 // lower 128-bit half
	Hi Uint128 // upper 128-bit half
}

// From128 converts 128-bit value v to a Uint256 value.
// Upper 128-bit half will be zero.
func Uint256From128(v Uint128) Uint256 {
	return Uint256{Lo: v}
}

// From64 converts 64-bit value v to a Uint256 value.
// Upper 128-bit half will be zero.
func Uint256From64(v uint64) Uint256 {
	return Uint256From128(Uint128From64(v))
}

// FromBig converts *big.Int to 256-bit Uint256 value ignoring overflows.
// If input integer is nil or negative then return Zero.
// If input interger overflows 256-bit then return Max.
func Uint256FromBig(i *big.Int) Uint256 {
	u, _ := Uint256FromBigEx(i)
	return u
}

// FromBigEx converts *big.Int to 256-bit Uint256 value (eXtended version).
// Provides ok successful flag as a second return value.
// If input integer is negative or overflows 256-bit then ok=false.
// If input is nil then zero 256-bit returned.
func Uint256FromBigEx(i *big.Int) (Uint256, bool) {
	switch {
	case i == nil:
		return Uint256Zero(), true // assuming nil === 0
	case i.Sign() < 0:
		return Uint256Zero(), false // value cannot be negative!
	case i.BitLen() > 256:
		return Uint256Max(), false // value overflows 256-bit!
	}

	// Note, actually result of big.Int.Uint64 is undefined
	// if stored value is greater than 2^64
	// but we assume that it just gets lower 64 bits.
	t := new(big.Int)
	lolo := i.Uint64()
	lohi := t.Rsh(i, 64).Uint64()
	hilo := t.Rsh(i, 128).Uint64()
	hihi := t.Rsh(i, 192).Uint64()
	return Uint256{
		Lo: Uint128{Lo: lolo, Hi: lohi},
		Hi: Uint128{Lo: hilo, Hi: hihi},
	}, true
}

// Big returns 256-bit value as a *big.Int.
//
//nolint:dupl
func (u Uint256) Big() *big.Int {
	t := new(big.Int)
	i := new(big.Int).SetUint64(u.Hi.Hi)
	i = i.Lsh(i, 64)
	i = i.Or(i, t.SetUint64(u.Hi.Lo))
	i = i.Lsh(i, 64)
	i = i.Or(i, t.SetUint64(u.Lo.Hi))
	i = i.Lsh(i, 64)
	i = i.Or(i, t.SetUint64(u.Lo.Lo))
	return i
}

// Equals returns true if two 256-bit values are equal.
// Uint256 values can be compared directly with == operator
// but use of the Equals method is preferred for consistency.
func (u Uint256) Equals(v Uint256) bool {
	return u.Lo.Equals(v.Lo) && u.Hi.Equals(v.Hi)
}
