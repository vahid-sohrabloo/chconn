package types

import (
	"math"
	"math/big"
)

// Note, Zero and Max are functions just to make read-only values.
// We cannot define constants for structures, and global variables
// are unacceptable because it will be possible to change them.

// Zero is the lowest possible Int128 value.
func Int128Zero() Int128 {
	return Int128From64(0)
}

// Max is the largest possible Int128 value.
func Int128Max() Int128 {
	return Int128{
		Lo: math.MaxUint64,
		Hi: math.MaxInt64,
	}
}

// Int128 is an unsigned 128-bit number.
// All methods are immutable, works just like standard uint64.
type Int128 struct {
	Lo uint64 // lower 64-bit half
	Hi int64  // upper 64-bit half
}

// Note, there in no New(lo, hi) just not to confuse
// which half goes first: lower or upper.
// Use structure initialization Int128{Lo: ..., Hi: ...} instead.

// From64 converts 64-bit value v to a Int128 value.
// Upper 64-bit half will be zero.
func Int128From64(v int64) Int128 {
	var hi int64
	if v < 0 {
		hi = -1
	}
	return Int128{Lo: uint64(v), Hi: hi}
}

// FromBig converts *big.Int to 128-bit Int128 value ignoring overflows.
// If input integer is nil or negative then return Zero.
// If input interger overflows 128-bit then return Max.
func Int128FromBig(i *big.Int) Int128 {
	u, _ := Int128FromBigEx(i)
	return u
}

// FromBigEx converts *big.Int to 128-bit Int128 value (eXtended version).
// Provides ok successful flag as a second return value.
// If input integer is negative or overflows 128-bit then ok=false.
// If input is nil then zero 128-bit returned.
func Int128FromBigEx(i *big.Int) (Int128, bool) {
	switch {
	case i == nil:
		return Int128Zero(), true // assuming nil === 0
	case i.BitLen() > 128:
		return Int128Max(), false // value overflows 128-bit!
	}

	neg := false
	if i.Sign() == -1 {
		i = new(big.Int).Neg(i)
		neg = true
	}

	// Note, actually result of big.Int.Uint64 is undefined
	// if stored value is greater than 2^64
	// but we assume that it just gets lower 64 bits.
	t := new(big.Int)
	lo := i.Uint64()
	hi := int64(t.Rsh(i, 64).Uint64())
	val := Int128{
		Lo: lo,
		Hi: hi,
	}
	if neg {
		return val.Neg(), true
	}
	return val, true
}

// Big returns 128-bit value as a *big.Int.
func (u Int128) Big() *big.Int {
	i := new(big.Int).SetInt64(u.Hi)

	i = i.Lsh(i, 64)
	i = i.Or(i, new(big.Int).SetUint64(u.Lo))
	return i
}

// Equals returns true if two 128-bit values are equal.
// Int128 values can be compared directly with == operator
// but use of the Equals method is preferred for consistency.
func (u Int128) Equals(v Int128) bool {
	return (u.Lo == v.Lo) && (u.Hi == v.Hi)
}

// Neg returns the additive inverse of an Int128
func (u Int128) Neg() (z Int128) {
	z.Hi = -u.Hi
	z.Lo = -u.Lo
	if z.Lo > 0 {
		z.Hi--
	}
	return z
}

func (u Int128) Uint128() Uint128 {
	return Uint128{Lo: u.Lo, Hi: uint64(u.Hi)}
}

func (u Int128) Uint64() uint64 {
	return u.Lo
}
