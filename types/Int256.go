package types

import (
	"math/big"
)

// Note, Zero and Max are functions just to make read-only values.
// We cannot define constants for structures, and global variables
// are unacceptable because it will be possible to change them.

// Zero is the lowest possible Int256 value.
func Int256Zero() Int256 {
	return Int256From64(0)
}

// Max is the largest possible Int256 value.
func Int256Max() Int256 {
	return Int256{
		Lo: Uint128Max(),
		Hi: Int128Max(),
	}
}

// Int256 is an unsigned 256-bit number.
// All methods are immutable, works just like standard uint64.
type Int256 struct {
	Lo Uint128 // lower 128-bit half
	Hi Int128  // upper 128-bit half
}

// From128 converts 128-bit value v to a Int256 value.
// Upper 128-bit half will be zero.
func Int256From128(v Int128) Int256 {
	var hi Int128
	if v.Hi < 0 {
		hi = Int128{Lo: 0, Hi: -1}
		v = v.Neg()
	}
	return Int256{Lo: Uint128{
		Lo: v.Lo,
		Hi: uint64(v.Hi),
	}, Hi: hi}
}

// From64 converts 64-bit value v to a Int256 value.
// Upper 128-bit half will be zero.
func Int256From64(v int64) Int256 {
	return Int256From128(Int128From64(v))
}

// FromBig converts *big.Int to 256-bit Int256 value ignoring overflows.
// If input integer is nil or negative then return Zero.
// If input integer overflows 256-bit then return Max.
func Int256FromBig(i *big.Int) Int256 {
	u, _ := Int256FromBigEx(i)
	return u
}

// FromBigEx converts *big.Int to 256-bit Int256 value (eXtended version).
// Provides ok successful flag as a second return value.
// If input integer is negative or overflows 256-bit then ok=false.
// If input is nil then zero 256-bit returned.
func Int256FromBigEx(i *big.Int) (Int256, bool) {
	switch {
	case i == nil:
		return Int256Zero(), true // assuming nil === 0

	case i.BitLen() > 256:
		return Int256Max(), false // value overflows 256-bit!
	}

	neg := false
	if i.Sign() == -1 {
		i = new(big.Int).Neg(i)
		neg = true
	}

	t := new(big.Int)
	lolo := i.Uint64()
	lohi := t.Rsh(i, 64).Uint64()
	hilo := t.Rsh(i, 128).Uint64()
	hihi := int64(t.Rsh(i, 192).Uint64())
	val := Int256{
		Lo: Uint128{Lo: lolo, Hi: lohi},
		Hi: Int128{Lo: hilo, Hi: hihi},
	}
	if neg {
		val = val.Neg()
	}
	return val, true
}

// Big returns 256-bit value as a *big.Int.
//
//nolint:dupl
func (u Int256) Big() *big.Int {
	t := new(big.Int)
	i := new(big.Int).SetInt64(u.Hi.Hi)
	i = i.Lsh(i, 64)
	i = i.Or(i, t.SetUint64(u.Hi.Lo))
	i = i.Lsh(i, 64)
	i = i.Or(i, t.SetUint64(u.Lo.Hi))
	i = i.Lsh(i, 64)
	i = i.Or(i, t.SetUint64(u.Lo.Lo))
	return i
}

// Equals returns true if two 256-bit values are equal.
// Int256 values can be compared directly with == operator
// but use of the Equals method is preferred for consistency.
func (u Int256) Equals(v Int256) bool {
	return u.Lo.Equals(v.Lo) && u.Hi.Equals(v.Hi)
}

// Neg returns the additive inverse of an Int256
func (u Int256) Neg() (z Int256) {
	z.Hi = u.Hi.Neg()
	z.Lo.Lo = -u.Lo.Lo
	z.Lo.Hi = -u.Lo.Hi
	// TODO, I'm not sure here.
	if z.Lo.Hi > 0 || z.Lo.Lo > 0 {
		z.Hi.Lo--
	}
	return z
}

func (u Int256) Uint128() Uint128 {
	return u.Lo
}

func (u Int256) Uint256() Uint256 {
	return Uint256{
		Lo: u.Lo,
		Hi: u.Hi.Uint128(),
	}
}

func (u Int256) Uint64() uint64 {
	return u.Lo.Uint64()
}
