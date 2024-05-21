package types

import (
	"math"
	"math/big"
	"strconv"
)

// Note, Zero and Max are functions just to make read-only values.
// We cannot define constants for structures, and global variables
// are unacceptable because it will be possible to change them.

// Zero is the lowest possible Uint128 value.
func Uint128Zero() Uint128 {
	return Uint128From64(0)
}

// Max is the largest possible Uint128 value.
func Uint128Max() Uint128 {
	return Uint128{
		Lo: math.MaxUint64,
		Hi: math.MaxUint64,
	}
}

// Uint128 is an unsigned 128-bit number.
// All methods are immutable, works just like standard uint64.
type Uint128 struct {
	Lo uint64 // lower 64-bit half
	Hi uint64 // upper 64-bit half
}

// Note, there in no New(lo, hi) just not to confuse
// which half goes first: lower or upper.
// Use structure initialization Uint128{Lo: ..., Hi: ...} instead.

// From64 converts 64-bit value v to a Uint128 value.
// Upper 64-bit half will be zero.
func Uint128From64(v uint64) Uint128 {
	return Uint128{Lo: v}
}

// FromBig converts *big.Int to 128-bit Uint128 value ignoring overflows.
// If input integer is nil or negative then return Zero.
// If input interger overflows 128-bit then return Max.
func Uint128FromBig(i *big.Int) Uint128 {
	u, _ := Uint128FromBigEx(i)
	return u
}

// FromBigEx converts *big.Int to 128-bit Uint128 value (eXtended version).
// Provides ok successful flag as a second return value.
// If input integer is negative or overflows 128-bit then ok=false.
// If input is nil then zero 128-bit returned.
func Uint128FromBigEx(i *big.Int) (Uint128, bool) {
	switch {
	case i == nil:
		return Uint128Zero(), true // assuming nil === 0
	case i.Sign() < 0:
		return Uint128Zero(), false // value cannot be negative!
	case i.BitLen() > 128:
		return Uint128Max(), false // value overflows 128-bit!
	}

	// Note, actually result of big.Int.Uint64 is undefined
	// if stored value is greater than 2^64
	// but we assume that it just gets lower 64 bits.
	t := new(big.Int)
	lo := i.Uint64()
	hi := t.Rsh(i, 64).Uint64()
	return Uint128{
		Lo: lo,
		Hi: hi,
	}, true
}

// Big returns 128-bit value as a *big.Int.
func (u Uint128) Big() *big.Int {
	i := new(big.Int).SetUint64(u.Hi)
	i = i.Lsh(i, 64)
	i = i.Or(i, new(big.Int).SetUint64(u.Lo))
	return i
}

// Equals returns true if two 128-bit values are equal.
// Uint128 values can be compared directly with == operator
// but use of the Equals method is preferred for consistency.
func (u Uint128) Equals(v Uint128) bool {
	return (u.Lo == v.Lo) && (u.Hi == v.Hi)
}

// Equals returns true if two 128-bit values are equal.
// Uint128 values can be compared directly with == operator
// but use of the Equals method is preferred for consistency.
func (u Uint128) Int128() Int128 {
	return Int128{
		Lo: u.Lo,
		Hi: int64(u.Hi),
	}
}

// Zero returns true if Uint128 value is zero.
func (u Uint128) Zero() bool {
	return u.Lo == 0 && u.Hi == 0
}

func (u Uint128) Uint64() uint64 {
	return u.Lo
}

func (u Uint128) String() string {
	// Check if the high part is 0, which simplifies the conversion
	if u.Hi == 0 {
		return strconv.FormatUint(u.Lo, 10)
	}

	return u.Big().String()
}

func (u Uint128) Append(b []byte) []byte {
	// Check if the high part is 0, which simplifies the conversion
	if u.Hi == 0 {
		return strconv.AppendUint(b, u.Lo, 10)
	}
	return u.Big().Append(b, 10)
}
