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

func TestDecimal32(t *testing.T) {
	d := Decimal32(0)
	assert.Equal(t, "0", string(d.Append(3, []byte{})))
	assert.Equal(t, "0", d.String(3))
	assert.Equal(t, float64(0), d.Float64(3))

	d = Decimal32(12_234)
	assert.Equal(t, "12.234", string(d.Append(3, []byte{})))
	assert.Equal(t, "12.234", d.String(3))

	d = Decimal32(12_234_890)
	assert.Equal(t, "12234.89", string(d.Append(3, []byte{})))
	assert.Equal(t, "12234.89", d.String(3))

	d = Decimal32(-12_234)
	assert.Equal(t, "-12.234", string(d.Append(3, []byte{})))
	assert.Equal(t, "-12.234", d.String(3))

	d = Decimal32(3)
	assert.Equal(t, "0.003", string(d.Append(3, []byte{})))
	assert.Equal(t, "0.003", d.String(3))

	d = Decimal32(30)
	assert.Equal(t, "0.03", string(d.Append(3, []byte{})))
	assert.Equal(t, "0.03", d.String(3))

	d = Decimal32(-3)
	assert.Equal(t, "-0.003", string(d.Append(3, []byte{})))
	assert.Equal(t, "-0.003", d.String(3))

	d = Decimal32(-30)
	assert.Equal(t, "-0.03", string(d.Append(3, []byte{})))
	assert.Equal(t, "-0.03", d.String(3))

	d = Decimal32(-30000)
	assert.Equal(t, "-30", string(d.Append(3, []byte{})))
	assert.Equal(t, "-30", d.String(3))
}

func TestDecimal64(t *testing.T) {
	d := Decimal64(0)
	assert.Equal(t, "0", string(d.Append(3, []byte{})))
	assert.Equal(t, "0", d.String(3))
	assert.Equal(t, float64(0), d.Float64(3))

	d = Decimal64(12_234)
	assert.Equal(t, "12.234", string(d.Append(3, []byte{})))
	assert.Equal(t, "12.234", d.String(3))

	d = Decimal64(12_234_567_890)
	assert.Equal(t, "12234567.89", string(d.Append(3, []byte{})))
	assert.Equal(t, "12234567.89", d.String(3))

	d = Decimal64(-12_234)
	assert.Equal(t, "-12.234", string(d.Append(3, []byte{})))
	assert.Equal(t, "-12.234", d.String(3))

	d = Decimal64(3)
	assert.Equal(t, "0.003", string(d.Append(3, []byte{})))
	assert.Equal(t, "0.003", d.String(3))

	d = Decimal64(30)
	assert.Equal(t, "0.03", string(d.Append(3, []byte{})))
	assert.Equal(t, "0.03", d.String(3))

	d = Decimal64(-3)
	assert.Equal(t, "-0.003", string(d.Append(3, []byte{})))
	assert.Equal(t, "-0.003", d.String(3))

	d = Decimal64(-30)
	assert.Equal(t, "-0.03", string(d.Append(3, []byte{})))
	assert.Equal(t, "-0.03", d.String(3))

	d = Decimal64(-30000)
	assert.Equal(t, "-30", string(d.Append(3, []byte{})))
	assert.Equal(t, "-30", d.String(3))
}

func TestDecimal128(t *testing.T) {
	d := Decimal128(Int128From64(0))
	assert.Equal(t, "0", string(d.Append(3, []byte{})))
	assert.Equal(t, "0", d.String(3))
	assert.Equal(t, float64(0), d.Float64(3))

	d = Decimal128(Int128From64(12_234))
	assert.Equal(t, "12.234", string(d.Append(3, []byte{})))
	assert.Equal(t, "12.234", d.String(3))

	assert.Equal(t, 12.234, d.Float64(3))

	d = Decimal128(Int128From64(12_234_567_890))
	assert.Equal(t, "12234567.89", string(d.Append(3, []byte{})))
	assert.Equal(t, "12234567.89", d.String(3))

	d = Decimal128(Int128From64(-12_234))
	assert.Equal(t, "-12.234", string(d.Append(3, []byte{})))
	assert.Equal(t, "-12.234", d.String(3))

	d = Decimal128(Int128From64(3))
	assert.Equal(t, "0.003", string(d.Append(3, []byte{})))
	assert.Equal(t, "0.003", d.String(3))

	d = Decimal128(Int128From64(30))
	assert.Equal(t, "0.03", string(d.Append(3, []byte{})))
	assert.Equal(t, "0.03", d.String(3))

	d = Decimal128(Int128From64(-3))
	assert.Equal(t, "-0.003", string(d.Append(3, []byte{})))
	assert.Equal(t, "-0.003", d.String(3))

	d = Decimal128(Int128From64(-30))
	assert.Equal(t, "-0.03", string(d.Append(3, []byte{})))
	assert.Equal(t, "-0.03", d.String(3))

	d = Decimal128(Int128From64(-30000))
	assert.Equal(t, "-30", string(d.Append(3, []byte{})))
	assert.Equal(t, "-30", d.String(3))
	assert.Equal(t, "-30", d.ToInt128(3).String())
}

func TestDecimal256(t *testing.T) {
	d := Decimal256(Int256From64(0))
	assert.Equal(t, "0", string(d.Append(3, []byte{})))
	assert.Equal(t, "0", d.String(3))
	assert.Equal(t, float64(0), d.Float64(3))

	d = Decimal256(Int256From64(12_234))
	assert.Equal(t, "12.234", string(d.Append(3, []byte{})))
	assert.Equal(t, "12.234", d.String(3))
	assert.Equal(t, 12.234, d.Float64(3))

	d = Decimal256(Int256From64(12_234_567_890))
	assert.Equal(t, "12234567.89", string(d.Append(3, []byte{})))
	assert.Equal(t, "12234567.89", d.String(3))

	d = Decimal256(Int256From64(-12_234))
	assert.Equal(t, "-12.234", string(d.Append(3, []byte{})))
	assert.Equal(t, "-12.234", d.String(3))

	d = Decimal256(Int256From64(3))
	assert.Equal(t, "0.003", string(d.Append(3, []byte{})))
	assert.Equal(t, "0.003", d.String(3))

	d = Decimal256(Int256From64(30))
	assert.Equal(t, "0.03", string(d.Append(3, []byte{})))
	assert.Equal(t, "0.03", d.String(3))

	d = Decimal256(Int256From64(-3))
	assert.Equal(t, "-0.003", string(d.Append(3, []byte{})))
	assert.Equal(t, "-0.003", d.String(3))

	d = Decimal256(Int256From64(-30))
	assert.Equal(t, "-0.03", string(d.Append(3, []byte{})))
	assert.Equal(t, "-0.03", d.String(3))

	d = Decimal256(Int256From64(-30000))
	assert.Equal(t, "-30", string(d.Append(3, []byte{})))
	assert.Equal(t, "-30", d.String(3))
	assert.Equal(t, "-30", d.ToInt256(3).String())
}
