package types

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUint128 unit tests for various Uint128 helpers.
func TestUint128Big(t *testing.T) {
	t.Run("FromBig", func(t *testing.T) {
		if got := Uint128FromBig(nil); !got.Equals(Uint128Zero()) {
			t.Fatalf("Uint128FromBig(nil) does not equal to 0, got %#x", got)
		}

		if got := Uint128FromBig(big.NewInt(-1)); !got.Equals(Uint128Zero()) {
			t.Fatalf("Uint128FromBig(-1) does not equal to 0, got %#x", got)
		}

		if got := Uint128FromBig(big.NewInt(124)).String(); got != "124" {
			t.Fatalf("Uint128FromBig(big.NewInt(124)) does not equal to 0, got %#x", got)
		}

		if got := Uint128FromBig(new(big.Int).Lsh(big.NewInt(1), 129)); !got.Equals(Uint128Max()) {
			t.Fatalf("Uint128FromBig(2^129) does not equal to Max(), got %#x", got)
		}
	})

	t.Run("ToBig", func(t *testing.T) {
		i := new(big.Int).SetInt64(124)
		assert.Equal(t, Uint128FromBig(i).Big().String(), "124")

		Uint128From64 := Uint128From64(124)
		assert.Equal(t, Uint128From64.Big().String(), "124")
	})
}

func TestUint128(t *testing.T) {
	d := Uint128From64(12_234)
	assert.Equal(t, "12234", string(d.Append([]byte{})))
	assert.Equal(t, "12234", d.String())

	d = Uint128From64(12_234_567_890)
	assert.Equal(t, "12234567890", string(d.Append([]byte{})))
	assert.Equal(t, "12234567890", d.String())

	d = Uint128From64(3)
	assert.Equal(t, "3", string(d.Append([]byte{})))
	assert.Equal(t, "3", d.String())

	d = Uint128From64(30)
	assert.Equal(t, "30", string(d.Append([]byte{})))
	assert.Equal(t, "30", d.String())
	assert.Equal(t, "30", d.Int128().String())
	assert.Equal(t, uint64(30), d.Uint64())

	assert.Equal(t, "340282366920938463463374607431768211455", string(Uint128Max().Append([]byte{})))
	assert.Equal(t, "340282366920938463463374607431768211455", Uint128Max().String())
}
