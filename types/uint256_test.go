package types

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUint256 unit tests for various Uint256 helpers.
func TestUint256Big(t *testing.T) {
	t.Run("FromBig", func(t *testing.T) {
		if got := Uint256FromBig(nil); !got.Equals(Uint256Zero()) {
			t.Fatalf("Uint256FromBig(nil) does not equal to 0, got %#x", got)
		}

		if got := Uint256FromBig(big.NewInt(-1)); !got.Equals(Uint256Zero()) {
			t.Fatalf("Uint256FromBig(-1) does not equal to 0, got %#x", got)
		}

		if got := Uint256FromBig(big.NewInt(124)).Big().String(); got != "124" {
			t.Fatalf("Uint256FromBig(big.NewInt(124)) does not equal to 0, got %#x", got)
		}

		if got := Uint256FromBig(new(big.Int).Lsh(big.NewInt(1), 257)); !got.Equals(Uint256Max()) {
			t.Fatalf("Uint256FromBig(2^257) does not equal to Max(), got %#x", got)
		}
		if got := Uint256FromBig(new(big.Int).Lsh(big.NewInt(1), 255)); got.String() != new(big.Int).Lsh(big.NewInt(1), 255).String() {
			t.Fatalf("Uint256FromBig(2^255) does not match the big got %s", got.String())
		}
	})
}

func TestUint256(t *testing.T) {
	d := Uint256From64(12_234)
	assert.Equal(t, "12234", string(d.Append([]byte{})))
	assert.Equal(t, "12234", d.String())

	d = Uint256From64(12_234_567_890)
	assert.Equal(t, "12234567890", string(d.Append([]byte{})))
	assert.Equal(t, "12234567890", d.String())

	d = Uint256From64(3)
	assert.Equal(t, "3", string(d.Append([]byte{})))
	assert.Equal(t, "3", d.String())

	d = Uint256From64(30)
	assert.Equal(t, "30", string(d.Append([]byte{})))
	assert.Equal(t, "30", d.String())
	assert.Equal(t, "30", d.Int256().String())
	assert.Equal(t, "30", d.Uint128().String())
	assert.Equal(t, uint64(30), d.Uint64())

	assert.Equal(t, "115792089237316195423570985008687907853269984665640564039457584007913129639935", string(Uint256Max().Append([]byte{})))
	assert.Equal(t, "115792089237316195423570985008687907853269984665640564039457584007913129639935", Uint256Max().String())
}
