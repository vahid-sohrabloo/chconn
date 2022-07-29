package types

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUint128 unit tests for various Uint128 helpers.
func TestUint128(t *testing.T) {
	t.Run("FromBig", func(t *testing.T) {
		if got := Uint128FromBig(nil); !got.Equals(Uint128Zero()) {
			t.Fatalf("Uint128FromBig(nil) does not equal to 0, got %#x", got)
		}

		if got := Uint128FromBig(big.NewInt(-1)); !got.Equals(Uint128Zero()) {
			t.Fatalf("Uint128FromBig(-1) does not equal to 0, got %#x", got)
		}

		if got := Uint256FromBig(big.NewInt(124)).Big().String(); got != "124" {
			t.Fatalf("Uint256FromBig(big.NewInt(124)) does not equal to 0, got %#x", got)
		}

		if got := Uint128FromBig(new(big.Int).Lsh(big.NewInt(1), 129)); !got.Equals(Uint128Max()) {
			t.Fatalf("Uint128FromBig(2^129) does not equal to Max(), got %#x", got)
		}
	})

	t.Run("ToBig", func(t *testing.T) {
		i := new(big.Int).SetInt64(124)
		assert.Equal(t, Uint256FromBig(i).Big().String(), "124")

		Uint256From64 := Uint256From64(124)
		assert.Equal(t, Uint256From64.Big().String(), "124")
	})
}
