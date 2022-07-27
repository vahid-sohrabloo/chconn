package types

import (
	"math/big"
	"testing"
)

// TestUint256 unit tests for various Uint256 helpers.
func TestUint256(t *testing.T) {
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
			t.Fatalf("Uint256FromBig(2^129) does not equal to Max(), got %#x", got)
		}

	})
}
