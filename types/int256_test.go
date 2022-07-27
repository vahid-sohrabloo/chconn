package types

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUint256 unit tests for various Int256 helpers.
func TestInt256(t *testing.T) {
	t.Run("FromBig", func(t *testing.T) {
		if got := Int256FromBig(nil); !got.Equals(Int256Zero()) {
			t.Fatalf("FromBig(nil) does not equal to 0, got %#x", got)
		}

		if got := Int256FromBig(new(big.Int).Lsh(big.NewInt(1), 257)); !got.Equals(Int256Max()) {
			t.Fatalf("FromBig(2^129) does not equal to Max(), got %#x", got)
		}
	})
	t.Run("ToBig", func(t *testing.T) {
		i := new(big.Int).SetInt64(124)
		assert.Equal(t, Int256FromBig(i).Big().String(), "124")

		int256From64 := Int256From64(124)
		assert.Equal(t, int256From64.Big().String(), "124")
	})
}
