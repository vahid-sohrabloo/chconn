package types

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUint128 unit tests for various Int128 helpers.
func TestInt128(t *testing.T) {
	t.Run("FromBig", func(t *testing.T) {
		if got := Int128FromBig(nil); !got.Equals(Int128Zero()) {
			t.Fatalf("Int128FromBig(nil) does not equal to 0, got %#x", got)
		}

		if got := Int128FromBig(new(big.Int).Lsh(big.NewInt(1), 129)); !got.Equals(Int128Max()) {
			t.Fatalf("Int128FromBig(2^129) does not equal to Max(), got %#x", got)
		}
	})
	t.Run("ToBig", func(t *testing.T) {
		i := new(big.Int).SetInt64(-124)
		assert.Equal(t, Int128FromBig(i).Big().String(), "-124")

		int128From64 := Int128From64(-124)
		assert.Equal(t, int128From64.Big().String(), "-124")
	})
}
