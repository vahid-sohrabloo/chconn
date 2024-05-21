package types

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUint256 unit tests for various Int256 helpers.
func TestInt256Big(t *testing.T) {
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

		i = new(big.Int).SetInt64(-124)
		assert.Equal(t, Int256FromBig(i).Big().String(), "-124")

		int256From64 := Int256From64(124)
		assert.Equal(t, int256From64.Big().String(), "124")
	})
}

func TestInt256(t *testing.T) {
	d := Int256From64(12_234)
	assert.Equal(t, "12234", string(d.Append([]byte{})))
	assert.Equal(t, "12234", d.String())

	d = Int256From64(12_234_567_890)
	assert.Equal(t, "12234567890", string(d.Append([]byte{})))
	assert.Equal(t, "12234567890", d.String())

	d = Int256From64(-12_234)
	assert.Equal(t, "-12234", string(d.Append([]byte{})))
	assert.Equal(t, "-12234", d.String())

	d = Int256From64(3)
	assert.Equal(t, "3", string(d.Append([]byte{})))
	assert.Equal(t, "3", d.String())

	d = Int256From64(30)
	assert.Equal(t, "30", string(d.Append([]byte{})))
	assert.Equal(t, "30", d.String())

	assert.Equal(t, uint64(30), d.Uint64())
	assert.Equal(t, "30", d.Uint128().String())
	assert.Equal(t, "30", d.Uint256().String())

	d = Int256From64(-3)
	assert.Equal(t, "-3", string(d.Append([]byte{})))
	assert.Equal(t, "-3", d.String())

	d = Int256From64(-30)
	assert.Equal(t, "-30", string(d.Append([]byte{})))
	assert.Equal(t, "-30", d.String())
}
