package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChTime(t *testing.T) {
	ct := NewChTime(12, 30, 45)
	assert.Equal(t, "12:30:45", ct.String())
	assert.Equal(t, 12, ct.Hours())
	assert.Equal(t, 30, ct.Minutes())
	assert.Equal(t, 45, ct.Seconds())

	// Negative time
	neg := ChTime(-3723) // -1:02:03
	assert.Equal(t, "-1:02:03", neg.String())

	// Zero
	assert.Equal(t, "0:00:00", ChTime(0).String())

	// Large value (beyond 24 hours)
	big := NewChTime(100, 30, 15)
	assert.Equal(t, "100:30:15", big.String())
}
