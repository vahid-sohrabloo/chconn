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

func TestChTime64(t *testing.T) {
	// Precision 3 (milliseconds)
	ct := NewChTime64(12, 30, 45, 123, 3)
	assert.Equal(t, "12:30:45.123", ct.String(3))
	assert.Equal(t, 12, ct.Hours(3))
	assert.Equal(t, 30, ct.Minutes(3))
	assert.Equal(t, 45, ct.Seconds(3))
	assert.Equal(t, int64(123), ct.SubSeconds(3))

	// Precision 0 (seconds, like ChTime)
	ct0 := NewChTime64(1, 2, 3, 0, 0)
	assert.Equal(t, "1:02:03", ct0.String(0))

	// Precision 6 (microseconds)
	ct6 := NewChTime64(0, 0, 1, 500000, 6)
	assert.Equal(t, "0:00:01.500000", ct6.String(6))

	// Negative
	neg := ChTime64(-3723500) // precision 3: -3723.500 seconds
	assert.Equal(t, "-1:02:03.500", neg.String(3))

	// Zero
	assert.Equal(t, "0:00:00.000", ChTime64(0).String(3))

	// Float64 conversion
	ct2 := NewChTime64FromFloat64(90.5, 3) // 1 min 30.5 sec
	assert.InDelta(t, 90.5, ct2.Float64(3), 0.001)
	assert.Equal(t, "0:01:30.500", ct2.String(3))
}
