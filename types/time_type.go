package types

import (
	"fmt"
	"math"
)

// ChTime represents a ClickHouse Time value — seconds since midnight.
// Range: -999:59:59 to 999:59:59 (allows values beyond 24 hours).
type ChTime int32

// Hours returns the hours component.
func (t ChTime) Hours() int {
	return int(t) / 3600
}

// Minutes returns the minutes component (0-59).
func (t ChTime) Minutes() int {
	s := int(t)
	if s < 0 {
		s = -s
	}
	return (s / 60) % 60
}

// Seconds returns the seconds component (0-59).
func (t ChTime) Seconds() int {
	s := int(t)
	if s < 0 {
		s = -s
	}
	return s % 60
}

// String returns the time in HH:MM:SS format.
func (t ChTime) String() string {
	sign := ""
	s := int(t)
	if s < 0 {
		sign = "-"
		s = -s
	}
	h := s / 3600
	m := (s / 60) % 60
	sec := s % 60
	return fmt.Sprintf("%s%d:%02d:%02d", sign, h, m, sec)
}

// NewChTime creates a ChTime from hours, minutes, seconds.
func NewChTime(hours, minutes, seconds int) ChTime {
	total := hours*3600 + minutes*60 + seconds
	return ChTime(total)
}

// ChTime64 represents a ClickHouse Time64 value — sub-second time since midnight.
// Stored as int64 ticks, where the tick resolution depends on precision (0-9).
// Precision 0 = seconds, 3 = milliseconds, 6 = microseconds, 9 = nanoseconds.
type ChTime64 int64

// precisionFactors maps precision (0-9) to ticks-per-second.
var precisionFactors = [10]int64{1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000}

// Hours returns the hours component.
func (t ChTime64) Hours(precision int) int {
	return int(t.totalSeconds(precision)) / 3600
}

// Minutes returns the minutes component (0-59).
func (t ChTime64) Minutes(precision int) int {
	s := t.totalSeconds(precision)
	if s < 0 {
		s = -s
	}
	return int(s/60) % 60
}

// Seconds returns the seconds component (0-59).
func (t ChTime64) Seconds(precision int) int {
	s := t.totalSeconds(precision)
	if s < 0 {
		s = -s
	}
	return int(s) % 60
}

// SubSeconds returns the fractional part as ticks (depends on precision).
func (t ChTime64) SubSeconds(precision int) int64 {
	factor := precisionFactors[precision]
	v := int64(t)
	if v < 0 {
		v = -v
	}
	return v % factor
}

func (t ChTime64) totalSeconds(precision int) int64 {
	return int64(t) / precisionFactors[precision]
}

// String returns the time in HH:MM:SS.fff format with the given precision.
func (t ChTime64) String(precision int) string {
	sign := ""
	v := int64(t)
	if v < 0 {
		sign = "-"
		v = -v
	}
	factor := precisionFactors[precision]
	totalSec := v / factor
	frac := v % factor

	h := totalSec / 3600
	m := (totalSec / 60) % 60
	s := totalSec % 60

	if precision == 0 {
		return fmt.Sprintf("%s%d:%02d:%02d", sign, h, m, s)
	}
	fracStr := fmt.Sprintf("%0*d", precision, frac)
	return fmt.Sprintf("%s%d:%02d:%02d.%s", sign, h, m, s, fracStr)
}

// Float64 returns the time as seconds with fractional part.
func (t ChTime64) Float64(precision int) float64 {
	return float64(t) / float64(precisionFactors[precision])
}

// NewChTime64 creates a ChTime64 from hours, minutes, seconds, and fractional ticks.
func NewChTime64(hours, minutes, seconds int, subSeconds int64, precision int) ChTime64 {
	factor := precisionFactors[precision]
	total := int64(hours)*3600*factor + int64(minutes)*60*factor + int64(seconds)*factor + subSeconds
	return ChTime64(total)
}

// NewChTime64FromFloat64 creates a ChTime64 from a float64 seconds value.
func NewChTime64FromFloat64(seconds float64, precision int) ChTime64 {
	return ChTime64(math.Round(seconds * float64(precisionFactors[precision])))
}
