package types

import "fmt"

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
