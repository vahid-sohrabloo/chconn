package helper

import (
	"testing"
)

func TestNumberByteForUvarint(t *testing.T) {
	testCases := []struct {
		name     string
		input    uint64
		expected int
	}{
		{"zero", 0, 1},
		{"one byte min", 1, 1},
		{"one byte max", 127, 1},
		{"two bytes min", 128, 2},
		{"two bytes mid", 8192, 2},
		{"two bytes max", 16383, 2},
		{"three bytes min", 16384, 3},
		{"three bytes max", 2097151, 3},
		{"four bytes min", 2097152, 4},
		{"four bytes max", 268435455, 4},
		{"five bytes min", 268435456, 5},
		{"boundary 2^32-1", 4294967295, 5},
		{"boundary 2^32", 4294967296, 5},
		{"large value", 12345678901234, 7},
		{"maximum uint64", ^uint64(0), 10}, // All bits set to 1
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := NumberByteForUvarint(tc.input)
			if result != tc.expected {
				t.Errorf("For input %d, expected %d bytes, got %d bytes",
					tc.input, tc.expected, result)
			}
		})
	}
}
