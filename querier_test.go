package chconn

import (
	"testing"
)

func TestConnImplementsQuerier(t *testing.T) {
	// Compile-time check
	var _ Querier = (*conn)(nil)
}
