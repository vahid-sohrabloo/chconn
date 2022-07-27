package types

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIP(t *testing.T) {
	ipv4 := IPv4FromAddr(netip.AddrFrom4([4]byte{1, 2, 3, 4}))
	assert.Equal(t, ipv4.NetIP().As4(), [4]byte{1, 2, 3, 4})
	ipv6 := IPv6FromAddr(netip.AddrFrom16([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	assert.Equal(t, ipv6.NetIP().As16(), [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
}
