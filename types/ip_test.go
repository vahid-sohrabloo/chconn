package types

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIP(t *testing.T) {
	ipv4 := IPv4FromAddr(netip.AddrFrom4([4]byte{1, 2, 3, 4}))
	assert.Equal(t, ipv4.NetIP().As4(), [4]byte{1, 2, 3, 4})
	ipString, err := ipv4.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "1.2.3.4", string(ipString))
	assert.Equal(t, "1.2.3.4", string(ipv4.Append([]byte{})))
	ipv6 := IPv6FromAddr(netip.AddrFrom16([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	assert.Equal(t, ipv6.NetIP().As16(), [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	ipString, err = ipv6.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "102:304:506:708:90a:b0c:d0e:f10", string(ipString))
	assert.Equal(t, "102:304:506:708:90a:b0c:d0e:f10", string(ipv6.Append([]byte{})))
}
