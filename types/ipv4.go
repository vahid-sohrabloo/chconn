package types

import "net/netip"

//  IPv4 is a compatible type for IPv4 address in clickhouse.
// clickhouse use Little endian for IPv4. but golang use big endian
type IPv4 [4]byte

func (ip IPv4) NetIP() netip.Addr {
	return netip.AddrFrom4([4]byte{ip[3], ip[2], ip[1], ip[0]})
}

func IPv4FromAddr(ipAddr netip.Addr) IPv4 {
	ip := ipAddr.As4()
	return IPv4{ip[3], ip[2], ip[1], ip[0]}
}
