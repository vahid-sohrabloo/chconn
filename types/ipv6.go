package types

import "net/netip"

type IPv6 [16]byte

func (ip IPv6) NetIP() netip.Addr {
	return netip.AddrFrom16(ip)
}

func IPv6FromAddr(ipAddr netip.Addr) IPv6 {
	return IPv6(ipAddr.As16())
}
