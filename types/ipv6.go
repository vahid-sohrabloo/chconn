package types

import "net/netip"

type IPv6 [16]byte

func (ip IPv6) NetIP() netip.Addr {
	return netip.AddrFrom16(ip)
}

func IPv6FromAddr(ipAddr netip.Addr) IPv6 {
	return IPv6(ipAddr.As16())
}

func (ip IPv6) MarshalText() ([]byte, error) {
	return []byte(ip.NetIP().String()), nil
}

func (ip IPv6) Append(b []byte) []byte {
	return ip.NetIP().AppendTo(b)
}
