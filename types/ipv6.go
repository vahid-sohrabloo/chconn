package types

import "net/netip"

// IPv6 represents a ClickHouse IPv6 address as a 16-byte array.
// Use [IPv6FromAddr] to convert from [netip.Addr].
type IPv6 [16]byte

func (ip IPv6) NetIP() netip.Addr {
	return netip.AddrFrom16(ip)
}

// IPv6FromAddr converts a [netip.Addr] to a ClickHouse [IPv6] value.
func IPv6FromAddr(ipAddr netip.Addr) IPv6 {
	return IPv6(ipAddr.As16())
}

func (ip IPv6) MarshalText() ([]byte, error) {
	return []byte(ip.NetIP().String()), nil
}

func (ip IPv6) Append(b []byte) []byte {
	return ip.NetIP().AppendTo(b)
}

func (d IPv6) GetCHType() string {
	return "IPv6"
}
