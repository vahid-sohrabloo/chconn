package types

// UUID represents a ClickHouse UUID value stored in little-endian byte order.
// Use [UUIDFromBigEndian] to convert from standard big-endian UUID bytes.
type UUID [16]byte

// UUIDFromBigEndian converts a big-endian UUID byte array (standard format) to the
// little-endian format used by ClickHouse.
func UUIDFromBigEndian(b [16]byte) UUID {
	var val [16]byte
	val[0], val[7] = b[7], b[0]
	val[1], val[6] = b[6], b[1]
	val[2], val[5] = b[5], b[2]
	val[3], val[4] = b[4], b[3]
	val[8], val[15] = b[15], b[8]
	val[9], val[14] = b[14], b[9]
	val[10], val[13] = b[13], b[10]
	val[11], val[12] = b[12], b[11]
	return val
}

func (u UUID) BigEndian() [16]byte {
	return UUIDFromBigEndian(u)
}

const uuidHexDigits = "0123456789abcdef"

func (u UUID) Append(b []byte) []byte {
	be := u.BigEndian()
	for i, v := range be {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			b = append(b, '-')
		}
		b = append(b, uuidHexDigits[v>>4], uuidHexDigits[v&0x0f])
	}
	return b
}

func (d UUID) GetCHType() string {
	return "UUID"
}
