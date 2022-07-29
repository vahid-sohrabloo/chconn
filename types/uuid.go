package types

type UUID [16]byte

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
