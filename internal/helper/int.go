package helper

func NumberByteForUvarint(s uint64) int {
	if s == 0 {
		return 1
	}

	bytes := 0
	for s > 0 {
		bytes++
		s >>= 7
	}
	return bytes
}
