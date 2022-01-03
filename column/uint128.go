package column

func NewUint128(nullable bool) *Raw {
	return NewRaw(Uint128Size, nullable)
}
