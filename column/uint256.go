package column

func NewUint256(nullable bool) *Raw {
	return NewRaw(Uint256Size, nullable)
}
