package column

func NewInt256(nullable bool) *Raw {
	return NewRaw(Int256Size, nullable)
}
