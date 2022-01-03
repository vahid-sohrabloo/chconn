package column

func NewInt128(nullable bool) *Raw {
	return NewRaw(Int128Size, nullable)
}
