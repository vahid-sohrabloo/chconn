package column

func NewDecimal256(nullable bool) *Raw {
	return NewRaw(Decimal256Size, nullable)
}
