package column

func NewDecimal128(nullable bool) *Raw {
	return NewRaw(Decimal128Size, nullable)
}
