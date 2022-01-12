package column

// NewDecimal256 return new Raw for Decimal256(3) ClickHouse DataType
func NewDecimal256(nullable bool) *Raw {
	return NewRaw(Decimal256Size, nullable)
}
