package column

// NewDecimal128 return new Raw for Decimal128(3) ClickHouse DataType
func NewDecimal128(nullable bool) *Raw {
	return NewRaw(Decimal128Size, nullable)
}
