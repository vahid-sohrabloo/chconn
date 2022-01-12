package column

// NewInt128 return new Raw for Int128 ClickHouse DataType
func NewInt128(nullable bool) *Raw {
	return NewRaw(Int128Size, nullable)
}
