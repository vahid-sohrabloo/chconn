package column

// NewUint128 return new Raw for UInt128 ClickHouse DataType
func NewUint128(nullable bool) *Raw {
	return NewRaw(Uint128Size, nullable)
}
