package column

// NewInt256 return new Raw for Int256 ClickHouse DataType
func NewInt256(nullable bool) *Raw {
	return NewRaw(Int256Size, nullable)
}
