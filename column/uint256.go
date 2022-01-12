package column

// NewUint256 return new Raw for UInt256 ClickHouse DataType
func NewUint256(nullable bool) *Raw {
	return NewRaw(Uint256Size, nullable)
}
