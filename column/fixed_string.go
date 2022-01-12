package column

// NewFixedString return new Raw for FixedString ClickHouse DataType
func NewFixedString(size int, nullable bool) *Raw {
	return NewRaw(size, nullable)
}
