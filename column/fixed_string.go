package column

type FixedString struct {
	Raw
}

// NewFixedString return new FixedString for FixedString ClickHouse DataType
func NewFixedString(size int, nullable bool) *FixedString {
	return &FixedString{
		Raw: Raw{
			dict: make(map[string]int),
			column: column{
				nullable:    nullable,
				colNullable: newNullable(),
				size:        size,
			},
		},
	}
}
