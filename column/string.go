package column

// String is a column of String ClickHouse data type
type String struct {
	StringBase[string]
}

// NewString is a column of String ClickHouse data type
func NewString() *String {
	return &String{}
}

func (c *String) Elem(arrayLevel int, nullable, lc bool) ColumnCore {
	if lc {
		return elemOfLowCardinality(c.LowCardinality(), arrayLevel, nullable)
	}
	if nullable {
		return elemOfStringNullable(c.Nullable(), arrayLevel)
	}
	if arrayLevel > 0 {
		return elemOfArray(c.Array(), arrayLevel-1)
	}
	return c
}
