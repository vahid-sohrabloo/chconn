package column

// String is a column of String ClickHouse data type
type String struct {
	StringBase[string]
}

// NewString is a column of String ClickHouse data type
func NewString() *String {
	return &String{}
}

func (c *String) Elem(arrayLevel int, nullable, lc bool) ColumnBasic {
	if lc {
		return c.LowCardinality().elem(arrayLevel, nullable)
	}
	if nullable {
		return c.Nullable().elem(arrayLevel)
	}
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}
