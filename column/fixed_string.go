package column

func NewFixedString(size int, nullable bool) *Raw {
	return NewRaw(size, nullable)
}
