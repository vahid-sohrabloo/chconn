package column

// Map returns a Map column that uses the receiver as the key column and value
// as the value column. It is the builder-chain form of [NewMap], and infers
// both type parameters:
//
//	column.NewString().Map(column.New[uint64]())        // Map(String, UInt64)
//	column.NewString().LC().Map(column.New[uint64]())   // Map(LowCardinality(String), UInt64)
func (c *Base[T]) Map[V any](value Column[V]) *Map[T, V] {
	return NewMap[T, V](c, value)
}

// Map returns a Map column that uses the receiver as the key column.
// See [Base.Map].
func (c *StringBase[T]) Map[V any](value Column[V]) *Map[T, V] {
	return NewMap[T, V](c, value)
}

// Map returns a Map column that uses the receiver as the key column.
// See [Base.Map].
func (c *LowCardinality[T]) Map[V any](value Column[V]) *Map[T, V] {
	return NewMap[T, V](c, value)
}
