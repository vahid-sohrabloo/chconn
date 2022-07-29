package column

// LowCardinalityNullable for LowCardinality(Nullable(T)) ClickHouse DataTypes
type LowCardinalityNullable[T comparable] struct {
	LowCardinality[T]
}

// NewLowCardinalityNullable return new LowCardinalityNullable for nullable LowCardinality ClickHouse DataTypes
func NewLowCardinalityNullable[T comparable](dictColumn Column[T]) *LowCardinalityNullable[T] {
	return NewLCNullable(dictColumn)
}

// NewLCNullable return new LowCardinalityNullable for nullable LowCardinality ClickHouse DataTypes
func NewLCNullable[T comparable](dictColumn Column[T]) *LowCardinalityNullable[T] {
	var empty T
	dictColumn.Append(empty)
	l := &LowCardinalityNullable[T]{
		LowCardinality: LowCardinality[T]{
			nullable:   true,
			dict:       make(map[T]int),
			dictColumn: dictColumn,
		},
	}
	return l
}

// Data get all nullable data in current block as a slice.
//
// NOTE: the return slice only valid in current block, if you want to use it after, you should copy it. or use Read
func (c *LowCardinalityNullable[T]) DataP() []*T {
	result := make([]*T, c.NumRow())
	for i, k := range c.readedKeys {
		if k == 0 {
			result[i] = nil
		} else {
			val := c.readedDict[k]
			result[i] = &val
		}
	}
	return result
}

// Read reads all nullable data in current block and append to the input.
func (c *LowCardinalityNullable[T]) ReadP(value []*T) []*T {
	for _, k := range c.readedKeys {
		if k == 0 {
			value = append(value, nil)
		} else {
			val := c.readedDict[k]
			value = append(value, &val)
		}
	}
	return value
}

// Row return nullable value of given row
// NOTE: Row number start from zero
func (c *LowCardinalityNullable[T]) RowP(row int) *T {
	if c.readedKeys[row] == 0 {
		return nil
	}
	val := c.readedDict[c.readedKeys[row]]
	return &val
}

// Append value for insert
func (c *LowCardinalityNullable[T]) Append(v T) {
	key, ok := c.dict[v]
	if !ok {
		key = len(c.dict)
		c.dict[v] = key
		c.dictColumn.Append(v)
	}
	c.keys = append(c.keys, key+1)
	c.numRow++
}

// Append nil value for insert
func (c *LowCardinalityNullable[T]) AppendNil() {
	c.keys = append(c.keys, 0)
	c.numRow++
}

// Append nullable value for insert
//
// as an alternative (for better performance), you can use `Append` and `AppendNil` to insert a value
func (c *LowCardinalityNullable[T]) AppendP(v *T) {
	if v == nil {
		c.AppendNil()
		return
	}
	c.Append(*v)
}

// AppendSlice append slice of value for insert
func (c *LowCardinalityNullable[T]) AppendSlice(vs []T) {
	for _, v := range vs {
		key, ok := c.dict[v]
		if !ok {
			key = len(c.dict)
			c.dict[v] = key
			c.dictColumn.Append(v)
		}
		c.keys = append(c.keys, key+1)
	}

	c.numRow += len(vs)
}

// AppendSlice append slice of nullable value for insert
func (c *LowCardinalityNullable[T]) AppendSliceP(vs []*T) {
	for _, v := range vs {
		if v == nil {
			c.keys = append(c.keys, 0)
			continue
		}
		key, ok := c.dict[*v]
		if !ok {
			key = len(c.dict)
			c.dict[*v] = key
			c.dictColumn.Append(*v)
		}
		c.keys = append(c.keys, key+1)
	}

	c.numRow += len(vs)
}

// Array return a Array type for this column
func (c *LowCardinalityNullable[T]) Array() *ArrayNullable[T] {
	return NewArrayNullable[T](c)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *LowCardinalityNullable[T]) Reset() {
	c.LowCardinality.Reset()
	var empty T
	c.dictColumn.Append(empty)
}

func (c *LowCardinalityNullable[T]) elem(arrayLevel int) ColumnBasic {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}
