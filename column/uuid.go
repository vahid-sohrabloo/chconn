package column

// UUID use for UUID ClickHouse DataType
type UUID struct {
	column
	val [16]byte
}

// NewUUID return new UUID for UUID ClickHouse DataType
func NewUUID(nullable bool) *UUID {
	return &UUID{
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        UUIDSize,
		},
	}
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP()
func (c *UUID) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.setVal(c.b[c.i : c.i+UUIDSize])
	c.i += UUIDSize
	return true
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *UUID) Row(row int) [16]byte {
	i := row * UUIDSize
	c.setVal(c.b[i : i+UUIDSize])
	return c.val
}

// Value of current pointer
//
// Use with Next()
func (c *UUID) Value() [16]byte {
	return c.val
}

// ReadAll read all value in this block and append to the input slice
func (c *UUID) ReadAll(value *[][16]byte) {
	for i := 0; i < c.totalByte; i += UUIDSize {
		c.setVal(c.b[i : i+UUIDSize])
		*value = append(*value,
			c.val)
	}
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *UUID) Fill(value [][16]byte) {
	for i := range value {
		c.setVal(c.b[c.i : c.i+UUIDSize])
		value[i] = c.val
		c.i += UUIDSize
	}
}

// ValueP Value of current pointer for nullable data
//
// As an alternative (for better performance), you can use `Value()` to get a value and `ValueIsNil()` to check if it is null.
//
// Use with Next()
func (c *UUID) ValueP() *[16]byte {
	if c.colNullable.b[(c.i-UUIDSize)/(UUIDSize)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *UUID) ReadAllP(value *[]*[16]byte) {
	for i := 0; i < c.totalByte; i += UUIDSize {
		if c.colNullable.b[i/UUIDSize] != 0 {
			*value = append(*value, nil)
			continue
		}
		c.setVal(c.b[i : i+UUIDSize])
		val := c.val
		*value = append(*value, &val)
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *UUID) FillP(value []*[16]byte) {
	for i := range value {
		if c.colNullable.b[c.i/UUIDSize] == 1 {
			value[i] = nil
			c.i += UUIDSize
			continue
		}
		c.setVal(c.b[c.i : c.i+UUIDSize])
		val := c.val
		value[i] = &val
		c.i += UUIDSize
	}
}

// Append value for insert
func (c *UUID) Append(v [16]byte) {
	c.numRow++
	c.writerData = append(c.writerData,
		v[7], v[6], v[5], v[4],
		v[3], v[2], v[1], v[0],
		v[15], v[14], v[13], v[12],
		v[11], v[10], v[9], v[8],
	)
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *UUID) AppendP(v *[16]byte) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *UUID) Reset() {
	c.column.Reset()
}

func (c *UUID) setVal(b []byte) {
	c.val[0], c.val[7] = b[7], b[0]
	c.val[1], c.val[6] = b[6], b[1]
	c.val[2], c.val[5] = b[5], b[2]
	c.val[3], c.val[4] = b[4], b[3]
	c.val[8], c.val[15] = b[15], b[8]
	c.val[9], c.val[14] = b[14], b[9]
	c.val[10], c.val[13] = b[13], b[10]
	c.val[11], c.val[12] = b[12], b[11]
}
