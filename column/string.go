package column

import (
	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

// String use for String ClickHouse DataType
type String struct {
	column
	dict map[string]int
	keys []int
	val  []byte
	vals [][]byte
}

// NewString return new String for String ClickHouse DataType
func NewString(nullable bool) *String {
	return &String{
		dict: make(map[string]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        0,
		},
	}
}

// ReadRaw read raw data from the reader. it runs automatically when you call `NextColumn()`
func (c *String) ReadRaw(num int, r *readerwriter.Reader) error {
	err := c.column.ReadRaw(num, r)
	if err != nil {
		return err
	}
	if cap(c.vals) < num {
		c.vals = make([][]byte, num)
	} else {
		c.vals = c.vals[:num]
	}
	for i := 0; i < num; i++ {
		l, err := c.r.Uvarint()
		if err != nil {
			return err
		}
		if cap(c.vals[i]) < int(l) {
			c.vals[i] = make([]byte, l)
		} else {
			c.vals[i] = c.vals[i][:l]
		}
		_, err = c.r.Read(c.vals[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Next forward pointer to the next value. Returns false if there are no more values.
//
// Use with Value() or ValueP() or ValueString() or ValueStringP()
func (c *String) Next() bool {
	if c.i >= c.numRow {
		return false
	}
	c.val = c.vals[c.i]
	c.i++
	return true
}

// Value of current pointer
//
// Use with Next()
func (c *String) Value() []byte {
	return c.val
}

// ValueString string value of current pointer
//
// Use with Next()
func (c *String) ValueString() string {
	return string(c.val)
}

// ValueP value of current pointer
//
// Use with Next()
func (c *String) ValueP() *[]byte {
	if c.colNullable.b[c.i-1] == 1 {
		return nil
	}
	val := make([]byte, len(c.val))
	copy(val, c.val)
	return &val
}

// ValueStringP string value of current pointer
//
// Use with Next()
func (c *String) ValueStringP() *string {
	if c.colNullable.b[c.i-1] == 1 {
		return nil
	}
	val := string(c.val)
	return &val
}

// ReadAll read all value in this block and append to the input slice
func (c *String) ReadAll(value *[][]byte) {
	for i := 0; i < c.numRow; i++ {
		str := c.vals[i]
		*value = append(*value, str)
	}
}

// ReadAllString read all string value in this block and append to the input slice
func (c *String) ReadAllString(value *[]string) {
	for i := 0; i < c.numRow; i++ {
		str := c.vals[i]
		*value = append(*value, string(str))
	}
}

// ReadAllP read all value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAll()` to get a values and `ReadAllNil()` to check if they are null.
func (c *String) ReadAllP(value *[]*[]byte) {
	for i := 0; i < c.numRow; i++ {
		if c.colNullable.b[i] != 0 {
			*value = append(*value, nil)
			continue
		}
		str := c.vals[i]
		val := make([]byte, len(str))
		copy(val, str)
		*value = append(*value, &val)
	}
}

// ReadAllStringP read all string value in this block and append to the input slice (for nullable data)
//
// As an alternative (for better performance), you can use `ReadAllString()` to get a values and `ReadAllNil()` to check if they are null.
func (c *String) ReadAllStringP(value *[]*string) {
	for i := 0; i < c.numRow; i++ {
		if c.colNullable.b[i] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := string(c.vals[i])
		*value = append(*value, &val)
	}
}

// Fill slice with value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *String) Fill(value [][]byte) {
	for i := range value {
		val := c.vals[c.i]
		value[i] = val
		c.i++
	}
}

// FillString slice with string value and forward the pointer by the length of the slice
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *String) FillString(value []string) {
	for i := range value {
		val := c.vals[c.i]
		value[i] = string(val)
		c.i++
	}
}

// FillP slice with value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `Fill()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *String) FillP(value []*[]byte) {
	for i := range value {
		if c.colNullable.b[c.i] != 0 {
			c.i++
			value[i] = nil
			continue
		}
		val := c.vals[c.i]
		value[i] = &val
		c.i++
	}
}

// FillStringP slice with string value and forward the pointer by the length of the slice (for nullable data)
//
// As an alternative (for better performance), you can use `FillString()` to get a values and `FillNil()` to check if they are null.
//
// NOTE: A slice that is longer than the remaining data is not safe to pass.
func (c *String) FillStringP(value []*string) {
	for i := range value {
		if c.colNullable.b[c.i] != 0 {
			c.i++
			value[i] = nil
			continue
		}
		val := string(c.vals[c.i])
		value[i] = &val
		c.i++
	}
}

func (c *String) appendLen(x int) {
	i := 0
	for x >= 0x80 {
		c.writerData = append(c.writerData, byte(x)|0x80)
		x >>= 7
		i++
	}
	c.writerData = append(c.writerData, byte(x))
}

// Append value for insert
func (c *String) Append(v []byte) {
	c.numRow++
	c.appendLen(len(v))
	c.writerData = append(c.writerData, v...)
}

// AppendString append string value for insert
func (c *String) AppendString(v string) {
	c.numRow++
	c.appendLen(len(v))
	c.writerData = append(c.writerData, v...)
}

// AppendP value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `Append` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *String) AppendP(v *[]byte) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

// AppendStringP append string value for insert (for nullable column)
//
// As an alternative (for better performance), you can use `AppendString` to append data. and `AppendIsNil` to say this value is null or not
//
// NOTE: for alternative mode. of your value is nil you still need to append default value. You can use `AppendEmpty()` for nil values
func (c *String) AppendStringP(v *string) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.AppendString(*v)
}

// AppendEmpty append empty value for insert
func (c *String) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, 0)
}

// AppendDict add value to the dictionary (if doesn't exist on dictionary) and append key of the dictionary to keys
//
// Only use for LowCardinality data type
func (c *String) AppendDict(v []byte) {
	key, ok := c.dict[string(v)]
	if !ok {
		key = len(c.dict)
		c.dict[string(v)] = key
		c.Append(v)
	}
	if c.nullable {
		c.keys = append(c.keys, key+1)
	} else {
		c.keys = append(c.keys, key)
	}
}

// AppendStringDict add string value to the dictionary (if doesn't exist on dictionary) and append key of the dictionary to keys
//
// Only use for LowCardinality data type
func (c *String) AppendStringDict(v string) {
	key, ok := c.dict[v]
	if !ok {
		key = len(c.dict)
		c.dict[v] = key
		c.AppendString(v)
	}
	if c.nullable {
		c.keys = append(c.keys, key+1)
	} else {
		c.keys = append(c.keys, key)
	}
}

// AppendDictNil add nil key for LowCardinality nullable data type
func (c *String) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

// AppendDictP add string value to the dictionary (if doesn't exist on dictionary)
// and append key of the dictionary to keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *String) AppendDictP(v *[]byte) {
	if v == nil {
		c.keys = append(c.keys, 0)
		return
	}
	key, ok := c.dict[string(*v)]
	if !ok {
		key = len(c.dict)
		c.dict[string(*v)] = key
		c.Append(*v)
	}
	c.keys = append(c.keys, key+1)
}

// AppendStringDictP add string value to the dictionary (if doesn't exist on dictionary)
// and append key of the dictionary to keys (for nullable data type)
//
// As an alternative (for better performance), You can use `AppendStringDict()` and `AppendDictNil` instead of this function.
//
// For alternative way You shouldn't append empty value for nullable data
func (c *String) AppendStringDictP(v *string) {
	if v == nil {
		c.keys = append(c.keys, 0)
		return
	}
	key, ok := c.dict[*v]
	if !ok {
		key = len(c.dict)
		c.dict[*v] = key
		c.AppendString(*v)
	}
	c.keys = append(c.keys, key+1)
}

// Keys current keys for LowCardinality data type
func (c *String) Keys() []int {
	return c.keys
}

// Reset all status and buffer data
//
// Reading data does not require a reset after each read. The reset will be triggered automatically.
//
// However, writing data requires a reset after each write.
func (c *String) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[string]int)
}
