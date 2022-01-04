package column

import (
	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

type String struct {
	column
	dict map[string]int
	keys []int
	val  []byte
	vals [][]byte
}

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
	var str []byte
	for i := 0; i < num; i++ {
		str, err = c.r.ByteString()
		if err != nil {
			return err
		}
		c.vals[i] = str
	}
	return nil
}

func (c *String) Next() bool {
	if c.i >= c.numRow {
		return false
	}
	c.val = c.vals[c.i]
	c.i++
	return true
}

func (c *String) Value() []byte {
	return c.val
}

func (c *String) ValueString() string {
	return string(c.val)
}

func (c *String) ValueP() *[]byte {
	if c.colNullable.b[c.i-1] == 1 {
		return nil
	}
	val := make([]byte, len(c.val))
	copy(val, c.val)
	return &val
}

func (c *String) ValueStringP() *string {
	if c.colNullable.b[c.i-1] == 1 {
		return nil
	}
	val := string(c.val)
	return &val
}

func (c *String) ReadAll(value *[][]byte) {
	for i := 0; i < c.numRow; i++ {
		str := c.vals[i]
		*value = append(*value, str)
	}
}

func (c *String) ReadAllString(value *[]string) {
	for i := 0; i < c.numRow; i++ {
		str := c.vals[i]
		*value = append(*value, string(str))
	}
}

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

func (c *String) Fill(value [][]byte) {
	for i := range value {
		val := c.vals[c.i]
		value[i] = val
		c.i++
	}
}

func (c *String) FillString(value []string) {
	for i := range value {
		val := c.vals[c.i]
		value[i] = string(val)
		c.i++
	}
}

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

func (c *String) Append(v []byte) {
	c.numRow++
	c.appendLen(len(v))
	c.writerData = append(c.writerData, v...)
}

func (c *String) AppendString(v string) {
	c.numRow++
	c.appendLen(len(v))
	c.writerData = append(c.writerData, v...)
}

func (c *String) AppendP(v *[]byte) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *String) AppendStringP(v *string) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.AppendString(*v)
}

func (c *String) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, 0)
}

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

func (c *String) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

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

func (c *String) Keys() []int {
	return c.keys
}

func (c *String) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[string]int)
}
