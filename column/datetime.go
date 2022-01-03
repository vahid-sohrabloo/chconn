package column

import (
	"encoding/binary"
	"time"
)

type DateTime struct {
	column
	val  time.Time
	dict map[time.Time]int
	keys []int
}

func NewDateTime(nullable bool) *DateTime {
	return &DateTime{
		dict: make(map[time.Time]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        DatetimeSize,
		},
	}
}

func (c *DateTime) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i-c.size:c.i])), 0)
	return true
}

func (c *DateTime) Value() time.Time {
	return c.val
}

func (c *DateTime) ReadAll(value *[]time.Time) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+c.size])), 0))
	}
}

func (c *DateTime) Fill(value []time.Time) {
	for i := range value {
		value[i] = time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i:c.i+c.size])), 0)
		c.i += c.size
	}
}

func (c *DateTime) ValueP() *time.Time {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *DateTime) ReadAllP(value *[]*time.Time) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+c.size])), 0)
		*value = append(*value, &val)
	}
}

func (c *DateTime) FillP(value []*time.Time) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i:c.i+c.size])), 0)
		value[i] = &val
		c.i += c.size
	}
}

func (c *DateTime) Append(v time.Time) {
	c.numRow++
	if v.Unix() <= 0 {
		c.writerData = append(c.writerData, emptyByte[:c.size]...)
		return
	}
	timestamp := v.Unix()
	c.writerData = append(c.writerData,
		byte(timestamp),
		byte(timestamp>>8),
		byte(timestamp>>16),
		byte(timestamp>>24),
	)
}

func (c *DateTime) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *DateTime) AppendP(v *time.Time) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *DateTime) AppendDict(v time.Time) {
	key, ok := c.dict[v]
	if !ok {
		key = len(c.dict)
		c.dict[v] = key
		c.Append(v)
	}
	if c.nullable {
		c.keys = append(c.keys, key+1)
	} else {
		c.keys = append(c.keys, key)
	}
}

func (c *DateTime) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *DateTime) AppendDictP(v *time.Time) {
	if v == nil {
		c.keys = append(c.keys, 0)
		return
	}
	key, ok := c.dict[*v]
	if !ok {
		key = len(c.dict)
		c.dict[*v] = key
		c.Append(*v)
	}
	c.keys = append(c.keys, key+1)
}

func (c *DateTime) Keys() []int {
	return c.keys
}

func (c *DateTime) resetDict() {
	c.keys = c.keys[:0]
	c.dict = make(map[time.Time]int)
}
