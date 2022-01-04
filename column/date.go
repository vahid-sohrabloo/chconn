package column

import (
	"encoding/binary"
	"time"
)

type Date struct {
	column
	val  time.Time
	dict map[time.Time]int
	keys []int
}

func NewDate(nullable bool) *Date {
	return &Date{
		dict: make(map[time.Time]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        DateSize,
		},
	}
}

func (c *Date) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = time.Unix(int64(binary.LittleEndian.Uint16(c.b[c.i-c.size:c.i]))*daySeconds, 0)
	return true
}

func (c *Date) Value() time.Time {
	return c.val
}

func (c *Date) ReadAll(value *[]time.Time) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			time.Unix(int64(binary.LittleEndian.Uint16(c.b[i:i+c.size]))*daySeconds, 0))
	}
}

func (c *Date) Fill(value []time.Time) {
	for i := range value {
		value[i] = time.Unix(int64(binary.LittleEndian.Uint16(c.b[c.i:c.i+c.size]))*daySeconds, 0)
		c.i += c.size
	}
}

func (c *Date) ValueP() *time.Time {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Date) ReadAllP(value *[]*time.Time) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint16(c.b[i:i+c.size]))*daySeconds, 0)
		*value = append(*value, &val)
	}
}

func (c *Date) FillP(value []*time.Time) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint16(c.b[c.i:c.i+c.size]))*daySeconds, 0)
		value[i] = &val
		c.i += c.size
	}
}

func (c *Date) Append(v time.Time) {
	c.numRow++
	if v.Unix() <= 0 {
		c.writerData = append(c.writerData, emptyByte[:c.size]...)
		return
	}
	_, offset := v.Zone()
	timestamp := (v.Unix() + int64(offset)) / daySeconds
	c.writerData = append(c.writerData,
		byte(timestamp),
		byte(timestamp>>8),
	)
}

func (c *Date) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Date) AppendP(v *time.Time) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Date) AppendDict(v time.Time) {
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

func (c *Date) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *Date) AppendDictP(v *time.Time) {
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

func (c *Date) Keys() []int {
	return c.keys
}

func (c *Date) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[time.Time]int)
}
