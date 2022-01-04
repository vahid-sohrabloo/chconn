package column

import (
	"encoding/binary"
	"time"
)

type Date32 struct {
	column
	val  time.Time
	dict map[time.Time]int
	keys []int
}

func NewDate32(nullable bool) *Date32 {
	return &Date32{
		dict: make(map[time.Time]int),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Date32Size,
		},
	}
}

func (c *Date32) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i-c.size:c.i]))*daySeconds, 0)
	return true
}

func (c *Date32) Value() time.Time {
	return c.val
}

func (c *Date32) ReadAll(value *[]time.Time) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+c.size]))*daySeconds, 0))
	}
}

func (c *Date32) Fill(value []time.Time) {
	for i := range value {
		value[i] = time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i:c.i+c.size]))*daySeconds, 0)
		c.i += c.size
	}
}

func (c *Date32) ValueP() *time.Time {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *Date32) ReadAllP(value *[]*time.Time) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[i:i+c.size]))*daySeconds, 0)
		*value = append(*value, &val)
	}
}

func (c *Date32) FillP(value []*time.Time) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := time.Unix(int64(binary.LittleEndian.Uint32(c.b[c.i:c.i+c.size]))*daySeconds, 0)
		value[i] = &val
		c.i += c.size
	}
}

func (c *Date32) Append(v time.Time) {
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
		byte(timestamp>>16),
		byte(timestamp>>24),
	)
}

func (c *Date32) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *Date32) AppendP(v *time.Time) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}

func (c *Date32) AppendDict(v time.Time) {
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

func (c *Date32) AppendDictNil() {
	c.keys = append(c.keys, 0)
}

func (c *Date32) AppendDictP(v *time.Time) {
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

func (c *Date32) Keys() []int {
	return c.keys
}

func (c *Date32) Reset() {
	c.column.Reset()
	c.keys = c.keys[:0]
	c.dict = make(map[time.Time]int)
}
