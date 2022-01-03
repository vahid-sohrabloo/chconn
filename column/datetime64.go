package column

import (
	"encoding/binary"
	"math"
	"time"
)

type DateTime64 struct {
	column
	val       time.Time
	precision int64
}

func NewDateTime64(precision int, nullable bool) *DateTime64 {
	return &DateTime64{
		precision: int64(math.Pow10(9 - precision)),
		column: column{
			nullable:    nullable,
			colNullable: newNullable(),
			size:        Datetime64Size,
		},
	}
}

func (c *DateTime64) Next() bool {
	if c.i >= c.totalByte {
		return false
	}
	c.i += c.size
	c.val = c.toDate(int64(binary.LittleEndian.Uint64(c.b[c.i-c.size : c.i])))
	return true
}

func (c *DateTime64) Value() time.Time {
	return c.val
}

func (c *DateTime64) ReadAll(value *[]time.Time) {
	for i := 0; i < c.totalByte; i += c.size {
		*value = append(*value,
			c.toDate(int64(binary.LittleEndian.Uint64(c.b[i:i+c.size]))))
	}
}

func (c *DateTime64) Fill(value []time.Time) {
	for i := range value {
		value[i] = c.toDate(int64(binary.LittleEndian.Uint64(c.b[c.i : c.i+c.size])))
		c.i += 8
	}
}

func (c *DateTime64) toDate(usec int64) time.Time {
	nano := usec * c.precision
	sec := nano / int64(10e8)
	nsec := nano - sec*10e8
	return time.Unix(sec, nsec)
}

func (c *DateTime64) ValueP() *time.Time {
	if c.colNullable.b[(c.i-c.size)/(c.size)] == 1 {
		return nil
	}
	val := c.val
	return &val
}

func (c *DateTime64) ReadAllP(value *[]*time.Time) {
	for i := 0; i < c.totalByte; i += c.size {
		if c.colNullable.b[i/c.size] != 0 {
			*value = append(*value, nil)
			continue
		}
		val := c.toDate(int64(binary.LittleEndian.Uint64(c.b[i : i+c.size])))
		*value = append(*value, &val)
	}
}

func (c *DateTime64) FillP(value []*time.Time) {
	for i := range value {
		if c.colNullable.b[c.i/c.size] == 1 {
			value[i] = nil
			c.i += c.size
			continue
		}
		val := c.toDate(int64(binary.LittleEndian.Uint64(c.b[c.i : c.i+c.size])))
		value[i] = &val
		c.i += c.size
	}
}
func (c *DateTime64) Append(v time.Time) {
	c.numRow++
	if v.Unix() < 0 {
		c.writerData = append(c.writerData, emptyByte[:c.size]...)
		return
	}
	timestamp := v.UnixNano() / c.precision
	c.writerData = append(c.writerData,
		byte(timestamp),
		byte(timestamp>>8),
		byte(timestamp>>16),
		byte(timestamp>>24),
		byte(timestamp>>32),
		byte(timestamp>>40),
		byte(timestamp>>48),
		byte(timestamp>>56),
	)
}

func (c *DateTime64) AppendEmpty() {
	c.numRow++
	c.writerData = append(c.writerData, emptyByte[:c.size]...)
}

func (c *DateTime64) AppendP(v *time.Time) {
	if v == nil {
		c.AppendEmpty()
		c.colNullable.Append(1)
		return
	}
	c.colNullable.Append(0)
	c.Append(*v)
}
