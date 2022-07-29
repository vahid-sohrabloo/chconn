package types

import (
	"time"

	"github.com/vahid-sohrabloo/chconn/v2/column"
)

var _ = column.Date[Date]{}
var _ = column.Date[Date32]{}
var _ = column.Date[DateTime]{}
var _ = column.Date[DateTime64]{}

type Date uint16

type Date32 uint32

type DateTime uint32

type DateTime64 uint64

const daySeconds = 24 * 60 * 60

// date32Epoch is unix time of 1925-01-01.
const date32Epoch = -1420070400

func TimeToDate(t time.Time) Date {
	_, offset := t.Zone()
	return Date((t.Unix() + int64(offset)) / daySeconds)
}

func (d Date) FromTime(v time.Time, precision int) Date {
	return TimeToDate(v)
}

func (d Date) ToTime(loc *time.Location, precision int) time.Time {
	return time.Unix(d.Unix(), 0).UTC()
}

func (d Date) Unix() int64 {
	return daySeconds * int64(d)
}

func (d Date32) Unix() int64 {
	return daySeconds*int64(d) + date32Epoch
}

func (d Date32) FromTime(v time.Time, precision int) Date32 {
	return TimeToDate32(v)
}

func (d Date32) ToTime(loc *time.Location, precision int) time.Time {
	return time.Unix(d.Unix(), 0).UTC()
}

func TimeToDate32(t time.Time) Date32 {
	_, offset := t.Zone()
	return Date32((t.Unix() + int64(offset) - date32Epoch) / daySeconds)
}

func TimeToDateTime(t time.Time) DateTime {
	if t.IsZero() {
		return 0
	}
	return DateTime(t.Unix())
}

func (d DateTime) FromTime(v time.Time, precision int) DateTime {
	return TimeToDateTime(v)
}

func (d DateTime) ToTime(loc *time.Location, precision int) time.Time {
	return time.Unix(int64(d), 0).In(loc)
}

var precisionFactor = [...]int64{
	1000000000,
	100000000,
	10000000,
	1000000,
	100000,
	10000,
	1000,
	100,
	10,
	1,
}

func TimeToDateTime64(t time.Time, precision int) DateTime64 {
	if t.IsZero() {
		return 0
	}
	return DateTime64(t.UnixNano() / precisionFactor[precision])
}

func (d DateTime64) FromTime(v time.Time, precision int) DateTime64 {
	return TimeToDateTime64(v, precision)
}

func (d DateTime64) ToTime(loc *time.Location, precision int) time.Time {
	if d == 0 {
		return time.Time{}
	}
	nsec := int64(d) * precisionFactor[precision]
	return time.Unix(nsec/1e9, nsec%1e9).In(loc)
}
