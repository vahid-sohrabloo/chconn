package types

import (
	"time"
)

// two bytes as the number of days since 1970-01-01 (unsigned).
type Date uint16

func (d Date) GetCHType() string {
	return "Date"
}

const minDate32 = int32(-25567) // 1900-01-01 00:00:00 +0000 UTC

type Date32 int32

func (d Date32) GetCHType() string {
	return "Date32"
}

type DateTime uint32

func (d DateTime) GetCHType() string {
	return "DateTime"
}

const minDateTime64 = int64(-2208988800) // 1900-01-01 00:00:00 +0000 UTC

type DateTime64 int64

func (d DateTime64) GetCHType() string {
	return "DateTime64"
}

const daySeconds = 24 * 60 * 60

func TimeToDate(t time.Time) Date {
	if t.Unix() <= 0 {
		return 0
	}
	_, offset := t.Zone()
	return Date((t.Unix() + int64(offset)) / daySeconds)
}

func (d Date) FromTime(v time.Time, precision int) Date {
	return TimeToDate(v)
}

func (d Date) ToTime(loc *time.Location, precision int) time.Time {
	return time.Unix(d.Unix(), 0).UTC()
}

func (d Date) Append(b []byte, loc *time.Location, precision int) []byte {
	year, month, day := d.ToTime(loc, precision).Date()
	initialSize := len(b)
	b = growSlice(b, len(b)+10)
	dateByte := b[initialSize:]
	_ = dateByte[9]

	dateByte[0] = '0' + byte(year/1000%10)
	dateByte[1] = '0' + byte(year/100%10)
	dateByte[2] = '0' + byte(year/10%10)
	dateByte[3] = '0' + byte(year%10)
	dateByte[4] = '-'
	dateByte[5] = '0' + byte(month/10)
	dateByte[6] = '0' + byte(month%10)
	dateByte[7] = '-'
	dateByte[8] = '0' + byte(day/10)
	dateByte[9] = '0' + byte(day%10)

	return b
}

func (d Date) Unix() int64 {
	return daySeconds * int64(d)
}

func (d Date32) Unix() int64 {
	return daySeconds * int64(d)
}

func (d Date32) FromTime(v time.Time, precision int) Date32 {
	return TimeToDate32(v)
}

func (d Date32) ToTime(loc *time.Location, precision int) time.Time {
	return time.Unix(d.Unix(), 0).UTC()
}

func (d Date32) Append(b []byte, loc *time.Location, precision int) []byte {
	year, month, day := d.ToTime(loc, precision).Date()
	initialSize := len(b)
	b = growSlice(b, len(b)+10)
	dateByte := b[initialSize:]
	_ = dateByte[9]

	dateByte[0] = '0' + byte(year/1000%10)
	dateByte[1] = '0' + byte(year/100%10)
	dateByte[2] = '0' + byte(year/10%10)
	dateByte[3] = '0' + byte(year%10)
	dateByte[4] = '-'
	dateByte[5] = '0' + byte(month/10)
	dateByte[6] = '0' + byte(month%10)
	dateByte[7] = '-'
	dateByte[8] = '0' + byte(day/10)
	dateByte[9] = '0' + byte(day%10)

	return b
}

func TimeToDate32(t time.Time) Date32 {
	_, offset := t.Zone()
	d := int32((t.Unix() + int64(offset)) / daySeconds)
	if d <= minDate32 {
		return Date32(minDate32)
	}

	return Date32(d)
}

func TimeToDateTime(t time.Time) DateTime {
	if t.Unix() <= 0 {
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

func (d DateTime) Append(b []byte, loc *time.Location, precision int) []byte {
	// todo find optimized way like golang RFC3339
	return d.ToTime(loc, precision).AppendFormat(b, "2006-01-02 15:04:05")
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
	if t.Unix() <= minDateTime64 {
		return DateTime64(minDateTime64)
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

func (d DateTime64) Append(b []byte, loc *time.Location, precision int) []byte {
	// todo find optimized way like golang RFC3339
	return d.ToTime(loc, precision).AppendFormat(b, "2006-01-02 15:04:05.000000000")
}
