package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDate(t *testing.T) {
	// Test TimeToDate function
	t1 := time.Now().UTC()
	d := TimeToDate(t1)
	assert.Equal(t, "Date", d.GetCHType())
	expected := Date(t1.Unix() / (24 * 60 * 60))
	assert.Equal(t, d, expected, "TimeToDate was incorrect")

	// Test FromTime function
	d2 := d.FromTime(t1, 0)
	assert.Equal(t, d2, d, "FromTime was incorrect")

	// Test ToTime function
	t2 := d.ToTime(time.UTC, 0)
	assert.Equal(t, t2.Unix(), int64(d)*(24*60*60), "ToTime was incorrect")

	// Test Append function
	b := []byte{}
	b2 := d.Append(b, time.UTC, 0)
	expected2 := t2.AppendFormat(b, "2006-01-02")
	assert.Equal(t, b2, expected2, "Append was incorrect")

	d3 := TimeToDate(time.Time{})
	expected = Date(0)
	assert.Equal(t, d3, expected, "TimeToDate was incorrect")
}

func TestDate32(t *testing.T) {
	// Test TimeToDate32 function
	t1 := time.Now().UTC()
	d := TimeToDate32(t1)
	assert.Equal(t, "Date32", d.GetCHType())
	expected := Date32(t1.Unix() / (24 * 60 * 60))
	assert.Equal(t, d, expected, "TimeToDate32 was incorrect")

	// Test FromTime function
	d2 := d.FromTime(t1, 0)
	assert.Equal(t, d2, d, "FromTime was incorrect")

	// Test ToTime function
	t2 := d.ToTime(time.UTC, 0)
	assert.Equal(t, t2.Unix(), int64(d)*(24*60*60), "ToTime was incorrect")

	// Test Append function
	b := []byte{}
	b2 := d.Append(b, time.UTC, 0)
	expected2 := t2.AppendFormat(b, "2006-01-02")
	assert.Equal(t, b2, expected2, "Append was incorrect")

	d3 := TimeToDate32(time.Time{})
	expected = Date32(minDate32)
	assert.Equal(t, d3, expected, "TimeToDate32 was incorrect")
}

func TestDateTime(t *testing.T) {
	// Test TimeToDateTime function
	t1 := time.Now()
	d := TimeToDateTime(t1)
	assert.Equal(t, "DateTime", d.GetCHType())
	expected := DateTime(t1.Unix())
	assert.Equal(t, d, expected, "TimeToDateTime64 was incorrect")

	// Test FromTime function
	d2 := d.FromTime(t1, 0)
	assert.Equal(t, d2, d, "FromTime was incorrect")

	// Test ToTime function
	t2 := d.ToTime(time.UTC, 0)
	assert.Equal(t, t2.Unix(), int64(d), "ToTime was incorrect")

	// Test Append function
	b := []byte{}
	b2 := d.Append(b, time.UTC, 0)
	expected2 := t2.AppendFormat(b, "2006-01-02 15:04:05")
	assert.Equal(t, b2, expected2, "Append was incorrect")

	d3 := TimeToDateTime(time.Time{})
	expected = DateTime(0)
	assert.Equal(t, d3, expected, "TimeToDateTime was incorrect")
}

func TestDateTime64(t *testing.T) {
	// Test TimeToDateTime64 function
	t1 := time.Now()
	precision := 3
	d := TimeToDateTime64(t1, precision)
	assert.Equal(t, "DateTime64", d.GetCHType())
	expected := DateTime64(t1.UnixNano() / precisionFactor[precision])
	assert.Equal(t, d, expected, "TimeToDateTime64 was incorrect")

	// Test FromTime function
	d2 := d.FromTime(t1, precision)
	assert.Equal(t, d2, d, "FromTime was incorrect")

	// Test ToTime function
	t2 := d.ToTime(time.UTC, precision)
	assert.Equal(t, t2.UnixNano(), int64(d)*precisionFactor[precision], "ToTime was incorrect")

	// Test Append function
	b := []byte{}
	b2 := d.Append(b, time.UTC, precision)
	expected2 := t2.AppendFormat(b, "2006-01-02 15:04:05.000000000")
	assert.Equal(t, b2, expected2, "Append was incorrect")

	d3 := TimeToDateTime64(time.Time{}, 0)
	expected = DateTime64(minDateTime64)
	assert.Equal(t, d3, expected, "TimeToDateTime64 was incorrect")
}
