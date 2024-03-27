package column

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/types"
)

// DateType is an interface to handle convert between time.Time and T.

type DateType[T types.Date | types.Date32 | types.DateTime | types.DateTime64] interface {
	types.Date | types.Date32 | types.DateTime | types.DateTime64
	ToTime(val *time.Location, precision int) time.Time
	Append(b []byte, val *time.Location, precision int) []byte
	FromTime(val time.Time, precision int) T
}

// Date is a date column of ClickHouse date type (Date, Date32, DateTime, DateTime64).
// it is a wrapper of time.Time. but if you want to work with the raw data like unix timestamp
// you can directly use `Column` (`New[T]()`)
//
// `types.Date` data types For `Date`.
//
// `types.Date32` data types For `Date32`
//
// `types.DateTime` data types For `DateTime`
//
// `types.DateTime64` data types For `DateTime64`
type Date[T DateType[T]] struct {
	Base[T]
	loc       *time.Location
	precision int
}

// NewDate create a new date column of ClickHouse date type (Date, Date32, DateTime, DateTime64).
// it is a wrapper of time.Time. but if you want to work with the raw data like unix timestamp
// you can directly use `Column` (`New[T]()``)
//
// `uint16` or `types.Date` or any 16 bits data types For `Date`.
//
// `uint32` or `types.Date32` or any 32 bits data types For `Date32`
//
// `uint32` or `types.DateTime` or any 32 bits data types For `DateTime`
//
// `uint64` or `types.DateTime64` or any 64 bits data types For `DateTime64`
//
// ONLY ON SELECT: timezone set automatically for `DateTime` and `DateTime64` if not set and present in clickhouse datatype)

func NewDate[T DateType[T]]() *Date[T] {
	var tmpValue T
	size := int(unsafe.Sizeof(tmpValue))
	return &Date[T]{
		Base: Base[T]{
			size:   size,
			strict: true,
			kind:   reflect.TypeOf(tmpValue).Kind(),
			rtype:  reflect.TypeOf(tmpValue),
		},
	}
}

// SetLocation set the location of the time.Time. Only use for `DateTime` and `DateTime64`
func (c *Date[T]) SetLocation(loc *time.Location) *Date[T] {
	c.loc = loc
	return c
}

// Location get location
//
// ONLY ON SELECT: set automatically for `DateTime` and `DateTime64` if not set and present in clickhouse datatype)
func (c *Date[T]) Location() *time.Location {
	if c.loc == nil && len(c.params) >= 2 && len(c.params[1].([]byte)) > 0 {
		loc, err := time.LoadLocation(strings.Trim(string(c.params[1].([]byte)), "'"))
		if err == nil {
			c.SetLocation(loc)
		} else {
			c.SetLocation(time.Local)
		}
	}
	if c.loc == nil {
		c.SetLocation(time.Local)
	}
	return c.loc
}

// SetPrecision set the precision of the time.Time. Only use for `DateTime64`
func (c *Date[T]) SetPrecision(precision int) *Date[T] {
	c.precision = precision
	return c
}

func (c *Date[T]) Scan(row int, dest any) error {
	switch dest := dest.(type) {
	case *T:
		*dest = c.Base.Row(row)
		return nil
	case **T:
		*dest = new(T)
		**dest = c.Base.Row(row)
		return nil
	case *time.Time:
		*dest = c.Row(row)
		return nil
	case **time.Time:
		*dest = new(time.Time)
		**dest = c.Row(row)
		return nil
	case *any:
		*dest = c.Row(row)
		return nil
	case sql.Scanner:
		return dest.Scan(c.Row(row))
	}

	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "*" + c.rtype.String() + " or *time.Time",
	}
}

// Data get all the data in current block as a slice.
func (c *Date[T]) Data() []time.Time {
	values := make([]time.Time, c.numRow)
	for i := 0; i < c.numRow; i++ {
		values[i] = c.Row(i)
	}
	return values
}

// Read reads all the data in current block and append to the input.
func (c *Date[T]) Read(value []time.Time) []time.Time {
	if cap(value)-len(value) >= c.NumRow() {
		value = (value)[:len(value)+c.NumRow()]
	} else {
		value = append(value, make([]time.Time, c.NumRow())...)
	}
	val := (value)[len(value)-c.NumRow():]
	for i := 0; i < c.NumRow(); i++ {
		val[i] = c.Row(i)
	}
	return value
}

// Row return the value of given row
// NOTE: Row number start from zero
func (c *Date[T]) Row(row int) time.Time {
	i := row * c.size
	return (*(*T)(unsafe.Pointer(&c.b[i]))).ToTime(c.Location(), c.precision)
}

// Append value for insert
func (c *Date[T]) Append(v time.Time) {
	c.preHookAppend()
	var val T
	c.values = append(c.values, val.FromTime(v, c.precision))
	c.numRow++
}

func (c *Date[T]) AppendAny(value any) error {
	switch v := value.(type) {
	case T:
		c.Append(v.ToTime(c.loc, c.precision))

		return nil
	case time.Time:
		c.Append(v)

		return nil
	case int64:
		c.Append(time.Unix(v, 0))

		return nil
	default:
		return fmt.Errorf("invalid type %T", value)
	}
}

// AppendMulti value for insert
func (c *Date[T]) AppendMulti(v ...time.Time) {
	c.preHookAppendMulti(len(v))
	var val T
	for _, v := range v {
		c.values = append(c.values, val.FromTime(v, c.precision))
	}
	c.numRow += len(v)
}

// Array return a Array type for this column
func (c *Date[T]) Array() *Array[time.Time] {
	return NewArray[time.Time](c)
}

// Nullable return a nullable type for this column
func (c *Date[T]) Nullable() *DateNullable[T] {
	return NewDateNullable(c)
}

// LC return a low cardinality type for this column
func (c *Date[T]) LC() *LowCardinality[time.Time] {
	return NewLC[time.Time](c)
}

// LowCardinality return a low cardinality type for this column
func (c *Date[T]) LowCardinality() *LowCardinality[time.Time] {
	return NewLC[time.Time](c)
}

func (c *Date[T]) Elem(arrayLevel int, nullable, lc bool) ColumnBasic {
	if lc {
		return c.LowCardinality().elem(arrayLevel, nullable)
	}
	if nullable {
		return c.Nullable().elem(arrayLevel)
	}
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *Date[T]) FullType() string {
	chType := string(c.chType)
	if chType == "" {
		chType = "DateTime"
	}
	if len(c.name) == 0 {
		return chType
	}
	return string(c.name) + " " + chType
}

func (c Date[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	i := row * c.size
	if !ignoreDoubleQuotes {
		b = append(b, '"')
	}
	b = (*(*T)(unsafe.Pointer(&c.b[i]))).Append(b, c.Location(), c.precision)
	if !ignoreDoubleQuotes {
		b = append(b, '"')
	}
	return b
}
