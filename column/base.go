package column

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/types"
)

type BaseType interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64 | ~int8 | ~int16 | ~int32 | ~int64 | ~float32 | ~float64 | ~string | ~bool |
		types.Int128 | types.Int256 | types.Uint128 | types.Uint256 | types.Decimal128 | types.Decimal256 |
		// repeated types [...]byte. go not support array size  in generic type
		// https://github.com/golang/go/issues/44253
		~[1]byte | ~[2]byte | ~[3]byte | ~[4]byte | ~[5]byte | ~[6]byte | ~[7]byte | ~[8]byte | ~[9]byte | ~[10]byte | ~[11]byte |
		~[12]byte | ~[13]byte | ~[14]byte | ~[15]byte | ~[16]byte | ~[17]byte | ~[18]byte | ~[19]byte | ~[20]byte |
		~[21]byte | ~[22]byte | ~[23]byte | ~[24]byte | ~[25]byte | ~[26]byte | ~[27]byte | ~[28]byte | ~[29]byte |
		~[30]byte | ~[31]byte | ~[32]byte | ~[33]byte | ~[34]byte | ~[35]byte | ~[36]byte | ~[37]byte | ~[38]byte |
		~[39]byte | ~[40]byte | ~[41]byte | ~[42]byte | ~[43]byte | ~[44]byte | ~[45]byte | ~[46]byte | ~[47]byte |
		~[48]byte | ~[49]byte | ~[50]byte | ~[51]byte | ~[52]byte | ~[53]byte | ~[54]byte | ~[55]byte | ~[56]byte |
		~[57]byte | ~[58]byte | ~[59]byte | ~[60]byte | ~[61]byte | ~[62]byte | ~[63]byte | ~[64]byte | ~[65]byte |
		~[66]byte | ~[67]byte | ~[68]byte | ~[69]byte | ~[70]byte | ~[71]byte | ~[72]byte | ~[73]byte | ~[74]byte
}

type decimalType int

const (
	decimalTypeNone decimalType = iota
	decimal32Type
	decimal64Type
	decimal128Type
	decimal256Type
)

// Column use for most (fixed size) ClickHouse Columns type
type Base[T BaseType] struct {
	column
	size      int
	strict    bool
	numRow    int
	kind      reflect.Kind
	rtype     reflect.Type
	values    []T
	params    []any
	isDecimal decimalType
}

// New create a new column
func New[T BaseType]() *Base[T] {
	var tmpValue T
	size := int(unsafe.Sizeof(tmpValue))
	return &Base[T]{
		size:   size,
		strict: true,
		kind:   reflect.TypeOf(tmpValue).Kind(),
		rtype:  reflect.TypeOf(tmpValue),
	}
}

// Data get all the data in current block as a slice.
//
// NOTE: the return slice only valid in current block, if you want to use it after, you should copy it. or use Read
func (c *Base[T]) Data() []T {
	value := *(*[]T)(unsafe.Pointer(&c.b))
	return value[:c.numRow]
}

// Read reads all the data in current block and append to the input.
func (c *Base[T]) Read(value []T) []T {
	return append(value, c.Data()...)
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Base[T]) Row(row int) T {
	i := row * c.size
	return *(*T)(unsafe.Pointer(&c.b[i]))
}

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *Base[T]) RowAny(row int) any {
	return c.Row(row)
}

// Append value for insert
func (c *Base[T]) Append(v T) {
	c.preHookAppend()
	c.values = append(c.values, v)
	c.numRow++
}

func (c *Base[T]) AppendAny(value any) error {
	v, ok := value.(T)
	if ok {
		c.Append(v)

		return nil
	}

	switch c.kind {
	case reflect.Int8:
		tmp, ok, err := getInt8Value(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Int16:
		tmp, ok, err := getInt16Value(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Int32:
		tmp, ok, err := getInt32Value(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Int64:
		tmp, ok, err := getInt64Value(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Int:
		tmp, ok, err := getIntValue(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Uint8:
		tmp, ok, err := getUint8Value(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Uint16:
		tmp, ok, err := getUint16Value(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Uint32:
		tmp, ok, err := getUint32Value(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Uint64:
		tmp, ok, err := getUint64Value(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Uint:
		tmp, ok, err := getUintValue(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Float32:
		tmp, ok, err := getFloat32Value(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Float64:
		tmp, ok, err := getFloat64Value(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.Bool:
		tmp, ok, err := getBoolValue(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	case reflect.String:
		tmp, ok, err := getStringValue(value)
		if err != nil {
			return fmt.Errorf("could not append %v to column: %w", value, err)
		}
		if ok {
			value = tmp
		}

	}

	v, ok = value.(T)
	if ok {
		c.Append(v)

		return nil
	}

	val := reflect.ValueOf(value)

	convertedVal, err := tryConvert(val, c.rtype)
	if err != nil {
		return fmt.Errorf("cannot convert value of type %T to %T", value, (*T)(nil))
	}

	c.Append(convertedVal.(T))

	return nil
}

// AppendMulti value for insert
func (c *Base[T]) AppendMulti(v ...T) {
	c.preHookAppendMulti(len(v))
	c.values = append(c.values, v...)
	c.numRow += len(v)
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *Base[T]) Remove(n int) {
	if c.NumRow() == 0 || c.NumRow() <= n {
		return
	}
	c.values = c.values[:n]
	c.numRow = len(c.values)
}

// NumRow return number of row for this block
func (c *Base[T]) NumRow() int {
	return c.numRow
}

// Array return a Array type for this column
func (c *Base[T]) Array() *Array[T] {
	return NewArray[T](c)
}

// Nullable return a nullable type for this column
func (c *Base[T]) Nullable() *BaseNullable[T] {
	return NewBaseNullable(c)
}

// LC return a low cardinality type for this column
func (c *Base[T]) LC() *LowCardinality[T] {
	return NewLC[T](c)
}

// LowCardinality return a low cardinality type for this column
func (c *Base[T]) LowCardinality() *LowCardinality[T] {
	return NewLowCardinality[T](c)
}

// appendEmpty append empty value for insert
func (c *Base[T]) appendEmpty() {
	var emptyValue T
	c.Append(emptyValue)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *Base[T]) Reset() {
	c.numRow = 0
	c.values = c.values[:0]
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *Base[T]) SetWriteBufferSize(row int) {
	if cap(c.values) < row {
		c.values = make([]T, 0, row)
	}
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *Base[T]) ReadRaw(num int, r *readerwriter.Reader) error {
	c.Reset()
	c.r = r
	c.numRow = num
	c.totalByte = num * c.size
	err := c.readBuffer()
	if err != nil {
		err = fmt.Errorf("read data: %w", err)
	}
	c.readyBufferHook()
	return err
}

func (c *Base[T]) readBuffer() error {
	if cap(c.b) < c.totalByte {
		c.b = make([]byte, c.totalByte)
	} else {
		c.b = c.b[:c.totalByte]
	}
	_, err := c.r.Read(c.b)
	return err
}

// HeaderReader reads header data from reader
// it uses internally
func (c *Base[T]) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	c.r = r
	return c.readColumn(readColumn, revision)
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *Base[T]) HeaderWriter(w *readerwriter.Writer) {
}

func (c *Base[T]) Elem(arrayLevel int, nullable, lc bool) ColumnBasic {
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

func (c *Base[T]) FullType() string {
	chType := string(c.chType)
	if chType == "" {
		chType = c.getChTypeFromKind()
	}
	if len(c.name) == 0 {
		return chType
	}
	return string(c.name) + " " + chType
}

type getCHType interface {
	GetCHType() string
}

func (c *Base[T]) getChTypeFromKind() string {
	var tmpT T
	if v, ok := any(tmpT).(getCHType); ok {
		return v.GetCHType()
	}
	kind := c.kind

	if kind == reflect.Int8 {
		return "Int8"
	} else if kind == reflect.Int16 {
		return "Int16"
	} else if kind == reflect.Int32 {
		return "Int32"
	} else if kind == reflect.Int64 {
		return "Int64"
	} else if kind == reflect.Uint8 {
		return "UInt8"
	} else if kind == reflect.Uint16 {
		return "UInt16"
	} else if kind == reflect.Uint32 {
		return "UInt32"
	} else if kind == reflect.Uint64 {
		return "UInt64"
	} else if kind == reflect.Float32 {
		return "Float32"
	} else if kind == reflect.Float64 {
		return "Float64"
	} else {
		panic(fmt.Sprintf("unsupported type: %s", c.kind))
	}
}

func (c *Base[T]) String(row int) string {
	val := c.Row(row)
	switch c.kind {
	case reflect.Int8:
		return strconv.FormatInt(int64(*(*int8)(unsafe.Pointer(&val))), 10)
	case reflect.Int16:
		return strconv.FormatInt(int64(*(*int16)(unsafe.Pointer(&val))), 10)
	case reflect.Int32:
		if c.isDecimal == decimal32Type {
			return (*types.Decimal32)(unsafe.Pointer(&val)).String(c.getDecimalScale())
		}
		return strconv.FormatInt(int64(*(*int32)(unsafe.Pointer(&val))), 10)
	case reflect.Int64:
		if c.isDecimal == decimal64Type {
			return (*types.Decimal64)(unsafe.Pointer(&val)).String(c.getDecimalScale())
		}
		return strconv.FormatInt(*(*int64)(unsafe.Pointer(&val)), 10)
	case reflect.Uint8:
		return strconv.FormatUint(uint64(*(*uint8)(unsafe.Pointer(&val))), 10)
	case reflect.Uint16:
		return strconv.FormatUint(uint64(*(*uint16)(unsafe.Pointer(&val))), 10)
	case reflect.Uint32:
		return strconv.FormatUint(uint64(*(*uint32)(unsafe.Pointer(&val))), 10)
	case reflect.Uint64:
		return strconv.FormatUint(*(*uint64)(unsafe.Pointer(&val)), 10)
	case reflect.Float32:
		return strconv.FormatFloat(float64(*(*float32)(unsafe.Pointer(&val))), 'f', -1, 32)
	case reflect.Float64:
		return strconv.FormatFloat(*(*float64)(unsafe.Pointer(&val)), 'f', -1, 64)
		// todo more types
	default:
		if c.kind == reflect.Array && c.rtype.Elem().Kind() == reflect.Uint8 {
			return string(*(*[]byte)(unsafe.Pointer(&val)))
		}
		if c.isDecimal == decimal128Type {
			return (*types.Decimal128)(unsafe.Pointer(&val)).String(c.getDecimalScale())
		}
		if c.isDecimal == decimal256Type {
			return (*types.Decimal256)(unsafe.Pointer(&val)).String(c.getDecimalScale())
		}
		if val, ok := any(val).(fmt.Stringer); ok {
			return val.String()
		}
		return fmt.Sprintf("%v", val)
	}
}

type appender interface {
	Append([]byte) []byte
}

//nolint:funlen,gocyclo
func (c *Base[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	val := c.Row(row)
	switch c.kind {
	case reflect.Int8:
		return strconv.AppendInt(b, int64(*(*int8)(unsafe.Pointer(&val))), 10)
	case reflect.Int16:
		return strconv.AppendInt(b, int64(*(*int16)(unsafe.Pointer(&val))), 10)
	case reflect.Int32:
		if c.isDecimal == decimal32Type {
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			b = (*types.Decimal32)(unsafe.Pointer(&val)).Append(c.getDecimalScale(), b)
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			return b
		}
		return strconv.AppendInt(b, int64(*(*int32)(unsafe.Pointer(&val))), 10)
	case reflect.Int64:
		if !ignoreDoubleQuotes {
			b = append(b, '"')
		}
		if c.isDecimal == decimal64Type {
			b = (*types.Decimal64)(unsafe.Pointer(&val)).Append(c.getDecimalScale(), b)
		} else {
			b = strconv.AppendInt(b, *(*int64)(unsafe.Pointer(&val)), 10)
		}
		if !ignoreDoubleQuotes {
			b = append(b, '"')
		}
		return b
	case reflect.Uint8:
		return strconv.AppendUint(b, uint64(*(*uint8)(unsafe.Pointer(&val))), 10)
	case reflect.Uint16:
		return strconv.AppendUint(b, uint64(*(*uint16)(unsafe.Pointer(&val))), 10)
	case reflect.Uint32:
		return strconv.AppendUint(b, uint64(*(*uint32)(unsafe.Pointer(&val))), 10)
	case reflect.Uint64:
		if !ignoreDoubleQuotes {
			b = append(b, '"')
		}
		b = strconv.AppendUint(b, *(*uint64)(unsafe.Pointer(&val)), 10)
		if !ignoreDoubleQuotes {
			b = append(b, '"')
		}
		return b
	case reflect.Float32:
		v := float64(*(*float32)(unsafe.Pointer(&val)))
		if math.IsInf(v, 0) || math.IsNaN(v) {
			return append(b, "null"...)
		}
		return strconv.AppendFloat(b, v, 'f', -1, 32)
	case reflect.Float64:
		v := *(*float64)(unsafe.Pointer(&val))
		if math.IsInf(v, 0) || math.IsNaN(v) {
			return append(b, "null"...)
		}
		return strconv.AppendFloat(b, *(*float64)(unsafe.Pointer(&val)), 'f', -1, 64)
		// todo more types
	default:
		if val, ok := any(val).(appender); ok {
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			b = val.Append(b)
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			return b
		}
		if val, ok := any(val).(fmt.Stringer); ok {
			return helper.AppendJSONSting(b, ignoreDoubleQuotes, []byte(val.String()))
		}
		if c.kind == reflect.Array && c.rtype.Elem().Kind() == reflect.Uint8 {
			arrayLength := c.rtype.Len()
			byteSlice := unsafe.Slice((*byte)(unsafe.Pointer(&val)), arrayLength)
			// Marshal the byte slice to JSON.
			return helper.AppendJSONSting(b, ignoreDoubleQuotes, byteSlice)
		}
		if c.isDecimal == decimal128Type {
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			b = (*types.Decimal128)(unsafe.Pointer(&val)).Append(c.getDecimalScale(), b)
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			return b
		}
		if c.isDecimal == decimal256Type {
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			b = (*types.Decimal256)(unsafe.Pointer(&val)).Append(c.getDecimalScale(), b)
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			return b
		}

		// todo
		panic("not support")
	}
}
