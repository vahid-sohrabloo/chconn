package column

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
	"github.com/vahid-sohrabloo/chconn/v3/types"
)

type BaseType interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64 | ~int8 | ~int16 | ~int32 | ~int64 | ~float32 | ~float64 | ~bool |
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
	size          int
	strict        bool
	numRow        int
	kind          reflect.Kind
	rtype         reflect.Type
	values        []T
	params        []any
	decimalType   decimalType
	isEnum8       bool
	isEnum16      bool
	enumStringMap map[int16]string
	sparseData    []T
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

func (c *Base[T]) canAppend(value any) bool {
	if _, ok := value.(T); ok {
		return true
	}
	return reflect.ValueOf(value).Kind() == c.kind
}

func (c *Base[T]) AppendAny(value any) error {
	if v, ok := value.(T); ok {
		c.Append(v)
		return nil
	}

	val := reflect.ValueOf(value)
	if val.Kind() == c.kind {
		c.Append(val.Convert(c.rtype).Interface().(T))
		return nil
	}

	return fmt.Errorf("invalid type: %T, expected type: %s", value, c.rtype)
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
func (c *Base[T]) ReadRaw(num int) error {
	c.Reset()
	c.numRow = num
	c.totalByte = c.numRow * c.size

	if c.columnHeader.IsSparse {
		totalRowsRead, err := c.readSparse()
		if err != nil {
			return fmt.Errorf("read sparse: %w", err)
		}
		c.numRow = totalRowsRead
		c.totalByte = totalRowsRead * c.size
	}
	err := c.readBuffer()
	if err != nil {
		err = fmt.Errorf("read data: %w", err)
	}
	c.readBufferHook()

	if c.columnHeader.IsSparse {
		c.itemsTotalSparse -= 1
		items := c.Data()
		c.sparseData = helper.ResetSlice(c.sparseData, int(c.itemsTotalSparse), true)

		for i, itemNumber := range c.sparseIndexes {
			c.sparseData[itemNumber-1] = items[i]
		}

		c.numRow = int(c.itemsTotalSparse)
		bSize := c.size * len(c.sparseData)
		c.b = helper.ResetSlice(c.b, bSize, false)
		copy(c.b, helper.ConvertToByte(c.sparseData, c.size))
	}
	return err
}

func (c *Base[T]) readBuffer() error {
	c.b = helper.ResetSlice(c.b, c.totalByte, false)
	_, err := c.r.Read(c.b)
	return err
}

// ReadHeader reads header data from reader
// it uses internally
func (c *Base[T]) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	return c.column.ReadHeader(r, serverInfo)
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *Base[T]) HeaderWriter(w *readerwriter.Writer) {
}

func (c *Base[T]) Elem(arrayLevel int, nullable, lc bool) ColumnCore {
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
	chType := string(c.columnHeader.ChType)
	if chType == "" {
		chType = c.getChTypeFromKind()
	}
	if len(c.columnHeader.Name) == 0 {
		return chType
	}
	return string(c.columnHeader.Name) + " " + chType
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

	if kind == reflect.Bool {
		return "Bool"
	} else if kind == reflect.Int8 {
		if c.isEnum8 {
			return "Enum8"
		}
		return "Int8"
	} else if kind == reflect.Int16 {
		if c.isEnum16 {
			return "Enum16"
		}
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
	} else if kind == reflect.Array && c.rtype.Elem().Kind() == reflect.Uint8 {
		return "FixedString(" + strconv.Itoa(c.rtype.Len()) + ")"
	} else {
		panic(fmt.Sprintf("unsupported type: %s (%s)", c.kind, string(c.Type())))
	}
}

type writeBinaryDataTo interface {
	WriteBinaryDataTo(w *readerwriter.Writer) string
}

func (c *Base[T]) writeBinaryDataTo(w *readerwriter.Writer) {
	var tmpT T
	if v, ok := any(tmpT).(writeBinaryDataTo); ok {
		v.WriteBinaryDataTo(w)
		return
	}
	switch c.kind {
	case reflect.Bool:
		w.Uint8(uint8(helper.BinaryTypeIndexBool))
	case reflect.Int8:
		if c.isEnum8 {
			w.Uint8(uint8(helper.BinaryTypeIndexEnum8))
		} else {
			w.Uint8(uint8(helper.BinaryTypeIndexInt8))
		}
	case reflect.Int16:
		if c.isEnum16 {
			w.Uint8(uint8(helper.BinaryTypeIndexEnum16))
		} else {
			w.Uint8(uint8(helper.BinaryTypeIndexInt16))
		}
	case reflect.Int32:
		w.Uint8(uint8(helper.BinaryTypeIndexInt32))
	case reflect.Int64:
		w.Uint8(uint8(helper.BinaryTypeIndexInt64))
	case reflect.Uint8:
		w.Uint8(uint8(helper.BinaryTypeIndexUInt8))
	case reflect.Uint16:
		w.Uint8(uint8(helper.BinaryTypeIndexUInt16))
	case reflect.Uint32:
		w.Uint8(uint8(helper.BinaryTypeIndexUInt32))
	case reflect.Uint64:
		w.Uint8(uint8(helper.BinaryTypeIndexUInt64))
	case reflect.Float32:
		w.Uint8(uint8(helper.BinaryTypeIndexFloat32))
	case reflect.Float64:
		w.Uint8(uint8(helper.BinaryTypeIndexFloat64))
	default:
		panic(fmt.Sprintf("unsupported type: %s", c.kind))
	}
}

type appender interface {
	Append([]byte) []byte
}

//nolint:funlen,gocyclo
func (c *Base[T]) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	val := c.Row(row)
	switch c.kind {
	case reflect.Bool:
		if *(*bool)(unsafe.Pointer(&val)) {
			return append(b, "true"...)
		} else {
			return append(b, "false"...)
		}
	case reflect.Int8:
		if c.isEnum8 {
			if c.enumStringMap == nil {
				c.enumStringMap, _, _ = helper.ExtractEnum(c.columnHeader.ChType[helper.Enum8StrLen : len(c.columnHeader.ChType)-1])
			}
			return helper.AppendJSONSting(b, ignoreDoubleQuotes, []byte(c.enumStringMap[int16(*(*int8)(unsafe.Pointer(&val)))]))
		}
		return strconv.AppendInt(b, int64(*(*int8)(unsafe.Pointer(&val))), 10)
	case reflect.Int16:
		if c.isEnum16 {
			if c.enumStringMap == nil {
				c.enumStringMap, _, _ = helper.ExtractEnum(c.columnHeader.ChType[helper.Enum16StrLen : len(c.columnHeader.ChType)-1])
			}
			return helper.AppendJSONSting(b, ignoreDoubleQuotes, []byte(c.enumStringMap[*(*int16)(unsafe.Pointer(&val))]))
		}
		return strconv.AppendInt(b, int64(*(*int16)(unsafe.Pointer(&val))), 10)
	case reflect.Int32:
		if c.decimalType == decimal32Type {
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
		if c.decimalType == decimal64Type {
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
		if c.decimalType == decimal128Type {
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			b = (*types.Decimal128)(unsafe.Pointer(&val)).Append(c.getDecimalScale(), b)
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			return b
		}
		if c.decimalType == decimal256Type {
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
