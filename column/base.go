package column

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"

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

// Column use for most (fixed size) ClickHouse Columns type
type Base[T BaseType] struct {
	column
	size   int
	strict bool
	numRow int
	kind   reflect.Kind
	rtype  reflect.Type
	values []T
	params []interface{}
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

// RowI return the value of given row.
// NOTE: Row number start from zero
func (c *Base[T]) RowI(row int) any {
	return c.Row(row)
}

//nolint:funlen,gocyclo
func (c *Base[T]) Scan(row int, dest any) error {
	switch dest := dest.(type) {
	case *bool:
		val, err := c.getBool(c.Row(row))
		*dest = val
		return err
	case **bool:
		val, err := c.getBool(c.Row(row))
		*dest = new(bool)
		**dest = val
		return err
	case *int8:
		val, err := c.getInt64(c.Row(row))
		*dest = int8(val)
		return err
	case **int8:
		val, err := c.getInt64(c.Row(row))
		*dest = new(int8)
		**dest = int8(val)
		return err
	case *int16:
		val, err := c.getInt64(c.Row(row))
		*dest = int16(val)
		return err
	case **int16:
		val, err := c.getInt64(c.Row(row))
		*dest = new(int16)
		**dest = int16(val)
		return err
	case *int32:
		val, err := c.getInt64(c.Row(row))
		*dest = int32(val)
		return err
	case **int32:
		val, err := c.getInt64(c.Row(row))
		*dest = new(int32)
		**dest = int32(val)
		return err
	case *int64:
		val, err := c.getInt64(c.Row(row))
		*dest = val
		return err
	case **int64:
		val, err := c.getInt64(c.Row(row))
		*dest = new(int64)
		**dest = val
		return err
	case *uint8:
		val, err := c.getUint64(c.Row(row))
		*dest = uint8(val)
		return err
	case **uint8:
		val, err := c.getUint64(c.Row(row))
		*dest = new(uint8)
		**dest = uint8(val)
		return err
	case *uint16:
		val, err := c.getUint64(c.Row(row))
		*dest = uint16(val)
		return err
	case **uint16:
		val, err := c.getUint64(c.Row(row))
		*dest = new(uint16)
		**dest = uint16(val)
		return err
	case *uint32:
		val, err := c.getUint64(c.Row(row))
		*dest = uint32(val)
		return err
	case **uint32:
		val, err := c.getUint64(c.Row(row))
		*dest = new(uint32)
		**dest = uint32(val)
		return err
	case *uint64:
		val, err := c.getUint64(c.Row(row))
		*dest = val
		return err
	case **uint64:
		val, err := c.getUint64(c.Row(row))
		*dest = new(uint64)
		**dest = val
		return err
	case *float32:
		val, err := c.geFloat64(c.Row(row))
		*dest = float32(val)
		return err
	case **float32:
		val, err := c.geFloat64(c.Row(row))
		*dest = new(float32)
		**dest = float32(val)
		return err
	case *float64:
		val, err := c.geFloat64(c.Row(row))
		*dest = val
		return err
	case **float64:
		val, err := c.geFloat64(c.Row(row))
		*dest = new(float64)
		**dest = val
		return err
	case *string:
		*dest = c.String(row)
		return nil
	case **string:
		*dest = new(string)
		**dest = c.String(row)
		return nil
	case *types.Uint128:
		val, err := c.getUint128(c.Row(row))
		*dest = val
		return err
	case **types.Uint128:
		val, err := c.getUint128(c.Row(row))
		*dest = new(types.Uint128)
		**dest = val
		return err
	case *types.Int128:
		val, err := c.getInt128(c.Row(row))
		*dest = val
		return err
	case **types.Int128:
		val, err := c.getInt128(c.Row(row))
		*dest = new(types.Int128)
		**dest = val
		return err
	case *types.Uint256:
		val, err := c.getUint256(c.Row(row))
		*dest = val
		return err
	case **types.Uint256:
		val, err := c.getUint256(c.Row(row))
		*dest = new(types.Uint256)
		**dest = val
		return err
	case *types.Int256:
		val, err := c.getInt256(c.Row(row))
		*dest = val
		return err
	case **types.Int256:
		val, err := c.getInt256(c.Row(row))
		*dest = new(types.Int256)
		**dest = val
		return err
	case *types.Decimal128:
		val, err := c.getInt128(c.Row(row))
		*dest = types.Decimal128(val)
		return err
	case **types.Decimal128:
		val, err := c.getInt128(c.Row(row))
		*dest = new(types.Decimal128)
		**dest = types.Decimal128(val)
		return err
	case *time.Time:
		panic(c.rtype.Name())
	}

	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("scan dest should be a pointer")
	}

	return c.scanReflect(row, val)
}

func (c *Base[T]) scanReflect(row int, val reflect.Value) error {
	if val.Elem().Kind() == reflect.Pointer {
		if val.Elem().IsNil() {
			val.Elem().Set(reflect.New(val.Type().Elem().Elem()))
		}
		err := c.scanReflect(row, val.Elem())
		if err != nil {
			return err
		}
		return nil
	}

	switch val.Elem().Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		v, err := c.getInt64(c.Row(row))
		val.Elem().SetInt(v)
		return err
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		v, err := c.getUint64(c.Row(row))
		val.Elem().SetUint(v)
		return err
	case reflect.Float32, reflect.Float64:
		v, err := c.geFloat64(c.Row(row))
		val.Elem().SetFloat(v)
		return err
	default:
		if val.Elem().Kind() == reflect.Array && val.Elem().Type().Elem().Kind() == reflect.Uint8 {
			if c.kind == reflect.Array && c.rtype.Elem().Kind() == reflect.Uint8 {
				val.Elem().Set(reflect.ValueOf(c.Row(row)))
				return nil
			}
			// todo: we can do it with unsafe
			str := c.String(row)
			for i := 0; i < val.Elem().Len() && i < len(str); i++ {
				val.Elem().Index(i).SetUint(uint64(str[i]))
			}
			return nil
		}
		rowVal := reflect.ValueOf(c.Row(row))
		if !val.Elem().Type().AssignableTo(rowVal.Type()) {
			return fmt.Errorf("can't assign %s to %s", rowVal.Type(), val.Elem().Type())
		}
		val.Elem().Set(rowVal)
	}
	return nil
}

func (c *Base[T]) getInt64(val T) (int64, error) {
	if c.kind == reflect.Int8 {
		return int64(*(*int8)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int16 {
		return int64(*(*int16)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int32 {
		return int64(*(*int32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int64 {
		return *(*int64)(unsafe.Pointer(&val)), nil
	}
	if c.kind == reflect.Uint8 {
		return int64(*(*uint8)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint16 {
		return int64(*(*uint16)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint32 {
		return int64(*(*uint32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint64 {
		return int64(*(*uint64)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Float32 {
		return int64(*(*float32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Float64 {
		return int64(*(*float64)(unsafe.Pointer(&val))), nil
	}
	//nolint:dupl
	if c.kind == reflect.Struct {
		var t T
		switch any(t).(type) {
		case types.Uint128:
			return int64((*types.Uint128)(unsafe.Pointer(&val)).Uint64()), nil
		case types.Int128:
			return int64((*types.Int128)(unsafe.Pointer(&val)).Uint64()), nil
		case types.Uint256:
			return int64((*types.Uint256)(unsafe.Pointer(&val)).Uint64()), nil
		case types.Int256:
			return int64((*types.Int256)(unsafe.Pointer(&val)).Uint64()), nil
		}
	}

	return 0, fmt.Errorf("unsupported type: %s", c.kind)
}

func (c *Base[T]) getUint64(val T) (uint64, error) {
	if c.kind == reflect.Int8 {
		return uint64(*(*int8)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int16 {
		return uint64(*(*int16)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int32 {
		return uint64(*(*int32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int64 {
		return uint64(*(*int64)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint8 {
		return uint64(*(*uint8)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint16 {
		return uint64(*(*uint16)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint32 {
		return uint64(*(*uint32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint64 {
		return *(*uint64)(unsafe.Pointer(&val)), nil
	}
	if c.kind == reflect.Float32 {
		return uint64(*(*float32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Float64 {
		return uint64(*(*float64)(unsafe.Pointer(&val))), nil
	}

	if c.kind == reflect.Struct {
		var t T
		switch any(t).(type) {
		case types.Uint128:
			return (*types.Uint128)(unsafe.Pointer(&val)).Uint64(), nil
		case types.Int128:
			return (*types.Int128)(unsafe.Pointer(&val)).Uint64(), nil
		case types.Uint256:
			return (*types.Uint256)(unsafe.Pointer(&val)).Uint64(), nil
		case types.Int256:
			return (*types.Int256)(unsafe.Pointer(&val)).Uint64(), nil
		}
	}

	return 0, fmt.Errorf("unsupported type: %s", c.kind)
}

func (c *Base[T]) geFloat64(val T) (float64, error) {
	if c.kind == reflect.Int8 {
		return float64(*(*int8)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int16 {
		return float64(*(*int16)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int32 {
		return float64(*(*int32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int64 {
		return float64(*(*int64)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint8 {
		return float64(*(*uint8)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint16 {
		return float64(*(*uint16)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint32 {
		return float64(*(*uint32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint64 {
		return float64(*(*uint64)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Float32 {
		return float64(*(*float32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Float64 {
		return *(*float64)(unsafe.Pointer(&val)), nil
	}
	//nolint:dupl
	if c.kind == reflect.Struct {
		var t T
		switch any(t).(type) {
		case types.Uint128:
			return float64((*types.Uint128)(unsafe.Pointer(&val)).Uint64()), nil
		case types.Int128:
			return float64((*types.Int128)(unsafe.Pointer(&val)).Uint64()), nil
		case types.Uint256:
			return float64((*types.Uint256)(unsafe.Pointer(&val)).Uint64()), nil
		case types.Int256:
			return float64((*types.Int256)(unsafe.Pointer(&val)).Uint64()), nil
		}
	}

	return 0, fmt.Errorf("unsupported type: %s", c.kind)
}

func (c *Base[T]) getBool(val T) (bool, error) {
	if c.kind == reflect.Int8 {
		return *(*int8)(unsafe.Pointer(&val)) != 0, nil
	}
	if c.kind == reflect.Int16 {
		return *(*int16)(unsafe.Pointer(&val)) != 0, nil
	}
	if c.kind == reflect.Int32 {
		return *(*int32)(unsafe.Pointer(&val)) != 0, nil
	}
	if c.kind == reflect.Int64 {
		return *(*int64)(unsafe.Pointer(&val)) != 0, nil
	}
	if c.kind == reflect.Uint8 {
		return *(*uint8)(unsafe.Pointer(&val)) != 0, nil
	}
	if c.kind == reflect.Uint16 {
		return *(*uint16)(unsafe.Pointer(&val)) != 0, nil
	}
	if c.kind == reflect.Uint32 {
		return *(*uint32)(unsafe.Pointer(&val)) != 0, nil
	}
	if c.kind == reflect.Uint64 {
		return *(*uint64)(unsafe.Pointer(&val)) != 0, nil
	}
	if c.kind == reflect.Float32 {
		return *(*float32)(unsafe.Pointer(&val)) != 0, nil
	}
	if c.kind == reflect.Float64 {
		return *(*float64)(unsafe.Pointer(&val)) != 0, nil
	}

	if c.kind == reflect.Struct {
		var t T
		switch any(t).(type) {
		case types.Uint128:
			return (*types.Uint128)(unsafe.Pointer(&val)).Uint64() > 0, nil
		case types.Int128:
			return (*types.Int128)(unsafe.Pointer(&val)).Uint64() > 0, nil
		case types.Uint256:
			return (*types.Uint256)(unsafe.Pointer(&val)).Uint64() > 0, nil
		case types.Int256:
			return (*types.Int256)(unsafe.Pointer(&val)).Uint64() > 0, nil
		}
	}

	return false, fmt.Errorf("unsupported type: %s", c.kind)
}

func (c *Base[T]) getUint128(val T) (types.Uint128, error) {
	if c.kind == reflect.Int8 {
		return types.Uint128From64(uint64(*(*int8)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int16 {
		return types.Uint128From64(uint64(*(*int16)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int32 {
		return types.Uint128From64(uint64(*(*int32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int64 {
		return types.Uint128From64(uint64(*(*int64)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Uint8 {
		return types.Uint128From64(uint64(*(*uint8)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Uint16 {
		return types.Uint128From64(uint64(*(*uint16)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Uint32 {
		return types.Uint128From64(uint64(*(*uint32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Uint64 {
		return types.Uint128From64(*(*uint64)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Float32 {
		return types.Uint128From64(uint64(*(*float32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Float64 {
		return types.Uint128From64(uint64(*(*float64)(unsafe.Pointer(&val)))), nil
	}

	if c.kind == reflect.Struct {
		var t T
		switch any(t).(type) {
		case types.Uint128:
			return *(*types.Uint128)(unsafe.Pointer(&val)), nil
		case types.Int128:
			return (*types.Int128)(unsafe.Pointer(&val)).Uint128(), nil
		case types.Uint256:
			return (*types.Uint256)(unsafe.Pointer(&val)).Uint128(), nil
		case types.Int256:
			return (*types.Int256)(unsafe.Pointer(&val)).Uint128(), nil
		}
	}

	return types.Uint128{}, fmt.Errorf("unsupported type: %s", c.kind)
}

func (c *Base[T]) getInt128(val T) (types.Int128, error) {
	if c.kind == reflect.Int8 {
		return types.Int128From64(int64(*(*int8)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int16 {
		return types.Int128From64(int64(*(*int16)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int32 {
		return types.Int128From64(int64(*(*int32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int64 {
		return types.Int128From64(*(*int64)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint8 {
		return types.Int128{Lo: uint64(*(*uint8)(unsafe.Pointer(&val)))}, nil
	}
	if c.kind == reflect.Uint16 {
		return types.Int128{Lo: uint64(*(*uint16)(unsafe.Pointer(&val)))}, nil
	}
	if c.kind == reflect.Uint32 {
		return types.Int128{Lo: uint64(*(*uint32)(unsafe.Pointer(&val)))}, nil
	}
	if c.kind == reflect.Uint64 {
		return types.Int128{Lo: *(*uint64)(unsafe.Pointer(&val))}, nil
	}
	if c.kind == reflect.Float32 {
		return types.Int128{Lo: uint64(*(*float32)(unsafe.Pointer(&val)))}, nil
	}
	if c.kind == reflect.Float64 {
		return types.Int128{Lo: uint64(*(*float64)(unsafe.Pointer(&val)))}, nil
	}

	if c.kind == reflect.Struct {
		switch v := any(val).(type) {
		case types.Uint128:
			return v.Int128(), nil
		case types.Int128:
			return v, nil
		case types.Uint256:
			return v.Uint128().Int128(), nil
		case types.Int256:
			return v.Uint128().Int128(), nil
		case types.Decimal128:
			return types.Int128(v), nil
		case types.Decimal256:
			return types.Int256(v).Uint128().Int128(), nil
		}
	}

	return types.Int128{}, fmt.Errorf("unsupported type: %s", c.kind)
}

func (c *Base[T]) getUint256(val T) (types.Uint256, error) {
	if c.kind == reflect.Int8 {
		return types.Uint256From64(uint64(*(*int8)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int16 {
		return types.Uint256From64(uint64(*(*int16)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int32 {
		return types.Uint256From64(uint64(*(*int32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int64 {
		return types.Uint256From64(uint64(*(*int64)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Uint8 {
		return types.Uint256From64(uint64(*(*uint8)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Uint16 {
		return types.Uint256From64(uint64(*(*uint16)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Uint32 {
		return types.Uint256From64(uint64(*(*uint32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Uint64 {
		return types.Uint256From64(*(*uint64)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Float32 {
		return types.Uint256From64(uint64(*(*float32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Float64 {
		return types.Uint256From64(uint64(*(*float64)(unsafe.Pointer(&val)))), nil
	}

	if c.kind == reflect.Struct {
		switch v := any(val).(type) {
		case types.Uint128:
			return types.Uint256From128(v), nil
		case types.Int128:
			return types.Uint256From128(v.Uint128()), nil
		case types.Uint256:
			return v, nil
		case types.Int256:
			return v.Uint256(), nil
		case types.Decimal128:
			return types.Uint256From128(types.Int128(v).Uint128()), nil
		case types.Decimal256:
			return types.Int256(v).Uint256(), nil
		}
	}

	return types.Uint256{}, nil
}

func (c *Base[T]) getInt256(val T) (types.Int256, error) {
	if c.kind == reflect.Int8 {
		return types.Int256From64(int64(*(*int8)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int16 {
		return types.Int256From64(int64(*(*int16)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int32 {
		return types.Int256From64(int64(*(*int32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int64 {
		return types.Int256From64(*(*int64)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Uint8 {
		return types.Int256From64(int64(*(*uint8)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Uint16 {
		return types.Int256From64(int64(*(*uint16)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Uint32 {
		return types.Int256From64(int64(*(*uint32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Uint64 {
		return types.Int256From64(int64(*(*uint64)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Float32 {
		return types.Int256From64(int64(*(*float32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Float64 {
		return types.Int256From64(int64(*(*float64)(unsafe.Pointer(&val)))), nil
	}

	if c.kind == reflect.Struct {
		switch v := any(val).(type) {
		case types.Uint128:
			return types.Int256From128(v.Int128()), nil
		case types.Int128:
			return types.Int256From128(v), nil
		case types.Uint256:
			return v.Int256(), nil
		case types.Int256:
			return v, nil
		case types.Decimal128:
			return types.Int256From128(types.Int128(v)), nil
		case types.Decimal256:
			return types.Int256(v), nil
		}
	}

	return types.Int256{}, fmt.Errorf("unsupported type: %s", c.kind)
}

// Append value for insert
func (c *Base[T]) Append(v T) {
	c.values = append(c.values, v)
	c.numRow++
}

// AppendMulti value for insert
func (c *Base[T]) AppendMulti(v ...T) {
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

func (c *Base[T]) getChTypeFromKind() string {
	switch c.kind {
	case reflect.Int8:
		return "Int8"
	case reflect.Int16:
		return "Int16"
	case reflect.Int32:
		return "Int32"
	case reflect.Int64:
		return "Int64"
	case reflect.Uint8:
		return "UInt8"
	case reflect.Uint16:
		return "UInt16"
	case reflect.Uint32:
		return "UInt32"
	case reflect.Uint64:
		return "UInt64"
	case reflect.Float32:
		return "Float32"
	case reflect.Float64:
		return "Float64"
	// todo more types
	default:
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
		return strconv.FormatInt(int64(*(*int32)(unsafe.Pointer(&val))), 10)
	case reflect.Int64:
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
		//nolint:staticcheck
		if c.kind == reflect.Array && c.rtype.Elem().Kind() == reflect.Uint8 {
			// todo
		}
		if val, ok := any(val).(fmt.Stringer); ok {
			return val.String()
		}
		return fmt.Sprintf("%v", val)
	}
}
