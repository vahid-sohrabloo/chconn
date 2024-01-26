package column

import (
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/types"
)

//nolint:funlen,gocyclo
func (c *Base[T]) Scan(row int, dest any) error {
	switch dest := dest.(type) {
	case *bool:
		val, err := c.getBool(c.Row(row))
		*dest = val
		return err
	case **bool:
		val, err := c.getBool(c.Row(row))
		if *dest == nil {
			*dest = new(bool)
		}
		**dest = val
		return err
	case *int8:
		val, err := c.getInt64(c.Row(row))
		*dest = int8(val)
		return err
	case **int8:
		val, err := c.getInt64(c.Row(row))
		if *dest == nil {
			*dest = new(int8)
		}
		**dest = int8(val)
		return err
	case *int16:
		val, err := c.getInt64(c.Row(row))
		*dest = int16(val)
		return err
	case **int16:
		val, err := c.getInt64(c.Row(row))
		if *dest == nil {
			*dest = new(int16)
		}
		**dest = int16(val)
		return err
	case *int32:
		val, err := c.getInt64(c.Row(row))
		*dest = int32(val)
		return err
	case **int32:
		val, err := c.getInt64(c.Row(row))
		if *dest == nil {
			*dest = new(int32)
		}
		**dest = int32(val)
		return err
	case *int64:
		val, err := c.getInt64(c.Row(row))
		*dest = val
		return err
	case **int64:
		val, err := c.getInt64(c.Row(row))
		if *dest == nil {
			*dest = new(int64)
		}
		**dest = val
		return err
	case *uint8:
		val, err := c.getUint64(c.Row(row))
		*dest = uint8(val)
		return err
	case **uint8:
		val, err := c.getUint64(c.Row(row))
		if *dest == nil {
			*dest = new(uint8)
		}
		**dest = uint8(val)
		return err
	case *uint16:
		val, err := c.getUint64(c.Row(row))
		*dest = uint16(val)
		return err
	case **uint16:
		val, err := c.getUint64(c.Row(row))
		if *dest == nil {
			*dest = new(uint16)
		}
		**dest = uint16(val)
		return err
	case *uint32:
		val, err := c.getUint64(c.Row(row))
		*dest = uint32(val)
		return err
	case **uint32:
		val, err := c.getUint64(c.Row(row))
		if *dest == nil {
			*dest = new(uint32)
		}
		**dest = uint32(val)
		return err
	case *uint64:
		val, err := c.getUint64(c.Row(row))
		*dest = val
		return err
	case **uint64:
		val, err := c.getUint64(c.Row(row))
		if *dest == nil {
			*dest = new(uint64)
		}
		**dest = val
		return err
	case *float32:
		val, err := c.geFloat64(c.Row(row))
		*dest = float32(val)
		return err
	case **float32:
		val, err := c.geFloat64(c.Row(row))
		if *dest == nil {
			*dest = new(float32)
		}
		**dest = float32(val)
		return err
	case *float64:
		val, err := c.geFloat64(c.Row(row))
		*dest = val
		return err
	case **float64:
		val, err := c.geFloat64(c.Row(row))
		if *dest == nil {
			*dest = new(float64)
		}
		**dest = val
		return err
	case *string:
		*dest = c.String(row)
		return nil
	case **string:
		if *dest == nil {
			*dest = new(string)
		}
		**dest = c.String(row)
		return nil
	case *types.Uint128:
		val, err := c.getUint128(c.Row(row))
		*dest = val
		return err
	case **types.Uint128:
		val, err := c.getUint128(c.Row(row))
		if *dest == nil {
			*dest = new(types.Uint128)
		}
		**dest = val
		return err
	case *types.Int128:
		val, err := c.getInt128(c.Row(row))
		*dest = val
		return err
	case **types.Int128:
		val, err := c.getInt128(c.Row(row))
		if *dest == nil {
			*dest = new(types.Int128)
		}
		**dest = val
		return err
	case *types.Uint256:
		val, err := c.getUint256(c.Row(row))
		*dest = val
		return err
	case **types.Uint256:
		val, err := c.getUint256(c.Row(row))
		if *dest == nil {
			*dest = new(types.Uint256)
		}
		**dest = val
		return err
	case *types.Int256:
		val, err := c.getInt256(c.Row(row))
		*dest = val
		return err
	case **types.Int256:
		val, err := c.getInt256(c.Row(row))
		if *dest == nil {
			*dest = new(types.Int256)
		}
		**dest = val
		return err
	case *types.Decimal32:
		if c.isDecimal != decimal32Type {
			return fmt.Errorf("column is not decimal32")
		}
		val := c.Row(row)
		*dest = (*(*types.Decimal32)(unsafe.Pointer(&val)))
		return nil
	case **types.Decimal32:
		if c.isDecimal != decimal32Type {
			return fmt.Errorf("column is not decimal32")
		}
		if *dest == nil {
			*dest = new(types.Decimal32)
		}
		val := c.Row(row)
		**dest = (*(*types.Decimal32)(unsafe.Pointer(&val)))
		return nil
	case *types.Decimal64:
		if c.isDecimal != decimal64Type {
			return fmt.Errorf("column is not decimal64")
		}
		val := c.Row(row)
		*dest = (*(*types.Decimal64)(unsafe.Pointer(&val)))
		return nil
	case **types.Decimal64:
		if c.isDecimal != decimal64Type {
			return fmt.Errorf("column is not decimal64")
		}
		if *dest == nil {
			*dest = new(types.Decimal64)
		}
		val := c.Row(row)
		**dest = (*(*types.Decimal64)(unsafe.Pointer(&val)))
		return nil
	case *types.Decimal128:
		if c.isDecimal != decimal128Type {
			return fmt.Errorf("column is not decimal128")
		}
		val := c.Row(row)
		*dest = (*(*types.Decimal128)(unsafe.Pointer(&val)))
		return nil
	case **types.Decimal128:
		if c.isDecimal != decimal128Type {
			return fmt.Errorf("column is not decimal128")
		}
		if *dest == nil {
			*dest = new(types.Decimal128)
		}
		val := c.Row(row)
		**dest = (*(*types.Decimal128)(unsafe.Pointer(&val)))
		return nil
	case *types.Decimal256:
		if c.isDecimal != decimal256Type {
			return fmt.Errorf("column is not decimal256")
		}
		val := c.Row(row)
		*dest = (*(*types.Decimal256)(unsafe.Pointer(&val)))
		return nil
	case **types.Decimal256:
		if c.isDecimal != decimal256Type {
			return fmt.Errorf("column is not decimal256")
		}
		if *dest == nil {
			*dest = new(types.Decimal256)
		}
		val := c.Row(row)
		**dest = (*(*types.Decimal256)(unsafe.Pointer(&val)))
		return nil
	case *any:
		val := c.Row(row)
		if c.isDecimal == decimal32Type {
			*dest = (*(*types.Decimal32)(unsafe.Pointer(&val))).Float64(c.getDecimalScale())
			return nil
		}
		if c.isDecimal == decimal64Type {
			*dest = (*(*types.Decimal64)(unsafe.Pointer(&val))).Float64(c.getDecimalScale())
			return nil
		}
		*dest = c.Row(row)
		return nil
	case *time.Time:
		panic(c.rtype.Name())
	}

	val := reflect.ValueOf(dest)
	return c.ScanValue(row, val)
}

func (c *Base[T]) ScanValue(row int, val reflect.Value) error {
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("scan dest should be a pointer")
	}

	if val.Elem().Kind() == reflect.Pointer {
		if val.Elem().IsNil() {
			val.Elem().Set(reflect.New(val.Type().Elem().Elem()))
		}
		err := c.ScanValue(row, val.Elem())
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

func (c *Base[T]) getDecimalScale() int {
	scale := 0
	if len(c.params) >= 2 {
		scale = c.params[1].(int)
	}
	return scale
}

func (c *Base[T]) getInt64(val T) (int64, error) {
	if c.kind == reflect.Int8 {
		return int64(*(*int8)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int16 {
		return int64(*(*int16)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int32 {
		if c.isDecimal == decimal32Type {
			return int64((*(*types.Decimal32)(unsafe.Pointer(&val))).Float64(c.getDecimalScale())), nil
		}
		return int64(*(*int32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int64 {
		if c.isDecimal == decimal64Type {
			return int64((*(*types.Decimal64)(unsafe.Pointer(&val))).Float64(c.getDecimalScale())), nil
		}
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
		switch v := any(val).(type) {
		case types.Uint128:
			return int64(v.Uint64()), nil
		case types.Int128:
			return int64(v.Uint64()), nil
		case types.Uint256:
			return int64(v.Uint64()), nil
		case types.Int256:
			return int64(v.Uint64()), nil
		case types.Decimal128:
			return int64(v.Float64(c.getDecimalScale())), nil
		case types.Decimal256:
			return int64(v.Float64(c.getDecimalScale())), nil
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
		if c.isDecimal == decimal32Type {
			return uint64((*(*types.Decimal32)(unsafe.Pointer(&val))).Float64(c.getDecimalScale())), nil
		}
		return uint64(*(*int32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int64 {
		if c.isDecimal == decimal64Type {
			return uint64((*(*types.Decimal64)(unsafe.Pointer(&val))).Float64(c.getDecimalScale())), nil
		}
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
		switch v := any(val).(type) {
		case types.Uint128:
			return v.Uint64(), nil
		case types.Int128:
			return v.Uint64(), nil
		case types.Uint256:
			return v.Uint64(), nil
		case types.Int256:
			return v.Uint64(), nil
		case types.Decimal128:
			return uint64(v.Float64(c.getDecimalScale())), nil
		case types.Decimal256:
			return uint64(v.Float64(c.getDecimalScale())), nil
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
		if c.isDecimal == decimal32Type {
			return (*(*types.Decimal32)(unsafe.Pointer(&val))).Float64(c.getDecimalScale()), nil
		}
		return float64(*(*int32)(unsafe.Pointer(&val))), nil
	}
	if c.kind == reflect.Int64 {
		if c.isDecimal == decimal64Type {
			return (*(*types.Decimal64)(unsafe.Pointer(&val))).Float64(c.getDecimalScale()), nil
		}
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
		switch v := any(val).(type) {
		case types.Uint128:
			return float64(v.Uint64()), nil
		case types.Int128:
			return float64(v.Uint64()), nil
		case types.Uint256:
			return float64(v.Uint64()), nil
		case types.Int256:
			return float64(v.Uint64()), nil
		case types.Decimal128:
			return v.Float64(c.getDecimalScale()), nil
		case types.Decimal256:
			return v.Float64(c.getDecimalScale()), nil
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
		switch v := any(val).(type) {
		case types.Uint128:
			return v.Uint64() > 0, nil
		case types.Int128:
			return v.Uint64() > 0, nil
		case types.Uint256:
			return v.Uint64() > 0, nil
		case types.Int256:
			return v.Uint64() > 0, nil
		case types.Decimal128:
			return v.Float64(0) > 0, nil
		case types.Decimal256:
			return v.Float64(0) > 0, nil
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
		if c.isDecimal == decimal32Type {
			return types.Uint128From64(uint64((*(*types.Decimal32)(unsafe.Pointer(&val))).Float64(c.getDecimalScale()))), nil
		}
		return types.Uint128From64(uint64(*(*int32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int64 {
		if c.isDecimal == decimal64Type {
			return types.Uint128From64(uint64((*(*types.Decimal64)(unsafe.Pointer(&val))).Float64(c.getDecimalScale()))), nil
		}
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
		switch v := any(val).(type) {
		case types.Uint128:
			return v, nil
		case types.Int128:
			return v.Uint128(), nil
		case types.Uint256:
			return v.Uint128(), nil
		case types.Int256:
			return v.Uint128(), nil
		case types.Decimal128:
			return v.ToInt128(c.getDecimalScale()).Uint128(), nil
		case types.Decimal256:
			return v.ToInt256(c.getDecimalScale()).Uint128(), nil
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
		if c.isDecimal == decimal32Type {
			return types.Int128From64(int64((*(*types.Decimal32)(unsafe.Pointer(&val))).Float64(c.getDecimalScale()))), nil
		}
		return types.Int128From64(int64(*(*int32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int64 {
		if c.isDecimal == decimal64Type {
			return types.Int128From64(int64((*(*types.Decimal64)(unsafe.Pointer(&val))).Float64(c.getDecimalScale()))), nil
		}
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
			return v.ToInt128(c.getDecimalScale()), nil
		case types.Decimal256:
			return v.ToInt256(c.getDecimalScale()).Uint128().Int128(), nil
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
		if c.isDecimal == decimal32Type {
			return types.Uint256From64(uint64((*(*types.Decimal32)(unsafe.Pointer(&val))).Float64(c.getDecimalScale()))), nil
		}
		return types.Uint256From64(uint64(*(*int32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int64 {
		if c.isDecimal == decimal64Type {
			return types.Uint256From64(uint64((*(*types.Decimal64)(unsafe.Pointer(&val))).Float64(c.getDecimalScale()))), nil
		}
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
			return types.Uint256From128(v.ToInt128(c.getDecimalScale()).Uint128()), nil
		case types.Decimal256:
			return v.ToInt256(c.getDecimalScale()).Uint256(), nil
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
		if c.isDecimal == decimal32Type {
			return types.Int256From64(int64((*(*types.Decimal32)(unsafe.Pointer(&val))).Float64(c.getDecimalScale()))), nil
		}
		return types.Int256From64(int64(*(*int32)(unsafe.Pointer(&val)))), nil
	}
	if c.kind == reflect.Int64 {
		if c.isDecimal == decimal64Type {
			return types.Int256From64(int64((*(*types.Decimal64)(unsafe.Pointer(&val))).Float64(c.getDecimalScale()))), nil
		}
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
			return types.Int256From128(v.ToInt128(c.getDecimalScale())), nil
		case types.Decimal256:
			return v.ToInt256(c.getDecimalScale()), nil
		}
	}

	return types.Int256{}, fmt.Errorf("unsupported type: %s", c.kind)
}
