package column

import (
	"errors"
	"fmt"
	"math"
	"reflect"
)

func getInt8Value(value any) (int8, bool, error) {
	switch v := value.(type) {
	case int8:
		return v, true, nil
	case int16:
		if v <= math.MaxInt8 && v >= math.MinInt8 {
			return int8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int8", v)
	case int32:
		if v <= math.MaxInt8 && v >= math.MinInt8 {
			return int8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int8", v)
	case int64:
		if v <= math.MaxInt8 && v >= math.MinInt8 {
			return int8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int8", v)
	case int:
		if v <= math.MaxInt8 && v >= math.MinInt8 {
			return int8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int8", v)
	case uint8:
		if v <= math.MaxInt8 {
			return int8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int8", v)
	case uint16:
		if v <= math.MaxInt8 {
			return int8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int8", v)
	case uint32:
		if v <= math.MaxInt8 {
			return int8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int8", v)
	case uint64:
		if v <= math.MaxInt8 {
			return int8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int8", v)
	case uint:
		if v <= math.MaxInt8 {
			return int8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int8", v)
	case float64:
		if v <= math.MaxInt8 && v >= math.MinInt8 {
			return int8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for int8", v)
	case float32:
		if v <= math.MaxInt8 && v >= math.MinInt8 {
			return int8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for int8", v)
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			intVal := val.Int()
			if intVal <= math.MaxInt8 && intVal >= math.MinInt8 {
				return int8(intVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int8", v)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			uintVal := val.Uint()
			if uintVal <= math.MaxInt8 {
				return int8(uintVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int8", v)
		case reflect.Float32, reflect.Float64:
			floatVal := val.Float()
			if floatVal <= math.MaxInt8 && floatVal >= math.MinInt8 {
				return int8(floatVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int8", v)
		}
	}
	return 0, false, nil
}

func getInt16Value(value any) (int16, bool, error) {
	switch v := value.(type) {
	case int8:
		return int16(v), true, nil
	case int16:
		return int16(v), true, nil
	case int32:
		if v <= math.MaxInt16 && v >= math.MinInt16 {
			return int16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int16", v)
	case int64:
		if v <= math.MaxInt16 && v >= math.MinInt16 {
			return int16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int16", v)
	case int:
		if v <= math.MaxInt16 && v >= math.MinInt16 {
			return int16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int16", v)
	case uint8:
		return int16(v), true, nil
	case uint16:
		if v <= math.MaxInt16 {
			return int16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int16", v)
	case uint32:
		if v <= math.MaxInt16 {
			return int16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int16", v)
	case uint64:
		if v <= math.MaxInt16 {
			return int16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int16", v)
	case uint:
		if v <= math.MaxInt16 {
			return int16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int16", v)
	case float64:
		if v <= math.MaxInt16 && v >= math.MinInt16 {
			return int16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for int16", v)
	case float32:
		if v <= math.MaxInt16 && v >= math.MinInt16 {
			return int16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for int16", v)
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			intVal := val.Int()
			if intVal <= math.MaxInt16 && intVal >= math.MinInt16 {
				return int16(intVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int16", v)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			uintVal := val.Uint()
			if uintVal <= math.MaxInt16 {
				return int16(uintVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int16", v)
		case reflect.Float32, reflect.Float64:
			floatVal := val.Float()
			if floatVal <= math.MaxInt16 && floatVal >= math.MinInt16 {
				return int16(floatVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int16", v)
		}
	}
	return 0, false, nil
}

func getInt32Value(value any) (int32, bool, error) {
	switch v := value.(type) {
	case int8:
		return int32(v), true, nil
	case int16:
		return int32(v), true, nil
	case int32:
		return int32(v), true, nil
	case int64:
		if v <= math.MaxInt32 && v >= math.MinInt32 {
			return int32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int32", v)
	case int:
		if v <= math.MaxInt32 && v >= math.MinInt32 {
			return int32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int32", v)
	case uint8:
		return int32(v), true, nil
	case uint16:
		return int32(v), true, nil
	case uint32:
		if v <= math.MaxInt32 {
			return int32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int32", v)
	case uint64:
		if v <= math.MaxInt32 {
			return int32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int32", v)
	case uint:
		if v <= math.MaxInt32 {
			return int32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int32", v)
	case float64:
		if v <= math.MaxInt32 && v >= math.MinInt32 {
			return int32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for int32", v)
	case float32:
		if v <= math.MaxInt32 && v >= math.MinInt32 {
			return int32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for int32", v)
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			intVal := val.Int()
			if intVal <= math.MaxInt32 && intVal >= math.MinInt32 {
				return int32(intVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int32", v)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			uintVal := val.Uint()
			if uintVal <= math.MaxInt32 {
				return int32(uintVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int32", v)
		case reflect.Float32, reflect.Float64:
			floatVal := val.Float()
			if floatVal <= math.MaxInt32 && floatVal >= math.MinInt32 {
				return int32(floatVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int32", v)
		}
	}
	return 0, false, nil
}

func getInt64Value(value any) (int64, bool, error) {
	switch v := value.(type) {
	case int8:
		return int64(v), true, nil
	case int16:
		return int64(v), true, nil
	case int32:
		return int64(v), true, nil
	case int64:
		return int64(v), true, nil
	case int:
		return int64(v), true, nil
	case uint8:
		return int64(v), true, nil
	case uint16:
		return int64(v), true, nil
	case uint32:
		return int64(v), true, nil
	case uint64:
		if v <= math.MaxInt64 {
			return int64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int64", v)
	case uint:
		if v <= math.MaxInt64 {
			return int64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int64", v)
	case float64:
		if v <= math.MaxInt64 && v >= math.MinInt64 {
			return int64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for int64", v)
	case float32:
		if v <= math.MaxInt64 && v >= math.MinInt64 {
			return int64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for int64", v)
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return int64(val.Int()), true, nil
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			uintVal := val.Uint()
			if uintVal <= math.MaxInt64 {
				return int64(uintVal), true, nil
			}
			return 0, false, fmt.Errorf("value %d is out of range for int64", v)
		case reflect.Float32, reflect.Float64:
			floatVal := val.Float()
			if floatVal <= math.MaxInt64 && floatVal >= math.MinInt64 {
				return int64(floatVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int64", v)
		}
	}
	return 0, false, nil
}

func getIntValue(value any) (int, bool, error) {
	switch v := value.(type) {
	case int8:
		return int(v), true, nil
	case int16:
		return int(v), true, nil
	case int32:
		return int(v), true, nil
	case int64:
		if v <= math.MaxInt && v >= math.MinInt {
			return int(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int", v)
	case int:
		return int(v), true, nil
	case uint8:
		return int(v), true, nil
	case uint16:
		return int(v), true, nil
	case uint32:
		if v <= math.MaxInt {
			return int(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int", v)
	case uint64:
		if v <= math.MaxInt {
			return int(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int", v)
	case uint:
		if v <= math.MaxInt {
			return int(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for int", v)
	case float64:
		if v <= math.MaxInt && v >= math.MinInt {
			return int(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for int", v)
	case float32:
		if v <= math.MaxInt && v >= math.MinInt {
			return int(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for int", v)
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			intVal := val.Int()
			if intVal <= math.MaxInt && intVal >= math.MinInt {
				return int(intVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int", v)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			uintVal := val.Uint()
			if uintVal <= math.MaxInt && uintVal >= math.MinInt {
				return int(uintVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int", v)
		case reflect.Float32, reflect.Float64:
			floatVal := val.Float()
			if floatVal <= math.MaxInt && floatVal >= math.MinInt {
				return int(floatVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for int", v)
		}
	}
	return 0, false, nil
}

func getUint8Value(value any) (uint8, bool, error) {
	switch v := value.(type) {
	case int8:
		if v >= 0 {
			return uint8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
	case int16:
		if v <= math.MaxUint8 && v >= 0 {
			return uint8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
	case int32:
		if v <= math.MaxUint8 && v >= 0 {
			return uint8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
	case int64:
		if v <= math.MaxUint8 && v >= 0 {
			return uint8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
	case int:
		if v <= math.MaxUint8 && v >= 0 {
			return uint8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
	case uint8:
		return uint8(v), true, nil
	case uint16:
		if v <= math.MaxUint8 {
			return uint8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
	case uint32:
		if v <= math.MaxUint8 {
			return uint8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
	case uint64:
		if v <= math.MaxUint8 {
			return uint8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
	case uint:
		if v <= math.MaxUint8 {
			return uint8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
	case float64:
		if v <= math.MaxUint8 && v >= 0 {
			return uint8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for uint8", v)
	case float32:
		if v <= math.MaxUint8 && v >= 0 {
			return uint8(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for uint8", v)
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			intVal := val.Int()
			if intVal <= math.MaxUint8 && intVal >= 0 {
				return uint8(intVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			uintVal := val.Uint()
			if uintVal <= math.MaxUint8 {
				return uint8(uintVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
		case reflect.Float32, reflect.Float64:
			floatVal := val.Float()
			if floatVal <= math.MaxUint8 && floatVal >= 0 {
				return uint8(floatVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint8", v)
		}
	}
	return 0, false, nil
}

func getUint16Value(value any) (uint16, bool, error) {
	switch v := value.(type) {
	case int8:
		if v >= 0 {
			return uint16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint16", v)
	case int16:
		if v >= 0 {
			return uint16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint16", v)
	case int32:
		if v <= math.MaxUint16 && v >= 0 {
			return uint16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint16", v)
	case int64:
		if v <= math.MaxUint16 && v >= 0 {
			return uint16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint16", v)
	case int:
		if v <= math.MaxUint16 && v >= 0 {
			return uint16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint16", v)
	case uint8:
		return uint16(v), true, nil
	case uint16:
		return uint16(v), true, nil
	case uint32:
		if v <= math.MaxUint16 {
			return uint16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint16", v)
	case uint64:
		if v <= math.MaxUint16 {
			return uint16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint16", v)
	case uint:
		if v <= math.MaxUint16 {
			return uint16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint16", v)
	case float64:
		if v <= math.MaxUint16 && v >= 0 {
			return uint16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for uint16", v)
	case float32:
		if v <= math.MaxUint16 && v >= 0 {
			return uint16(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for uint16", v)
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			intVal := val.Int()
			if intVal <= math.MaxUint16 && intVal >= 0 {
				return uint16(intVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint16", v)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			uintVal := val.Uint()
			if uintVal <= math.MaxUint16 {
				return uint16(uintVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint16", v)
		case reflect.Float32, reflect.Float64:
			floatVal := val.Float()
			if floatVal <= math.MaxUint16 && floatVal >= 0 {
				return uint16(floatVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint16", v)
		}
	}
	return 0, false, nil
}

func getUint32Value(value any) (uint32, bool, error) {
	switch v := value.(type) {
	case int8:
		if v >= 0 {
			return uint32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint32", v)
	case int16:
		if v >= 0 {
			return uint32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint32", v)
	case int32:
		if v >= 0 {
			return uint32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint32", v)
	case int64:
		if v <= math.MaxUint32 && v >= 0 {
			return uint32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint32", v)
	case int:
		if v <= math.MaxUint32 && v >= 0 {
			return uint32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint32", v)
	case uint8:
		return uint32(v), true, nil
	case uint16:
		return uint32(v), true, nil
	case uint32:
		return uint32(v), true, nil
	case uint64:
		if v <= math.MaxUint32 {
			return uint32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint32", v)
	case uint:
		if v <= math.MaxUint32 {
			return uint32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint32", v)
	case float64:
		if v <= math.MaxUint32 && v >= 0 {
			return uint32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for uint32", v)
	case float32:
		if v <= math.MaxUint32 && v >= 0 {
			return uint32(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for uint32", v)
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			intVal := val.Int()
			if intVal <= math.MaxUint32 && intVal >= 0 {
				return uint32(intVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint32", v)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			uintVal := val.Uint()
			if uintVal <= math.MaxUint32 {
				return uint32(uintVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint32", v)
		case reflect.Float32, reflect.Float64:
			floatVal := val.Float()
			if floatVal <= math.MaxUint32 && floatVal >= 0 {
				return uint32(floatVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint32", v)
		}
	}
	return 0, false, nil
}

func getUint64Value(value any) (uint64, bool, error) {
	switch v := value.(type) {
	case int8:
		if v >= 0 {
			return uint64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint64", v)
	case int16:
		if v >= 0 {
			return uint64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint64", v)
	case int32:
		if v >= 0 {
			return uint64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint64", v)
	case int64:
		if v <= math.MaxUint64 && v >= 0 {
			return uint64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint64", v)
	case int:
		if v <= math.MaxUint64 && v >= 0 {
			return uint64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint64", v)
	case uint8:
		return uint64(v), true, nil
	case uint16:
		return uint64(v), true, nil
	case uint32:
		return uint64(v), true, nil
	case uint64:
		if v <= math.MaxUint64 {
			return uint64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint64", v)
	case uint:
		if v <= math.MaxUint64 {
			return uint64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint64", v)
	case float64:
		if v <= math.MaxUint64 && v >= 0 {
			return uint64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for uint64", v)
	case float32:
		if v <= math.MaxUint64 && v >= 0 {
			return uint64(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for uint64", v)
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			intVal := val.Int()
			if intVal <= math.MaxUint64 && intVal >= 0 {
				return uint64(intVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint64", v)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return uint64(val.Uint()), true, nil
		case reflect.Float32, reflect.Float64:
			floatVal := val.Float()
			if floatVal <= math.MaxUint64 && floatVal >= 0 {
				return uint64(floatVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint64", v)
		}
	}
	return 0, false, nil
}

func getUintValue(value any) (uint, bool, error) {
	switch v := value.(type) {
	case int8:
		if v >= 0 {
			return uint(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint", v)
	case int16:
		if v >= 0 {
			return uint(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint", v)
	case int32:
		if v >= 0 {
			return uint(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint", v)
	case int64:
		if v <= math.MaxUint && v >= 0 {
			return uint(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint", v)
	case int:
		if v <= math.MaxUint && v >= 0 {
			return uint(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint", v)
	case uint8:
		return uint(v), true, nil
	case uint16:
		return uint(v), true, nil
	case uint32:
		return uint(v), true, nil
	case uint64:
		if v <= math.MaxUint {
			return uint(v), true, nil
		}

		return 0, false, fmt.Errorf("value %d is out of range for uint", v)
	case uint:
		return uint(v), true, nil
	case float64:
		if v <= math.MaxUint && v >= 0 {
			return uint(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for uint", v)
	case float32:
		if v <= math.MaxUint && v >= 0 {
			return uint(v), true, nil
		}

		return 0, false, fmt.Errorf("value %f is out of range for uint", v)
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			intVal := val.Int()
			if intVal <= math.MaxUint && intVal >= 0 {
				return uint(intVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint", v)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			uintVal := val.Uint()
			if uintVal <= math.MaxUint && uintVal >= 0 {
				return uint(uintVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint", v)
		case reflect.Float32, reflect.Float64:
			floatVal := val.Float()
			if floatVal <= math.MaxUint && floatVal >= 0 {
				return uint(floatVal), true, nil
			}

			return 0, false, fmt.Errorf("value %d is out of range for uint", v)
		}
	}
	return 0, false, nil
}

func getFloat32Value(value any) (float32, bool, error) {
	switch v := value.(type) {
	case int8:
		return float32(v), true, nil
	case int16:
		return float32(v), true, nil
	case int32:
		return float32(v), true, nil
	case int64:
		return float32(v), true, nil
	case int:
		return float32(v), true, nil
	case uint8:
		return float32(v), true, nil
	case uint16:
		return float32(v), true, nil
	case uint32:
		return float32(v), true, nil
	case uint64:
		return float32(v), true, nil
	case uint:
		return float32(v), true, nil
	case float64:
		return float32(v), true, nil
	case float32:
		return float32(v), true, nil
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return float32(val.Int()), true, nil
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return float32(val.Uint()), true, nil
		case reflect.Float32, reflect.Float64:
			return float32(val.Float()), true, nil
		}
	}
	return 0, false, nil
}

func getFloat64Value(value any) (float64, bool, error) {
	switch v := value.(type) {
	case int8:
		return float64(v), true, nil
	case int16:
		return float64(v), true, nil
	case int32:
		return float64(v), true, nil
	case int64:
		return float64(v), true, nil
	case int:
		return float64(v), true, nil
	case uint8:
		return float64(v), true, nil
	case uint16:
		return float64(v), true, nil
	case uint32:
		return float64(v), true, nil
	case uint64:
		return float64(v), true, nil
	case uint:
		return float64(v), true, nil
	case float64:
		return float64(v), true, nil
	case float32:
		return float64(v), true, nil
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return float64(val.Int()), true, nil
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return float64(val.Uint()), true, nil
		case reflect.Float32, reflect.Float64:
			return float64(val.Float()), true, nil
		}
	}
	return 0, false, nil
}

func getBoolValue(value any) (bool, bool, error) {
	switch v := value.(type) {
	case bool:
		return bool(v), true, nil
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Bool:
			return bool(val.Bool()), true, nil
		}
	}
	return false, false, nil
}

func getStringValue(value any) (string, bool, error) {
	switch v := value.(type) {
	case string:
		return string(v), true, nil
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.String:
			return string(val.String()), true, nil
		}
	}
	return "", false, nil
}

func tryConvert(val reflect.Value, targetType reflect.Type) (any, error) {
	// Ensure that the source is a convertible type.
	if !val.IsValid() {
		return nil, errors.New("invalid source value")
	}

	// Check if both types are numerical
	if val.Kind() >= reflect.Int && val.Kind() <= reflect.Float64 && targetType.Kind() >= reflect.Int && targetType.Kind() <= reflect.Float64 {
		switch targetType.Kind() {
		case reflect.Float32, reflect.Float64:
			convertedVal := val.Convert(targetType)
			if val.Kind() != reflect.Float32 && val.Kind() != reflect.Float64 {
				// Check if the conversion back to the original type equals the original value.
				if convertedVal.Convert(val.Type()).Interface() != val.Interface() {
					return nil, fmt.Errorf("conversion would result in precision loss")
				}
			}
			return convertedVal.Interface(), nil
		default:
			err := checkIntOverflowUnderflow(val, targetType)
			if err != nil {
				return nil, err
			}

			if val.Type().ConvertibleTo(targetType) {
				return val.Convert(targetType).Interface(), nil
			} else {
				return nil, fmt.Errorf("cannot convert value of type %s to %s", val.Type().String(), targetType.String())
			}
		}
	} else {
		if val.Type().ConvertibleTo(targetType) {
			return val.Convert(targetType).Interface(), nil
		} else {
			return nil, fmt.Errorf("cannot convert value of type %s to %s", val.Type().String(), targetType.String())
		}
	}
}

func checkIntOverflowUnderflow(val reflect.Value, targetType reflect.Type) error {
	// Convert val to the largest float64 for comparison, regardless of its actual type.
	valFloat := val.Convert(reflect.TypeOf(float64(0))).Float()

	minVal := float64(math.MinInt64)
	maxVal := float64(math.MaxInt64)

	switch targetType.Kind() {
	case reflect.Int8:
		minVal = float64(math.MinInt8)
		maxVal = float64(math.MaxInt8)
	case reflect.Int16:
		minVal = float64(math.MinInt16)
		maxVal = float64(math.MaxInt16)
	case reflect.Int32:
		minVal = float64(math.MinInt32)
		maxVal = float64(math.MaxInt32)
	case reflect.Int64:
		minVal = float64(math.MinInt64)
		maxVal = float64(math.MaxInt64)
	case reflect.Int:
		minVal = float64(math.MinInt)
		maxVal = float64(math.MaxInt)
	case reflect.Uint8:
		minVal = 0
		maxVal = float64(math.MaxUint8)
	case reflect.Uint16:
		minVal = 0
		maxVal = float64(math.MaxUint16)
	case reflect.Uint32:
		minVal = 0
		maxVal = float64(math.MaxUint32)
	}

	if valFloat < minVal || valFloat > maxVal {
		return fmt.Errorf("value %v overflows target type %s", valFloat, targetType.String())
	}
	return nil
}
