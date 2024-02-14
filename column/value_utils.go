package column

import "reflect"

func getInt8Value(value any) (int8, bool) {
	switch v := value.(type) {
	case int8:
		return v, true
	case int16:
		return int8(v), true
	case int32:
		return int8(v), true
	case int64:
		return int8(v), true
	case int:
		return int8(v), true
	case uint8:
		return int8(v), true
	case uint16:
		return int8(v), true
	case uint32:
		return int8(v), true
	case uint64:
		return int8(v), true
	case uint:
		return int8(v), true
	case float64:
		return int8(v), true
	case float32:
		return int8(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return int8(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return int8(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return int8(val.Float()), true
		}
	}
	return 0, false
}

func getInt16Value(value any) (int16, bool) {
	switch v := value.(type) {
	case int8:
		return int16(v), true
	case int16:
		return int16(v), true
	case int32:
		return int16(v), true
	case int64:
		return int16(v), true
	case int:
		return int16(v), true
	case uint8:
		return int16(v), true
	case uint16:
		return int16(v), true
	case uint32:
		return int16(v), true
	case uint64:
		return int16(v), true
	case uint:
		return int16(v), true
	case float64:
		return int16(v), true
	case float32:
		return int16(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return int16(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return int16(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return int16(val.Float()), true
		}
	}
	return 0, false
}

func getInt32Value(value any) (int32, bool) {
	switch v := value.(type) {
	case int8:
		return int32(v), true
	case int16:
		return int32(v), true
	case int32:
		return int32(v), true
	case int64:
		return int32(v), true
	case int:
		return int32(v), true
	case uint8:
		return int32(v), true
	case uint16:
		return int32(v), true
	case uint32:
		return int32(v), true
	case uint64:
		return int32(v), true
	case uint:
		return int32(v), true
	case float64:
		return int32(v), true
	case float32:
		return int32(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return int32(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return int32(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return int32(val.Float()), true
		}
	}
	return 0, false
}

func getInt64Value(value any) (int64, bool) {
	switch v := value.(type) {
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return int64(v), true
	case int:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case uint:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return int64(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return int64(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return int64(val.Float()), true
		}
	}
	return 0, false
}

func getIntValue(value any) (int, bool) {
	switch v := value.(type) {
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case int:
		return int(v), true
	case uint8:
		return int(v), true
	case uint16:
		return int(v), true
	case uint32:
		return int(v), true
	case uint64:
		return int(v), true
	case uint:
		return int(v), true
	case float64:
		return int(v), true
	case float32:
		return int(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return int(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return int(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return int(val.Float()), true
		}
	}
	return 0, false
}

func getUint8Value(value any) (uint8, bool) {
	switch v := value.(type) {
	case int8:
		return uint8(v), true
	case int16:
		return uint8(v), true
	case int32:
		return uint8(v), true
	case int64:
		return uint8(v), true
	case int:
		return uint8(v), true
	case uint8:
		return uint8(v), true
	case uint16:
		return uint8(v), true
	case uint32:
		return uint8(v), true
	case uint64:
		return uint8(v), true
	case uint:
		return uint8(v), true
	case float64:
		return uint8(v), true
	case float32:
		return uint8(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return uint8(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return uint8(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return uint8(val.Float()), true
		}
	}
	return 0, false
}

func getUint16Value(value any) (uint16, bool) {
	switch v := value.(type) {
	case int8:
		return uint16(v), true
	case int16:
		return uint16(v), true
	case int32:
		return uint16(v), true
	case int64:
		return uint16(v), true
	case int:
		return uint16(v), true
	case uint8:
		return uint16(v), true
	case uint16:
		return uint16(v), true
	case uint32:
		return uint16(v), true
	case uint64:
		return uint16(v), true
	case uint:
		return uint16(v), true
	case float64:
		return uint16(v), true
	case float32:
		return uint16(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return uint16(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return uint16(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return uint16(val.Float()), true
		}
	}
	return 0, false
}

func getUint32Value(value any) (uint32, bool) {
	switch v := value.(type) {
	case int8:
		return uint32(v), true
	case int16:
		return uint32(v), true
	case int32:
		return uint32(v), true
	case int64:
		return uint32(v), true
	case int:
		return uint32(v), true
	case uint8:
		return uint32(v), true
	case uint16:
		return uint32(v), true
	case uint32:
		return uint32(v), true
	case uint64:
		return uint32(v), true
	case uint:
		return uint32(v), true
	case float64:
		return uint32(v), true
	case float32:
		return uint32(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return uint32(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return uint32(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return uint32(val.Float()), true
		}
	}
	return 0, false
}

func getUint64Value(value any) (uint64, bool) {
	switch v := value.(type) {
	case int8:
		return uint64(v), true
	case int16:
		return uint64(v), true
	case int32:
		return uint64(v), true
	case int64:
		return uint64(v), true
	case int:
		return uint64(v), true
	case uint8:
		return uint64(v), true
	case uint16:
		return uint64(v), true
	case uint32:
		return uint64(v), true
	case uint64:
		return uint64(v), true
	case uint:
		return uint64(v), true
	case float64:
		return uint64(v), true
	case float32:
		return uint64(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return uint64(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return uint64(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return uint64(val.Float()), true
		}
	}
	return 0, false
}

func getUintValue(value any) (uint, bool) {
	switch v := value.(type) {
	case int8:
		return uint(v), true
	case int16:
		return uint(v), true
	case int32:
		return uint(v), true
	case int64:
		return uint(v), true
	case int:
		return uint(v), true
	case uint8:
		return uint(v), true
	case uint16:
		return uint(v), true
	case uint32:
		return uint(v), true
	case uint64:
		return uint(v), true
	case uint:
		return uint(v), true
	case float64:
		return uint(v), true
	case float32:
		return uint(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return uint(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return uint(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return uint(val.Float()), true
		}
	}
	return 0, false
}

func getFloat32Value(value any) (float32, bool) {
	switch v := value.(type) {
	case int8:
		return float32(v), true
	case int16:
		return float32(v), true
	case int32:
		return float32(v), true
	case int64:
		return float32(v), true
	case int:
		return float32(v), true
	case uint8:
		return float32(v), true
	case uint16:
		return float32(v), true
	case uint32:
		return float32(v), true
	case uint64:
		return float32(v), true
	case uint:
		return float32(v), true
	case float64:
		return float32(v), true
	case float32:
		return float32(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return float32(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return float32(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return float32(val.Float()), true
		}
	}
	return 0, false
}

func getFloat64Value(value any) (float64, bool) {
	switch v := value.(type) {
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case int:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case uint:
		return float64(v), true
	case float64:
		return float64(v), true
	case float32:
		return float64(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return float64(val.Int()), true
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return float64(val.Uint()), true
		case reflect.Float32, reflect.Float64:
			return float64(val.Float()), true
		}
	}
	return 0, false
}

func getBoolValue(value any) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return bool(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.Bool:
			return bool(val.Bool()), true
		}
	}
	return false, false
}

func getStringValue(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return string(v), true
	default:
		val := reflect.ValueOf(value)
		valKind := val.Kind()
		switch valKind {
		case reflect.String:
			return string(val.String()), true
		}
	}
	return "", false
}
