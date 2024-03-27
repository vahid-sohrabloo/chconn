package column

import (
	"database/sql"
	"reflect"
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/types"
)

//nolint:gocyclo
func (c *Base[T]) Scan(row int, dest any) error {
	switch dest := dest.(type) {
	case *T:
		*dest = c.Row(row)
		return nil
	case **T:
		*dest = new(T)
		**dest = c.Row(row)
		return nil
	case *float32:
		if c.kind == reflect.Int32 && c.isDecimal == decimal32Type {
			v := c.Row(row)
			*dest = float32((*types.Decimal32)(unsafe.Pointer(&v)).Float64(c.getDecimalScale()))
			return nil
		}
	case **float32:
		if c.kind == reflect.Int32 && c.isDecimal == decimal32Type {
			if *dest == nil {
				*dest = new(float32)
			}
			v := c.Row(row)
			**dest = float32((*types.Decimal32)(unsafe.Pointer(&v)).Float64(c.getDecimalScale()))
			return nil
		}
	case *float64:
		if c.kind == reflect.Int64 && c.isDecimal == decimal64Type {
			v := c.Row(row)
			*dest = (*types.Decimal64)(unsafe.Pointer(&v)).Float64(c.getDecimalScale())
			return nil
		}
	case **float64:
		if c.kind == reflect.Int64 && c.isDecimal == decimal64Type {
			if *dest == nil {
				*dest = new(float64)
			}
			v := c.Row(row)
			**dest = (*types.Decimal64)(unsafe.Pointer(&v)).Float64(c.getDecimalScale())
			return nil
		}
	case *any:
		val := c.Row(row)
		if c.isDecimal == decimal32Type {
			*dest = (*types.Decimal32)(unsafe.Pointer(&val)).Float64(c.getDecimalScale())
			return nil
		}
		if c.isDecimal == decimal64Type {
			*dest = (*types.Decimal64)(unsafe.Pointer(&val)).Float64(c.getDecimalScale())
			return nil
		}
		*dest = c.Row(row)
		return nil
	case sql.Scanner:
		return dest.Scan(c.Row(row))
	}

	return ErrScanType{
		destType:   reflect.TypeOf(dest).String(),
		columnType: "*" + c.rtype.String(),
	}
}

func (c *Base[T]) getDecimalScale() int {
	scale := 0
	if len(c.params) >= 2 {
		scale = c.params[1].(int)
	}
	return scale
}
