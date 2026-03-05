package column

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// structFieldInfo caches the mapping from struct fields to JSON dot-paths.
type structFieldInfo struct {
	// fields maps dot-path to struct field index chain
	fields map[string][]int
}

var structFieldCache sync.Map // map[reflect.Type]*structFieldInfo

func getStructFieldInfo(t reflect.Type) *structFieldInfo {
	if cached, ok := structFieldCache.Load(t); ok {
		return cached.(*structFieldInfo)
	}
	info := &structFieldInfo{
		fields: make(map[string][]int),
	}
	collectStructFields(t, nil, "", info.fields)
	structFieldCache.Store(t, info)
	return info
}

func collectStructFields(t reflect.Type, index []int, prefix string, out map[string][]int) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}

		name := f.Name
		if tag := f.Tag.Get("json"); tag != "" && tag != "-" {
			name = strings.Split(tag, ",")[0]
		}
		if tag := f.Tag.Get("db"); tag != "" && tag != "-" {
			name = strings.Split(tag, ",")[0]
		}

		fieldIndex := append(append([]int(nil), index...), i)
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}

		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			collectStructFields(f.Type, fieldIndex, prefix, out)
			continue
		}

		if f.Type.Kind() == reflect.Struct {
			collectStructFields(f.Type, fieldIndex, path, out)
			continue
		}

		out[path] = fieldIndex
	}
}

func structToJSONValue(rv reflect.Value) *JSONValue {
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	info := getStructFieldInfo(rv.Type())
	jv := NewJSONValue()
	for path, idx := range info.fields {
		fv := rv.FieldByIndex(idx)
		if fv.IsZero() {
			continue
		}
		if fv.Kind() == reflect.Ptr {
			if fv.IsNil() {
				continue
			}
			fv = fv.Elem()
		}
		jv.SetValueAtPath(path, fv.Interface())
	}
	return jv
}

func (c *JSON) scanIntoStruct(row int, dest reflect.Value) error {
	if dest.Kind() == reflect.Ptr {
		if dest.IsNil() {
			dest.Set(reflect.New(dest.Type().Elem()))
		}
		dest = dest.Elem()
	}
	info := getStructFieldInfo(dest.Type())

	for path, idx := range info.fields {
		col, colRow, found := c.findColumnForPath(row, path)
		if !found {
			continue
		}
		fv := dest.FieldByIndex(idx)
		if err := col.Scan(colRow, fv.Addr().Interface()); err != nil {
			return fmt.Errorf("json: scan struct field %q: %w", path, err)
		}
	}
	return nil
}

func (c *JSON) scanIntoMap(row int, dest reflect.Value) error {
	if dest.IsNil() {
		dest.Set(reflect.MakeMap(dest.Type()))
	}
	obj := c.rowObject(row)
	for k, v := range obj {
		dest.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(v))
	}
	return nil
}

// findColumnForPath looks up a path in typed then dynamic columns,
// returning the column, the row within that column, and whether it was found.
func (c *JSON) findColumnForPath(row int, path string) (ColumnCore, int, bool) {
	if idx, ok := c.typedPathIndex[path]; ok {
		return c.typedColumns[idx], row, true
	}
	if idx, ok := c.dynamicPathIndex[path]; ok {
		col := c.dynamicColumns[idx]
		return col, row, true
	}
	return nil, 0, false
}
