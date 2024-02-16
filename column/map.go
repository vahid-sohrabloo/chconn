package column

import (
	"fmt"
	"reflect"
)

// Map is a column of Map(K,V) ClickHouse data type
// Map in clickhouse actually is a array of pair(K,V)
type Map[K comparable, V any] struct {
	MapBase
	keyColumnData   []K
	valueColumnData []V
}

// NewMap create a new map column of Map(K,V) ClickHouse data type
func NewMap[K comparable, V any](
	keyColumn Column[K],
	valueColumn Column[V],
) *Map[K, V] {
	a := &Map[K, V]{
		MapBase: MapBase{
			keyColumn:    keyColumn,
			valueColumn:  valueColumn,
			offsetColumn: New[uint64](),
		},
	}
	a.resetHook = func() {
		a.keyColumnData = a.keyColumnData[:0]
		a.valueColumnData = a.valueColumnData[:0]
	}
	return a
}

// Data get all the data in current block as a slice.
func (c *Map[K, V]) Data() []map[K]V {
	values := make([]map[K]V, c.offsetColumn.numRow)
	offsets := c.Offsets()
	if len(offsets) == 0 {
		return values
	}
	keyColumnData := c.getKeyColumnData()
	valueColumnData := c.getValueColumnData()
	var lastOffset uint64
	for i, offset := range offsets {
		val := make(map[K]V)
		for ki, key := range keyColumnData[lastOffset:offset] {
			val[key] = valueColumnData[lastOffset:offset][ki]
		}
		values[i] = val
		lastOffset = offset
	}
	return values
}

// Read reads all the data in current block and append to the input.
func (c *Map[K, V]) Read(value []map[K]V) []map[K]V {
	return append(value, c.Data()...)
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *Map[K, V]) Row(row int) map[K]V {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	keyColumnData := c.getKeyColumnData()
	valueColumnData := c.getValueColumnData()

	val := make(map[K]V)
	offset := c.offsetColumn.Row(row)
	for ki, key := range keyColumnData[lastOffset:offset] {
		val[key] = valueColumnData[lastOffset:offset][ki]
	}
	return val
}

// RowAny return the value of given row.
// NOTE: Row number start from zero
func (c *Map[K, V]) RowAny(row int) any {
	return c.Row(row)
}

// Append value for insert
func (c *Map[K, V]) Append(v map[K]V) {
	c.AppendLen(len(v))
	for k, d := range v {
		c.keyColumn.(Column[K]).Append(k)
		c.valueColumn.(Column[V]).Append(d)
	}
}

func (c *Map[K, V]) AppendAny(value any) error {
	switch v := value.(type) {
	case map[K]V:
		c.Append(v)
	case map[any]any:
		c.AppendLen(len(v))
		for k, val := range v {
			err := c.keyColumn.(Column[K]).AppendAny(k)
			if err != nil {
				return fmt.Errorf("coult not append key %v to key column: %w", k, err)
			}
			err = c.valueColumn.(Column[V]).AppendAny(val)
			if err != nil {
				return fmt.Errorf("coult not append value %v to value column: %w", val, err)
			}
		}

		return nil
	default:
		mapVal := reflect.ValueOf(value)
		if mapVal.Kind() != reflect.Map {
			return fmt.Errorf("value is not a map")
		}

		for _, key := range mapVal.MapKeys() {
			k := key.Interface()
			err := c.keyColumn.(Column[K]).AppendAny(k)
			if err != nil {
				return fmt.Errorf("coult not append key %v to key column: %w", k, err)
			}
			val := mapVal.MapIndex(key).Interface()
			err = c.valueColumn.(Column[V]).AppendAny(val)
			if err != nil {
				return fmt.Errorf("coult not append value %v to value column: %w", val, err)
			}
		}

		return nil
	}
	return nil
}

// AppendMulti value for insert
func (c *Map[K, V]) AppendMulti(val ...map[K]V) {
	for _, v := range val {
		c.AppendLen(len(v))
		for k, d := range v {
			c.keyColumn.(Column[K]).Append(k)
			c.valueColumn.(Column[V]).Append(d)
		}
	}
}

func (c *Map[K, V]) getKeyColumnData() []K {
	if len(c.keyColumnData) == 0 {
		c.keyColumnData = c.keyColumn.(Column[K]).Data()
	}
	return c.keyColumnData
}
func (c *Map[K, V]) getValueColumnData() []V {
	if len(c.valueColumnData) == 0 {
		c.valueColumnData = c.valueColumn.(Column[V]).Data()
	}
	return c.valueColumnData
}

// KeyColumn return the key column
func (c *Map[K, V]) KeyColumn() Column[K] {
	return c.keyColumn.(Column[K])
}

// ValueColumn return the value column
func (c *Map[K, V]) ValueColumn() Column[V] {
	return c.valueColumn.(Column[V])
}

// Array return a Array type for this column
func (c *Map[K, V]) Array() *Array[map[K]V] {
	return NewArray[map[K]V](c)
}
