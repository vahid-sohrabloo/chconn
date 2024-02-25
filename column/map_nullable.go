package column

import (
	"reflect"

	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

// MapNullable is a column of Map(K,V) ClickHouse data type where V is nullable.
// Map in clickhouse actually is a array of pair(K,V)
type MapNullable[K comparable, V any] struct {
	Map[K, V]
	valueColumn     NullableColumn[V]
	keyColumnData   []K
	valueColumnData []*V
}

// NewMapNullable create a new map column of Map(K,V) ClickHouse data type
func NewMapNullable[K comparable, V any](
	keyColumn Column[K],
	valueColumn NullableColumn[V],
) *MapNullable[K, V] {
	a := &MapNullable[K, V]{
		valueColumn: valueColumn,
		Map: Map[K, V]{
			MapBase: MapBase{
				keyColumn:     keyColumn,
				valueColumn:   valueColumn,
				offsetColumn:  New[uint64](),
				mapChconnType: "MapNullable[]" + reflect.TypeOf((*K)(nil)).String() + ", " + reflect.TypeOf((*V)(nil)).String() + "]",
			},
		},
	}
	return a
}

// Data get all the data in current block as a slice.
func (c *MapNullable[T, V]) DataP() []map[T]*V {
	values := make([]map[T]*V, c.offsetColumn.numRow)
	var lastOffset uint64
	for i := 0; i < c.offsetColumn.numRow; i++ {
		val := make(map[T]*V)
		offset := c.offsetColumn.Row(i)
		for ki, key := range c.keyColumnData[lastOffset:offset] {
			v := c.valueColumnData[lastOffset:offset][ki]
			val[key] = v
		}
		values[i] = val
		lastOffset = c.offsetColumn.Row(i)
	}
	return values
}

// Read reads all the data in current block and append to column.
func (c *MapNullable[T, V]) ReadP(value []map[T]*V) []map[T]*V {
	return append(value, c.DataP()...)
}

// Row return the value of given row.
// NOTE: Row number start from zero
func (c *MapNullable[T, V]) RowP(row int) map[T]*V {
	var lastOffset uint64
	if row != 0 {
		lastOffset = c.offsetColumn.Row(row - 1)
	}
	val := make(map[T]*V)
	offset := c.offsetColumn.Row(row)
	for ki, key := range c.keyColumnData[lastOffset:offset] {
		v := c.valueColumnData[lastOffset:offset][ki]
		val[key] = v
	}
	return val
}

func (c *MapNullable[K, V]) AppendP(v map[K]*V) {
	c.AppendLen(len(v))
	for k, d := range v {
		c.keyColumn.(Column[K]).Append(k)
		c.valueColumn.AppendP(d)
	}
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *MapNullable[K, V]) ReadRaw(num int, r *readerwriter.Reader) error {
	err := c.Map.ReadRaw(num, r)
	if err != nil {
		return err
	}

	c.keyColumnData = c.keyColumn.(Column[K]).Data()
	c.valueColumnData = c.valueColumn.DataP()

	return nil
}

// ValueColumn return the value column
func (c *MapNullable[K, V]) ValueColumn() NullableColumn[V] {
	return c.valueColumn
}
