package column

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

const (
	jsonDeprecatedSerializationVersion = uint64(0)
	jsonStringSerializationVersion     = uint64(1)
	jsonObjectSerializationVersion     = uint64(3)
	jsonUnsetSerializationVersion      = ^uint64(0)
)

type JSON struct {
	column

	dynamicPaths     []string
	dynamicPathIndex map[string]int
	dynamicColumns   []*Dynamic

	jsonStrings *String

	serializationVersion uint64
	rows                 int
}

func NewJSON() *JSON {
	return &JSON{
		dynamicPathIndex:     make(map[string]int),
		dynamicPaths:         make([]string, 0),
		dynamicColumns:       make([]*Dynamic, 0),
		jsonStrings:          NewString(),
		serializationVersion: jsonUnsetSerializationVersion,
	}
}

func (c *JSON) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	chType := helper.FilterSimpleAggregate(c.columnHeader.ChType)
	if !helper.IsJSON(chType) {
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}

	if len(chType) > helper.LenJSONStr {
		return fmt.Errorf("json: typed JSON definition is not supported yet: %s", string(chType))
	}

	// prepare backing string column for string serialization
	if err := c.jsonStrings.SetColumnHeader(ColumnHeader{ChType: []byte("String")}); err != nil {
		return fmt.Errorf("json: set inner string header: %w", err)
	}
	c.serializationVersion = jsonStringSerializationVersion

	return nil
}

func (c *JSON) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	if err := c.column.ReadHeader(r, serverInfo); err != nil {
		return err
	}

	version, err := r.Uint64()
	if err != nil {
		return fmt.Errorf("json: read serialization version: %w", err)
	}
	c.serializationVersion = version
	fmt.Println("JSON serialization version:", version)

	switch version {
	case jsonStringSerializationVersion:
		return c.jsonStrings.ReadHeader(r, serverInfo)
	case jsonObjectSerializationVersion:
		return c.decodeObjectHeader(r, serverInfo)
	case jsonDeprecatedSerializationVersion:
		return fmt.Errorf("json: deprecated serialization version %d is not supported", version)
	default:
		return fmt.Errorf("json: unsupported serialization version %d", version)
	}
}

func (c *JSON) decodeObjectHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	totalDynamicPaths, err := r.Uvarint()
	if err != nil {
		return fmt.Errorf("json: read dynamic path count: %w", err)
	}

	for k := range c.dynamicPathIndex {
		delete(c.dynamicPathIndex, k)
	}
	c.dynamicPaths = c.dynamicPaths[:0]

	for _, col := range c.dynamicColumns {
		col.Reset()
	}
	c.dynamicColumns = c.dynamicColumns[:0]

	for i := uint64(0); i < totalDynamicPaths; i++ {
		pathBytes, err := r.ReadBytes(nil)
		if err != nil {
			return fmt.Errorf("json: read dynamic path %d: %w", i, err)
		}
		path := string(pathBytes)

		dynamicCol := NewDynamic()
		if err := dynamicCol.SetColumnHeader(ColumnHeader{ChType: []byte("Dynamic()")}); err != nil {
			return fmt.Errorf("json: prepare dynamic column for path %q: %w", path, err)
		}
		if err := dynamicCol.ReadHeader(r, serverInfo); err != nil {
			return fmt.Errorf("json: read dynamic header for path %q: %w", path, err)
		}

		c.dynamicPaths = append(c.dynamicPaths, path)
		c.dynamicColumns = append(c.dynamicColumns, dynamicCol)
		c.dynamicPathIndex[path] = len(c.dynamicPaths) - 1
	}

	return nil
}

func (c *JSON) ReadRaw(num int) error {
	c.rows = num

	switch c.serializationVersion {
	case jsonStringSerializationVersion:
		return c.jsonStrings.ReadRaw(num)
	case jsonObjectSerializationVersion:
		for i, col := range c.dynamicColumns {
			if err := col.ReadRaw(num); err != nil {
				return fmt.Errorf("json: read dynamic data for path %q: %w", c.dynamicPaths[i], err)
			}
		}
		return nil
	default:
		return fmt.Errorf("json: unsupported serialization version %d", c.serializationVersion)
	}
}

func (c *JSON) Reset() {
	c.rows = 0
	if c.isStringSerialization() {
		c.jsonStrings.Reset()
	}
	for _, col := range c.dynamicColumns {
		col.Reset()
	}
}

func (c *JSON) SetWriteBufferSize(row int) {
	if c.isStringSerialization() {
		c.jsonStrings.SetWriteBufferSize(row)
	}
}

func (c *JSON) Append(v any) {
	if err := c.AppendAny(v); err != nil {
		c.appendErr = err
	}
}

func (c *JSON) AppendAny(v any) error {
	if c.serializationVersion == jsonUnsetSerializationVersion {
		c.serializationVersion = jsonStringSerializationVersion
	}
	if c.serializationVersion != jsonStringSerializationVersion {
		return fmt.Errorf("json: append is only supported for string serialization")
	}
	return c.jsonStrings.AppendAny(v)
}

func (c *JSON) AppendMulti(v ...any) {
	for _, val := range v {
		c.Append(val)
	}
}

func (c *JSON) NumRow() int {
	if c.isStringSerialization() {
		return c.jsonStrings.NumRow()
	}
	return c.rows
}

func (c *JSON) rowObject(row int) map[string]any {
	obj := make(map[string]any, len(c.dynamicPaths))
	for i, path := range c.dynamicPaths {
		obj[path] = c.dynamicColumns[i].RowAny(row)
	}
	return obj
}

func (c *JSON) Data() []any {
	if c.isStringSerialization() {
		vals := c.jsonStrings.Data()
		data := make([]any, len(vals))
		for i, v := range vals {
			data[i] = v
		}
		return data
	}
	data := make([]any, c.rows)
	for i := 0; i < c.rows; i++ {
		data[i] = c.rowObject(i)
	}
	return data
}

func (c *JSON) Read(value []any) []any {
	for i := 0; i < c.NumRow(); i++ {
		value = append(value, c.Row(i))
	}
	return value
}

func (c *JSON) Row(row int) any {
	if c.isStringSerialization() {
		return c.jsonStrings.Row(row)
	}
	return c.rowObject(row)
}

func (c *JSON) RowAny(row int) any {
	return c.Row(row)
}

func (c *JSON) Scan(row int, dest any) error {
	switch c.serializationVersion {
	case jsonStringSerializationVersion:
		return c.jsonStrings.Scan(row, dest)
	case jsonUnsetSerializationVersion:
		return c.jsonStrings.Scan(row, dest)
	case jsonObjectSerializationVersion:
		switch d := dest.(type) {
		case *map[string]any:
			*d = c.rowObject(row)
			return nil
		case **map[string]any:
			obj := c.rowObject(row)
			*d = &obj
			return nil
		case *string:
			data, err := json.Marshal(c.rowObject(row))
			if err != nil {
				return fmt.Errorf("json: marshal object: %w", err)
			}
			*d = string(data)
			return nil
		case *[]byte:
			data, err := json.Marshal(c.rowObject(row))
			if err != nil {
				return fmt.Errorf("json: marshal object: %w", err)
			}
			*d = append((*d)[:0], data...)
			return nil
		case *any:
			*d = c.rowObject(row)
			return nil
		default:
			return fmt.Errorf("json: unsupported scan destination %T", dest)
		}
	default:
		return fmt.Errorf("json: unsupported serialization version %d", c.serializationVersion)
	}
}

func (c *JSON) ValidateInsert() error {
	if c.isStringSerialization() {
		return c.jsonStrings.ValidateInsert()
	}
	return fmt.Errorf("json: insert is only supported for string serialization")
}

func (c *JSON) chconnType() string {
	return "column.JSON()"
}

func (c *JSON) structType() string {
	return helper.JSONStr
}

func (c *JSON) WriteTo(w io.Writer) (int64, error) {
	if c.serializationVersion == jsonUnsetSerializationVersion {
		c.serializationVersion = jsonStringSerializationVersion
	}
	if c.serializationVersion != jsonStringSerializationVersion {
		return 0, fmt.Errorf("json: write is only supported for string serialization")
	}
	return c.jsonStrings.WriteTo(w)
}

func (c *JSON) HeaderWriter(w *readerwriter.Writer) {
	switch c.serializationVersion {
	case jsonObjectSerializationVersion:
		w.Uint64(jsonObjectSerializationVersion)
		w.Uvarint(uint64(len(c.dynamicPaths)))
		for _, path := range c.dynamicPaths {
			w.ByteString([]byte(path))
		}
	case jsonStringSerializationVersion, jsonUnsetSerializationVersion:
		c.serializationVersion = jsonStringSerializationVersion
		w.Uint64(jsonStringSerializationVersion)
		c.jsonStrings.HeaderWriter(w)
	default:
		w.Uint64(c.serializationVersion)
	}
}

func (c *JSON) Remove(n int) {
	if c.isStringSerialization() {
		c.jsonStrings.Remove(n)
	}
}

func (c *JSON) Delete(start int, end int) {
	if c.isStringSerialization() {
		c.jsonStrings.Delete(start, end)
	}
}

func (c *JSON) DeleteFunc(del func(row int) bool) {
	if c.isStringSerialization() {
		c.jsonStrings.DeleteFunc(del)
	}
}

func (c *JSON) startBatchDelete() {
	if c.isStringSerialization() {
		c.jsonStrings.startBatchDelete()
	}
}

func (c *JSON) batchDeleteKeep(start, end int) {
	if c.isStringSerialization() {
		c.jsonStrings.batchDeleteKeep(start, end)
	}
}

func (c *JSON) endBatchDelete() {
	if c.isStringSerialization() {
		c.jsonStrings.endBatchDelete()
	}
}

func (c *JSON) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	switch c.serializationVersion {
	case jsonStringSerializationVersion:
		return c.jsonStrings.ToJSON(row, ignoreDoubleQuotes, b)
	case jsonUnsetSerializationVersion:
		return c.jsonStrings.ToJSON(row, ignoreDoubleQuotes, b)
	case jsonObjectSerializationVersion:
		data, err := json.Marshal(c.rowObject(row))
		if err != nil {
			return append(b, 'n', 'u', 'l', 'l')
		}
		return append(b, data...)
	default:
		return append(b, 'n', 'u', 'l', 'l')
	}
}

func (c *JSON) FullType() string {
	if len(c.columnHeader.Name) == 0 {
		return "JSON"
	}
	return string(c.columnHeader.Name) + " JSON"
}

func (c *JSON) Array() *Array[any] {
	return NewArray[any](c)
}

func (c *JSON) Elem(arrayLevel int, nullable bool) ColumnCore {
	return c
}

func (c *JSON) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexJSON))
}

func (c *JSON) canAppend(value any) bool {
	if c.serializationVersion == jsonUnsetSerializationVersion {
		c.serializationVersion = jsonStringSerializationVersion
	}

	if c.serializationVersion != jsonStringSerializationVersion {
		return false
	}
	return c.jsonStrings.canAppend(value)
}

func (c *JSON) isStringSerialization() bool {
	return c.serializationVersion == jsonStringSerializationVersion || c.serializationVersion == jsonUnsetSerializationVersion
}
