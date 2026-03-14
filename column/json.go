package column

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

const (
	// ClickHouse JSON serialization versions:
	// V1=0: object mode with max_dynamic_paths prefix
	// STRING=1: string mode
	// V2=2: object mode without max_dynamic_paths
	// FLATTENED=3: flattened mode
	// V3=4: object mode with shared data
	jsonObjectV1SerializationVersion   = uint64(0)
	jsonStringSerializationVersion     = uint64(1)
	jsonObjectV2SerializationVersion   = uint64(2)
	jsonFlattenedSerializationVersion  = uint64(3)
	jsonObjectV3SerializationVersion   = uint64(4)
	jsonUnsetSerializationVersion      = ^uint64(0)
)

type JSON struct {
	column

	typedPaths     []string
	typedPathIndex map[string]int
	typedColumns   []ColumnCore

	dynamicPaths     []string
	dynamicPathIndex map[string]int
	dynamicColumns   []*Dynamic

	jsonStrings      *String
	maxDynamicPaths  uint64
	serverTimeZone   string

	serializationVersion uint64
	rows                 int
}

func NewJSON() *JSON {
	return &JSON{
		typedPathIndex:       make(map[string]int),
		typedPaths:           make([]string, 0),
		typedColumns:         make([]ColumnCore, 0),
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

	// prepare backing string column for string serialization
	if err := c.jsonStrings.SetColumnHeader(ColumnHeader{ChType: []byte("String")}); err != nil {
		return fmt.Errorf("json: set inner string header: %w", err)
	}
	c.serializationVersion = jsonStringSerializationVersion

	// parse typed path definitions from JSON(a String, b.c Int64, max_dynamic_paths=N)
	if len(chType) > helper.LenJSONStr+1 {
		inner := chType[helper.LenJSONStr+1 : len(chType)-1] // strip "JSON(" and ")"
		if err := c.parseTypedDefinitions(inner); err != nil {
			return fmt.Errorf("json: parse typed definitions: %w", err)
		}
	}

	return nil
}

func (c *JSON) parseTypedDefinitions(inner []byte) error {
	entries, err := helper.TypesInParentheses(inner)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		s := string(bytes.TrimSpace(entry.ChType))
		// check for settings like max_dynamic_paths=N
		if idx := strings.IndexByte(s, '='); idx >= 0 {
			key := strings.TrimSpace(s[:idx])
			val := strings.TrimSpace(s[idx+1:])
			if key == "max_dynamic_paths" {
				n, err := strconv.ParseUint(val, 10, 64)
				if err != nil {
					return fmt.Errorf("invalid max_dynamic_paths value %q: %w", val, err)
				}
				c.maxDynamicPaths = n
			}
			continue
		}
		// typed path definition: name is the path, chType is the column type
		if len(entry.Name) == 0 {
			continue
		}
		path := string(entry.Name)
		col, err := ColumnByType(entry.ChType, 0, false, false, c.serverTimeZone)
		if err != nil {
			return fmt.Errorf("create column for typed path %q (%s): %w", path, string(entry.ChType), err)
		}
		if err := col.SetColumnHeader(ColumnHeader{ChType: entry.ChType, Name: entry.Name}); err != nil {
			return fmt.Errorf("set header for typed path %q: %w", path, err)
		}
		c.typedPathIndex[path] = len(c.typedPaths)
		c.typedPaths = append(c.typedPaths, path)
		c.typedColumns = append(c.typedColumns, col)
	}
	return nil
}

func (c *JSON) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	if err := c.column.ReadHeader(r, serverInfo); err != nil {
		return err
	}
	c.serverTimeZone = serverInfo.Timezone

	version, err := r.Uint64()
	if err != nil {
		return fmt.Errorf("json: read serialization version: %w", err)
	}
	c.serializationVersion = version

	switch version {
	case jsonStringSerializationVersion:
		return c.jsonStrings.ReadHeader(r, serverInfo)
	case jsonObjectV1SerializationVersion, jsonObjectV2SerializationVersion,
		jsonObjectV3SerializationVersion:
		return c.decodeObjectHeader(r, serverInfo)
	default:
		return fmt.Errorf("json: unsupported serialization version %d", version)
	}
}

func (c *JSON) decodeObjectHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	// V1 (version 0) has an extra max_dynamic_paths field before the path count
	if c.serializationVersion == jsonObjectV1SerializationVersion {
		maxDynPaths, err := r.Uvarint()
		if err != nil {
			return fmt.Errorf("json: read max_dynamic_paths: %w", err)
		}
		c.maxDynamicPaths = maxDynPaths
	}

	// V3 (version 4) has shared data version and a statistics flag
	if c.serializationVersion == jsonObjectV3SerializationVersion {
		// Read shared data serialization version (uint64)
		if _, err := r.Uint64(); err != nil {
			return fmt.Errorf("json: read shared data version: %w", err)
		}
		// Read statistics flag (bool as uint8)
		if _, err := r.ReadByte(); err != nil {
			return fmt.Errorf("json: read statistics flag: %w", err)
		}
	}

	// Read number of dynamic paths
	totalDynamicPaths, err := r.Uvarint()
	if err != nil {
		return fmt.Errorf("json: read dynamic path count: %w", err)
	}

	// Clear dynamic state
	for k := range c.dynamicPathIndex {
		delete(c.dynamicPathIndex, k)
	}
	c.dynamicPaths = c.dynamicPaths[:0]
	for _, col := range c.dynamicColumns {
		col.Reset()
	}
	c.dynamicColumns = c.dynamicColumns[:0]

	// Read dynamic path names
	dynPathNames := make([]string, totalDynamicPaths)
	for i := uint64(0); i < totalDynamicPaths; i++ {
		pathBytes, err := r.ReadBytes(nil)
		if err != nil {
			return fmt.Errorf("json: read dynamic path %d: %w", i, err)
		}
		dynPathNames[i] = string(pathBytes)
	}

	// Read typed column headers (state prefix)
	for i, col := range c.typedColumns {
		if err := col.ReadHeader(r, serverInfo); err != nil {
			return fmt.Errorf("json: read typed header for path %q: %w", c.typedPaths[i], err)
		}
	}

	// Read dynamic column headers
	for i := uint64(0); i < totalDynamicPaths; i++ {
		path := dynPathNames[i]

		dynamicCol := NewDynamic()
		if err := dynamicCol.SetColumnHeader(ColumnHeader{ChType: []byte("Dynamic")}); err != nil {
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

	if c.isStringSerialization() {
		return c.jsonStrings.ReadRaw(num)
	}
	if c.isObjectSerialization() {
		// Read typed columns first
		for i, col := range c.typedColumns {
			if err := col.ReadRaw(num); err != nil {
				return fmt.Errorf("json: read typed data for path %q: %w", c.typedPaths[i], err)
			}
		}
		// Then dynamic columns
		for i, col := range c.dynamicColumns {
			if err := col.ReadRaw(num); err != nil {
				return fmt.Errorf("json: read dynamic data for path %q: %w", c.dynamicPaths[i], err)
			}
		}
		// V1 includes SharedData: one UInt64 per row at the end
		if c.serializationVersion == jsonObjectV1SerializationVersion && num > 0 {
			sharedDataSize := 8 * num
			if _, err := c.r.Read(make([]byte, sharedDataSize)); err != nil {
				return fmt.Errorf("json: read shared data (%d bytes): %w", sharedDataSize, err)
			}
		}
		return nil
	}
	return fmt.Errorf("json: unsupported serialization version %d", c.serializationVersion)
}

func (c *JSON) Reset() {
	c.rows = 0
	if c.isStringSerialization() {
		c.jsonStrings.Reset()
	}
	for _, col := range c.typedColumns {
		col.Reset()
	}
	for _, col := range c.dynamicColumns {
		col.Reset()
	}
}

func (c *JSON) SetWriteBufferSize(row int) {
	if c.isStringSerialization() {
		c.jsonStrings.SetWriteBufferSize(row)
	}
	for _, col := range c.typedColumns {
		col.SetWriteBufferSize(row)
	}
	for _, col := range c.dynamicColumns {
		col.SetWriteBufferSize(row)
	}
}

func (c *JSON) Append(v any) {
	if err := c.AppendAny(v); err != nil {
		c.appendErr = err
	}
}

func (c *JSON) AppendAny(v any) error {
	switch val := v.(type) {
	case *JSONValue:
		return c.appendJSONValue(val)
	case JSONValue:
		return c.appendJSONValue(&val)
	case map[string]any:
		return c.appendMap(val)
	default:
		// Check for struct via reflection
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		if rv.Kind() == reflect.Struct {
			jv := structToJSONValue(rv)
			return c.appendJSONValue(jv)
		}
		// Fall back to string serialization for string/[]byte values
		if c.serializationVersion == jsonUnsetSerializationVersion {
			c.serializationVersion = jsonStringSerializationVersion
		}
		if c.serializationVersion == jsonStringSerializationVersion {
			return c.jsonStrings.AppendAny(v)
		}
		return fmt.Errorf("json: cannot append value of type %T in object mode", v)
	}
}

func (c *JSON) appendJSONValue(jv *JSONValue) error {
	c.ensureObjectMode()
	c.rows++

	// Route values to typed columns
	typedSeen := make([]bool, len(c.typedPaths))
	for path, val := range jv.valuesByPath {
		if idx, ok := c.typedPathIndex[path]; ok {
			typedSeen[idx] = true
			if err := c.typedColumns[idx].AppendAny(val); err != nil {
				return fmt.Errorf("json: append to typed path %q: %w", path, err)
			}
			continue
		}
		// Route to dynamic columns
		if err := c.appendToDynamic(path, val); err != nil {
			return fmt.Errorf("json: append to dynamic path %q: %w", path, err)
		}
	}

	// Append nil for missing typed paths
	for i, seen := range typedSeen {
		if !seen {
			if err := c.typedColumns[i].AppendAny(nil); err != nil {
				return fmt.Errorf("json: append nil to typed path %q: %w", c.typedPaths[i], err)
			}
		}
	}

	// Backfill nil for dynamic columns that didn't get a value this row
	for i, col := range c.dynamicColumns {
		if col.NumRow() < c.rows {
			col.Append(nil)
			_ = i
		}
	}

	return nil
}

func (c *JSON) appendMap(m map[string]any) error {
	flat := make(map[string]any, len(m))
	flattenMap(m, "", flat)
	jv := &JSONValue{valuesByPath: flat}
	return c.appendJSONValue(jv)
}

func (c *JSON) appendToDynamic(path string, val any) error {
	idx, ok := c.dynamicPathIndex[path]
	if !ok {
		// Create new dynamic column for this path
		dynamicCol := NewDynamic()
		idx = len(c.dynamicPaths)
		c.dynamicPaths = append(c.dynamicPaths, path)
		c.dynamicColumns = append(c.dynamicColumns, dynamicCol)
		c.dynamicPathIndex[path] = idx

		// Backfill nils for all previously inserted rows
		for i := 0; i < c.rows-1; i++ {
			dynamicCol.Append(nil)
		}
	}
	c.dynamicColumns[idx].Append(val)
	return nil
}

func (c *JSON) ensureObjectMode() {
	if c.serializationVersion == jsonUnsetSerializationVersion {
		c.serializationVersion = jsonObjectV1SerializationVersion
	}
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
	obj := make(map[string]any, len(c.typedPaths)+len(c.dynamicPaths))
	for i, path := range c.typedPaths {
		val := c.typedColumns[i].RowAny(row)
		setNestedValue(obj, path, val)
	}
	for i, path := range c.dynamicPaths {
		if c.dynamicColumns[i].RowIsNil(row) {
			continue
		}
		val := c.dynamicColumns[i].RowAny(row)
		setNestedValue(obj, path, val)
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
	if c.isStringSerialization() {
		return c.jsonStrings.Scan(row, dest)
	}
	if c.isObjectSerialization() {
		switch d := dest.(type) {
		case *map[string]any:
			*d = c.rowObject(row)
			return nil
		case **map[string]any:
			obj := c.rowObject(row)
			*d = &obj
			return nil
		case *string:
			b := c.objectToJSON(row, false, nil)
			*d = string(b)
			return nil
		case *[]byte:
			*d = c.objectToJSON(row, false, (*d)[:0])
			return nil
		case *any:
			*d = c.rowObject(row)
			return nil
		default:
			// Try struct scan via reflection
			rv := reflect.ValueOf(dest)
			if rv.Kind() == reflect.Ptr && rv.Elem().Kind() == reflect.Struct {
				return c.scanIntoStruct(row, rv)
			}
			if rv.Kind() == reflect.Ptr && rv.Elem().Kind() == reflect.Map {
				return c.scanIntoMap(row, rv.Elem())
			}
			return fmt.Errorf("json: unsupported scan destination %T", dest)
		}
	}
	return fmt.Errorf("json: unsupported serialization version %d", c.serializationVersion)
}

func (c *JSON) ValidateInsert() error {
	if c.isStringSerialization() {
		return c.jsonStrings.ValidateInsert()
	}
	// Object mode: verify all columns have matching row counts
	for i, col := range c.typedColumns {
		if col.NumRow() != c.rows {
			return fmt.Errorf("json: typed path %q has %d rows, expected %d", c.typedPaths[i], col.NumRow(), c.rows)
		}
	}
	for i, col := range c.dynamicColumns {
		if col.NumRow() != c.rows {
			return fmt.Errorf("json: dynamic path %q has %d rows, expected %d", c.dynamicPaths[i], col.NumRow(), c.rows)
		}
	}
	return nil
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
	if c.serializationVersion == jsonStringSerializationVersion {
		return c.jsonStrings.WriteTo(w)
	}
	// Object mode: write typed columns then dynamic columns
	var n int64
	for i, col := range c.typedColumns {
		nw, err := col.WriteTo(w)
		if err != nil {
			return n, fmt.Errorf("json: write typed path %q: %w", c.typedPaths[i], err)
		}
		n += nw
	}
	for i, col := range c.dynamicColumns {
		nw, err := col.WriteTo(w)
		if err != nil {
			return n, fmt.Errorf("json: write dynamic path %q: %w", c.dynamicPaths[i], err)
		}
		n += nw
	}
	// V1 includes SharedData: one UInt64 (all zeros) per row at the end
	if c.serializationVersion == jsonObjectV1SerializationVersion && c.rows > 0 {
		sharedData := make([]byte, 8*c.rows)
		nw, err := w.Write(sharedData)
		if err != nil {
			return n, fmt.Errorf("json: write shared data: %w", err)
		}
		n += int64(nw)
	}
	return n, nil
}

func (c *JSON) HeaderWriter(w *readerwriter.Writer) {
	if c.isObjectSerialization() {
		w.Uint64(c.serializationVersion)
		if c.serializationVersion == jsonObjectV1SerializationVersion {
			w.Uvarint(c.maxDynamicPaths)
		}
		w.Uvarint(uint64(len(c.dynamicPaths)))
		for _, path := range c.dynamicPaths {
			w.ByteString([]byte(path))
		}
		for _, col := range c.typedColumns {
			col.HeaderWriter(w)
		}
		for _, col := range c.dynamicColumns {
			col.HeaderWriter(w)
		}
		return
	}
	switch c.serializationVersion {
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
		return
	}
	for _, col := range c.typedColumns {
		col.Remove(n)
	}
	for _, col := range c.dynamicColumns {
		col.Remove(n)
	}
	if n < c.rows {
		c.rows = n
	}
}

func (c *JSON) Delete(start int, end int) {
	if c.isStringSerialization() {
		c.jsonStrings.Delete(start, end)
		return
	}
	for _, col := range c.typedColumns {
		col.Delete(start, end)
	}
	for _, col := range c.dynamicColumns {
		col.Delete(start, end)
	}
	c.rows -= end - start
}

func (c *JSON) DeleteFunc(del func(row int) bool) {
	if c.isStringSerialization() {
		c.jsonStrings.DeleteFunc(del)
		return
	}
	for _, col := range c.typedColumns {
		col.DeleteFunc(del)
	}
	for _, col := range c.dynamicColumns {
		col.DeleteFunc(del)
	}
	if len(c.typedColumns) > 0 {
		c.rows = c.typedColumns[0].NumRow()
	} else if len(c.dynamicColumns) > 0 {
		c.rows = c.dynamicColumns[0].NumRow()
	}
}

func (c *JSON) startBatchDelete() {
	if c.isStringSerialization() {
		c.jsonStrings.startBatchDelete()
		return
	}
	for _, col := range c.typedColumns {
		col.startBatchDelete()
	}
	for _, col := range c.dynamicColumns {
		col.startBatchDelete()
	}
}

func (c *JSON) batchDeleteKeep(start, end int) {
	if c.isStringSerialization() {
		c.jsonStrings.batchDeleteKeep(start, end)
		return
	}
	for _, col := range c.typedColumns {
		col.batchDeleteKeep(start, end)
	}
	for _, col := range c.dynamicColumns {
		col.batchDeleteKeep(start, end)
	}
}

func (c *JSON) endBatchDelete() {
	if c.isStringSerialization() {
		c.jsonStrings.endBatchDelete()
		return
	}
	for _, col := range c.typedColumns {
		col.endBatchDelete()
	}
	for _, col := range c.dynamicColumns {
		col.endBatchDelete()
	}
	if len(c.typedColumns) > 0 {
		c.rows = c.typedColumns[0].NumRow()
	} else if len(c.dynamicColumns) > 0 {
		c.rows = c.dynamicColumns[0].NumRow()
	}
}

func (c *JSON) objectToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	// Build a nested structure from typed + dynamic paths, then serialize
	type pathCol struct {
		path string
		col  ColumnCore
	}

	allPaths := make([]pathCol, 0, len(c.typedPaths)+len(c.dynamicPaths))
	for i, path := range c.typedPaths {
		allPaths = append(allPaths, pathCol{path: path, col: c.typedColumns[i]})
	}
	for i, path := range c.dynamicPaths {
		if c.dynamicColumns[i].RowIsNil(row) {
			continue
		}
		allPaths = append(allPaths, pathCol{path: path, col: c.dynamicColumns[i]})
	}

	// Group by top-level key to handle nested paths
	type node struct {
		// leaf: col is set
		col ColumnCore
		// branch: children
		children map[string]*node
		keys     []string // ordered keys
	}

	root := &node{children: make(map[string]*node)}
	for _, pc := range allPaths {
		parts := strings.Split(pc.path, ".")
		cur := root
		for i, part := range parts {
			if i == len(parts)-1 {
				// leaf
				child := &node{col: pc.col}
				if cur.children == nil {
					cur.children = make(map[string]*node)
				}
				if _, exists := cur.children[part]; !exists {
					cur.keys = append(cur.keys, part)
				}
				cur.children[part] = child
			} else {
				if cur.children == nil {
					cur.children = make(map[string]*node)
				}
				if _, exists := cur.children[part]; !exists {
					cur.keys = append(cur.keys, part)
					cur.children[part] = &node{children: make(map[string]*node)}
				}
				cur = cur.children[part]
			}
		}
	}

	var writeNode func(n *node, b []byte) []byte
	writeNode = func(n *node, b []byte) []byte {
		if n.col != nil {
			return n.col.ToJSON(row, ignoreDoubleQuotes, b)
		}
		b = append(b, '{')
		first := true
		for _, key := range n.keys {
			child := n.children[key]
			if !first {
				b = append(b, ',')
			}
			first = false
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			b = append(b, key...)
			if !ignoreDoubleQuotes {
				b = append(b, '"')
			}
			b = append(b, ':')
			b = writeNode(child, b)
		}
		b = append(b, '}')
		return b
	}

	return writeNode(root, b)
}

func (c *JSON) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	if c.isStringSerialization() {
		// In string mode, the stored value is already raw JSON — output as-is
		return append(b, c.jsonStrings.Row(row)...)
	}
	if c.isObjectSerialization() {
		return c.objectToJSON(row, ignoreDoubleQuotes, b)
	}
	return append(b, 'n', 'u', 'l', 'l')
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
	switch value.(type) {
	case *JSONValue, JSONValue, map[string]any:
		return true
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Struct {
		return true
	}

	if c.serializationVersion == jsonUnsetSerializationVersion ||
		c.serializationVersion == jsonStringSerializationVersion {
		return c.jsonStrings.canAppend(value)
	}
	return false
}

func (c *JSON) isStringSerialization() bool {
	return c.serializationVersion == jsonStringSerializationVersion || c.serializationVersion == jsonUnsetSerializationVersion
}

func (c *JSON) isObjectSerialization() bool {
	switch c.serializationVersion {
	case jsonObjectV1SerializationVersion, jsonObjectV2SerializationVersion, jsonObjectV3SerializationVersion:
		return true
	}
	return false
}
