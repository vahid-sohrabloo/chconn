package column

import (
	"fmt"
	"io"
	"reflect"
	"slices"
	"sync"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

// Dynamic represents a ClickHouse Dynamic column type that can hold values of different types at runtime.
type Dynamic struct {
	column
	variant              *Variant
	withDynamicColumn    bool
	columnsAppend        map[reflect.Type]ColumnCore
	discriminatorsAppend []ColumnCore
	columnsType          [][]byte
	// columnsRead          map[string]ColumnCore
	sharedVariant *SharedVariant
}

// NewDynamic creates a new Dynamic column that can hold values of different types at runtime.
func NewDynamic(columns ...ColumnCore) *Dynamic {
	if len(columns) > 0 {
		// Check for duplicate column types
		seen := make(map[string]struct{}, len(columns))
		for _, col := range columns {
			if _, ok := col.(*SharedVariant); ok {
				continue
			}
			ft := col.FullType()
			if _, exists := seen[ft]; exists {
				panic("Dynamic column has duplicate column type: " + ft)
			}
			seen[ft] = struct{}{}
		}

		sharedIndex := slices.IndexFunc(columns, func(c ColumnCore) bool {
			_, ok := c.(*SharedVariant)
			return ok
		})
		var sharedVariant *SharedVariant

		if sharedIndex == -1 {
			sharedVariant = NewSharedVariant()
			columns = append(columns, NewSharedVariant())
		} else {
			sharedVariant = columns[sharedIndex].(*SharedVariant)
		}
		return &Dynamic{
			variant:       NewVariant(columns...),
			sharedVariant: sharedVariant,
		}
	}
	sharedVariant := NewSharedVariant()

	return &Dynamic{
		withDynamicColumn: true,
		sharedVariant:     sharedVariant,
		columnsAppend:     make(map[reflect.Type]ColumnCore),
		// columnsRead:       make(map[string]ColumnCore),
	}
}

// NumRow return number of row for this block
func (c *Dynamic) NumRow() int {
	if c.withDynamicColumn {
		return len(c.discriminatorsAppend)
	}
	return c.variant.NumRow()
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *Dynamic) Reset() {
	if c.withDynamicColumn {
		c.discriminatorsAppend = c.discriminatorsAppend[:0]
		for _, col := range c.columnsAppend {
			col.Reset()
		}
		c.sharedVariant.Reset()
		for i := range c.columnsType {
			c.columnsType[i] = c.columnsType[i][:0]
		}
	}
	if c.variant != nil {
		c.variant.Reset()
	}
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *Dynamic) SetWriteBufferSize(row int) {
	c.variant.SetWriteBufferSize(row)
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *Dynamic) ReadRaw(num int) error {
	err := c.variant.ReadRaw(num)
	return err
}

func (c *Dynamic) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	err := c.column.ReadHeader(r, serverInfo)
	if err != nil {
		return err
	}

	if !helper.IsDynamic(c.columnHeader.ChType) {
		panic("not dynamic")
	}
	version, err := r.Uint64()
	if err != nil {
		return fmt.Errorf("dynamic: read version: %w", err)
	}

	if version != 2 {
		if _, err := r.Uvarint(); err != nil {
			return fmt.Errorf("dynamic: read max number of columns: %w", err)
		}
	}

	totalTypes, err := r.Uvarint()

	if err != nil {
		return fmt.Errorf("dynamic: read total types: %w", err)
	}
	if c.variant == nil {
		c.variant = NewVariant(c.sharedVariant)
	} else {
		for _, col := range c.variant.columns {
			dColumnsPool.putColumn(string(col.Type()), col)
		}
		c.variant.columns[0] = c.sharedVariant
		c.variant.columns = c.variant.columns[:1]
	}

	for i := range totalTypes {
		strLen, err := r.Uvarint()
		if err != nil {
			return fmt.Errorf("dynamic: read variant type len: %w", err)
		}
		if len(c.columnsType) <= int(i) {
			c.columnsType = append(c.columnsType, make([]byte, strLen))
		} else {
			helper.ResetSlice(c.columnsType[i], int(strLen), false)
		}
		_, err = r.Read(c.columnsType[i])
		if err != nil {
			return fmt.Errorf("dynamic: read variant type at index %d: %w", i, err)
		}

		column, err := dColumnsPool.getColumn(string(c.columnsType[i]), serverInfo.Timezone)
		if err != nil {
			return fmt.Errorf("dynamic: read variant type at index %d: %w", i, err)
		}
		err = column.SetColumnHeader(ColumnHeader{
			ChType: c.columnsType[i],
		})
		if err != nil {
			return fmt.Errorf("dynamic: set header column for %s: %w", string(c.columnsType[i]), err)
		}
		c.variant.columns = append(c.variant.columns, column)
	}

	c.variant.reorderColumn()

	err = c.variant.ReadHeader(r, serverInfo)
	if err != nil {
		return fmt.Errorf("dynamic: read variant header: %w", err)
	}

	return nil
}

// Append value to the column
func (c *Dynamic) Append(v any) {
	err := c.AppendAny(v)
	if err != nil {
		c.appendErr = err
	}
}

func (c *Dynamic) canAppend(value any) bool {
	return true
}

func (c *Dynamic) AppendAny(value any) error {
	if !c.withDynamicColumn {
		return c.variant.AppendAny(value)
	}
	if value == nil {
		c.discriminatorsAppend = append(c.discriminatorsAppend, nil)
		return nil
	}
	rtype := reflect.TypeOf(value)
	if rtype.Kind() == reflect.Ptr {
		rtype = rtype.Elem()
	}
	column, ok := c.columnsAppend[rtype]
	if !ok {
		var err error
		column, err = type2Column(rtype, 0, false)
		if err != nil {
			return err
		}
		c.columnsAppend[rtype] = column
	}
	c.discriminatorsAppend = append(c.discriminatorsAppend, column)
	return column.AppendAny(value)
}

// AppendMulti append multiple value to the column
func (c *Dynamic) AppendMulti(v ...any) {
	for _, val := range v {
		c.Append(val)
	}
}

// Data get all the data in current block as a slice.
func (c *Dynamic) Data() []any {
	return c.variant.Data()
}

// Read reads all the data in current block and append to the input.
func (c *Dynamic) Read(value []any) []any {
	return c.variant.Read(value)
}

// Row return the value of given row
func (c *Dynamic) Row(row int) any {
	return c.RowAny(row)
}

// RowAny return the value of given row as any.
func (c *Dynamic) RowAny(row int) any {
	return c.variant.RowAny(row)
}

// RowIsNil returns true if the row is nil
func (c *Dynamic) RowIsNil(row int) bool {
	if !c.withDynamicColumn {
		return c.discriminatorsAppend[row] == nil
	}
	return c.variant.RowIsNil(row)
}

// RowPos returns the column index and row index of the given row
//
// will return 0, 0 if its for insert with empty column
func (c *Dynamic) RowPos(row int) (columnIndex uint8, columnRow int) {
	if !c.withDynamicColumn {
		return 0, 0
	}
	return c.variant.RowPos(row)
}

// Scan value from column to dest
func (c *Dynamic) Scan(row int, dest any) error {
	return c.variant.Scan(row, dest)
}

func (c *Dynamic) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	if !helper.IsDynamic(c.columnHeader.ChType) {
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	return nil
}

// Validate is validate the column for insert and select.
// it uses internally
func (c *Dynamic) ValidateInsert() error {
	return nil
}

func (c *Dynamic) chconnType() string {
	return "column.Dynamic()"
}

func (c *Dynamic) structType() string {
	return helper.DynamicStr
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *Dynamic) WriteTo(w io.Writer) (int64, error) {
	return c.variant.WriteTo(w)
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *Dynamic) HeaderWriter(w *readerwriter.Writer) {
	if c.withDynamicColumn {
		columns := make([]ColumnCore, 0, len(c.columnsAppend))
		for _, col := range c.columnsAppend {
			columns = append(columns, col)
		}
		columns = append(columns, c.sharedVariant)
		c.variant = NewVariant(columns...)
		c.variant.discriminators.SetWriteBufferSize(len(c.discriminatorsAppend))
		c.variant.discriminators.Reset()
		for _, col := range c.discriminatorsAppend {
			if col == nil {
				c.variant.discriminators.Append(255)
				continue
			}
			c.variant.discriminators.Append(col.getLocationInParent())
		}
	}

	w.Uint64(2)
	// -1 because of shared variant
	w.Uvarint(uint64(len(c.variant.columns)) - 1)

	for _, col := range c.variant.columns {
		if _, ok := col.(*SharedVariant); ok {
			continue
		}
		w.String(col.FullType())
	}
	c.variant.HeaderWriter(w)
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *Dynamic) Remove(n int) {
	if c.withDynamicColumn {
		// Count how many rows to keep per sub-column (entries before index n)
		keepCount := make(map[ColumnCore]int, len(c.columnsAppend))
		for _, d := range c.discriminatorsAppend[:n] {
			if d != nil {
				keepCount[d]++
			}
		}
		c.discriminatorsAppend = c.discriminatorsAppend[:n]
		for _, col := range c.columnsAppend {
			col.Remove(keepCount[col])
		}
		return
	}
	c.variant.Remove(n)
}

func (c *Dynamic) Delete(start, end int) {
	if c.NumRow() == 0 || c.NumRow() <= start {
		return
	}
	if end > c.NumRow() {
		end = c.NumRow()
	}
	if start >= end {
		return
	}

	if c.withDynamicColumn {
		c.dynamicDeleteRows(func(row int) bool {
			return row >= start && row < end
		})
		return
	}
	c.variant.Delete(start, end)
}

func (c *Dynamic) DeleteFunc(del func(row int) bool) {
	if c.NumRow() == 0 {
		return
	}
	if c.withDynamicColumn {
		c.dynamicDeleteRows(del)
		return
	}
	c.variant.DeleteFunc(del)
}

// dynamicDeleteRows removes rows matching del from the dynamic column path (withDynamicColumn=true).
func (c *Dynamic) dynamicDeleteRows(del func(row int) bool) {
	// Track which sub-column rows to delete
	subDeletes := make(map[ColumnCore][]bool, len(c.columnsAppend))
	for _, col := range c.columnsAppend {
		subDeletes[col] = make([]bool, col.NumRow())
	}

	keepIndex := 0
	subRowIndex := make(map[ColumnCore]int, len(c.columnsAppend))
	for row := 0; row < len(c.discriminatorsAppend); row++ {
		col := c.discriminatorsAppend[row]
		if del(row) {
			if col != nil {
				subDeletes[col][subRowIndex[col]] = true
				subRowIndex[col]++
			}
			continue
		}
		if col != nil {
			subRowIndex[col]++
		}
		c.discriminatorsAppend[keepIndex] = col
		keepIndex++
	}

	c.discriminatorsAppend = c.discriminatorsAppend[:keepIndex]

	// Delete rows from each sub-column
	for col, dels := range subDeletes {
		col.DeleteFunc(func(row int) bool {
			return dels[row]
		})
	}
}

func (c *Dynamic) startBatchDelete() {
	if c.withDynamicColumn {
		return
	}
	c.variant.startBatchDelete()
}

func (c *Dynamic) batchDeleteKeep(start, end int) {
	if c.withDynamicColumn {
		return
	}
	c.variant.batchDeleteKeep(start, end)
}

func (c *Dynamic) endBatchDelete() {
	if c.withDynamicColumn {
		return
	}
	c.variant.endBatchDelete()
}

func (c *Dynamic) FullType() string {
	if len(c.columnHeader.Name) == 0 {
		return "Dynamic()"
	}
	return string(c.columnHeader.Name) + " Dynamic()"
}

func (c *Dynamic) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	return c.variant.ToJSON(row, ignoreDoubleQuotes, b)
}

// Array return a Array type for this column
func (c *Dynamic) Array() *Array[any] {
	return NewArray[any](c)
}

func (c *Dynamic) Elem(arrayLevel int) ColumnCore {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

func (c *Dynamic) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexDynamic))
	w.Uint8(255)
}

type dynamicColumnPool struct {
	pools map[string]*sync.Pool
	mu    sync.Mutex
}

func newDynamicColumnPool() *dynamicColumnPool {
	return &dynamicColumnPool{
		pools: make(map[string]*sync.Pool),
	}
}

var dColumnsPool = newDynamicColumnPool()

// getColumn fetches an object from the pool based on type
func (cp *dynamicColumnPool) getColumn(colType, timezone string) (ColumnCore, error) {
	cp.mu.Lock()
	pool, exists := cp.pools[colType]
	if !exists {
		// Create a new pool for this type
		pool = &sync.Pool{}
		cp.pools[colType] = pool
	}
	cp.mu.Unlock()

	col := pool.Get()
	if col == nil {
		col, err := ColumnByType([]byte(colType), 0, false, false, timezone)
		return col, err
	}
	return col.(ColumnCore), nil
}

func (cp *dynamicColumnPool) putColumn(colType string, col ColumnCore) {
	cp.mu.Lock()
	pool, exists := cp.pools[colType]
	if !exists {
		// If a pool for this type does not exist, create one
		pool = &sync.Pool{}
		cp.pools[colType] = pool
	}
	cp.mu.Unlock()

	pool.Put(col)
}
