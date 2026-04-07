package column

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

// Variant is a column of Variant(T1,T2,.....,Tn) ClickHouse data type
type Variant struct {
	column
	discriminators         *Base[uint8]
	discriminatorsIndexPos []int
	columns                []ColumnCore
	totalNils              int
}

// NewVariant create a new Variant of Variant(T1,T2,.....,Tn) ClickHouse data type
func NewVariant(columns ...ColumnCore) *Variant {
	if len(columns) < 1 {
		panic("Variant must have at least one column")
	}
	v := &Variant{
		columns:        columns,
		discriminators: New[uint8](),
	}
	v.reorderColumn()

	return v
}

func (c *Variant) reorderColumn() {
	sort.Slice(c.columns, func(i, j int) bool { return c.columns[i].FullType() < c.columns[j].FullType() })
	for i, col := range c.columns {
		col.setLocationInParent(i)
		col.setVariantParent(c)
	}
}

// NumRow return number of row for this block
func (c *Variant) NumRow() int {
	return c.discriminators.NumRow()
}

// Array return a Array type for this column
func (c *Variant) Array() *Array[any] {
	return NewArray[any](c)
}

// Reset all statuses and buffered data
//
// After each reading, the reading data does not need to be reset. It will be automatically reset.
//
// When inserting, buffers are reset only after the operation is successful.
// If an error occurs, you can safely call insert again.
func (c *Variant) Reset() {
	c.discriminators.Reset()
	c.totalNils = 0
	for _, col := range c.columns {
		col.Reset()
	}
}

// SetWriteBufferSize set write buffer (number of rows)
// this buffer only used for writing.
// By setting this buffer, you will avoid allocating the memory several times.
func (c *Variant) SetWriteBufferSize(row int) {
	c.discriminators.SetWriteBufferSize(row)
	for _, col := range c.columns {
		col.SetWriteBufferSize(row)
	}
}

// ReadRaw read raw data from the reader. it runs automatically
func (c *Variant) ReadRaw(num int) error {
	err := c.discriminators.ReadRaw(num)
	if err != nil {
		return fmt.Errorf("Variant: read discriminators column: %w", err)
	}
	if cap(c.discriminatorsIndexPos) < num {
		c.discriminatorsIndexPos = make([]int, num)
	} else {
		c.discriminatorsIndexPos = c.discriminatorsIndexPos[:num]
	}
	var dataLen [256]int
	for i, n := range c.discriminators.values {
		c.discriminatorsIndexPos[i] = dataLen[n]
		dataLen[n]++
	}
	for i, col := range c.columns {
		err := col.ReadRaw(dataLen[i])
		if err != nil {
			return fmt.Errorf("Variant: read column index %d: %w", i, err)
		}
	}
	return nil
}

func (c *Variant) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	err := c.column.ReadHeader(r, serverInfo)
	if err != nil {
		return err
	}
	c.discriminators.r = r

	// ready SerializationVersion.
	_, err = c.r.Uint64()
	if err != nil {
		return fmt.Errorf("Variant: read version: %w", err)
	}

	for i, col := range c.columns {
		err := col.ReadHeader(r, serverInfo)
		if err != nil {
			return fmt.Errorf("Variant: read column header index %d: %w", i, err)
		}
	}

	return nil
}

// AppendDiscriminators append discriminators to the column
// you can get use column.LocationInParent to get the index of the column
func (c *Variant) AppendDiscriminators(discriminators uint8) {
	c.discriminators.Append(discriminators)
}

// AppendDiscriminatorsMulti append multiple discriminators to the column
func (c *Variant) AppendDiscriminatorsMulti(discriminators ...uint8) {
	c.discriminators.AppendMulti(discriminators...)
}

// AppendNil append nil to the column
func (c *Variant) AppendNil() {
	c.totalNils++
	c.discriminators.Append(255)
}

// Append append value to the column
func (c *Variant) Append(v any) {
	err := c.AppendAny(v)
	if err != nil {
		c.appendErr = err
	}
}

func (c *Variant) canAppend(value any) bool {
	if value == nil {
		return true
	}
	for _, col := range c.columns {
		if col.canAppend(value) {
			return true
		}
	}
	return false
}

func (c *Variant) AppendAny(value any) error {
	if value == nil {
		c.AppendNil()
		return nil
	}
	for _, col := range c.columns {
		if col.canAppend(value) {
			return col.AppendAny(value)
		}
	}

	return fmt.Errorf("cannot append value of type %T to Variant column. can't find a column that can accept this value", value)
}

// AppendMulti append multiple value to the column
func (c *Variant) AppendMulti(v ...any) {
	for _, val := range v {
		c.Append(val)
	}
}

// Data get all the data in current block as a slice.
func (c *Variant) Data() []any {
	val := make([]any, c.NumRow())
	for i := 0; i < c.NumRow(); i++ {
		val[i] = c.Row(i)
	}
	return val
}

// Read reads all the data in current block and append to the input.
func (c *Variant) Read(value []any) []any {
	value = slices.Grow(value, c.NumRow())
	for i := 0; i < c.NumRow(); i++ {
		value = append(value, c.Row(i))
	}
	return value
}

// Row return the value of given row
func (c *Variant) Row(row int) any {
	return c.RowAny(row)
}

// RowAny return the value of given row as any.
func (c *Variant) RowAny(row int) any {
	index := c.discriminators.Row(row)
	if index == 255 {
		return nil
	}

	return c.columns[index].RowAny(c.discriminatorsIndexPos[row])
}

// RowIsNil returns true if the row is nil
func (c *Variant) RowIsNil(row int) bool {
	return c.discriminators.Row(row) == 255
}

// RowPos returns the column index and row index of the given row
func (c *Variant) RowPos(row int) (columnIndex uint8, columnRow int) {
	index := c.discriminators.Row(row)
	if index == 255 {
		return 0, -1
	}
	return index, c.discriminatorsIndexPos[row]
}

// Scan scan value from column to dest
func (c *Variant) Scan(row int, dest any) error {
	columnIndex, columnRow := c.RowPos(row)
	if columnRow == -1 {
		return nil
	}
	return c.columns[columnIndex].Scan(columnRow, dest)
}

// Column returns the all sub columns
func (c *Variant) Columns() []ColumnCore {
	return c.columns
}

func (c *Variant) SetColumnHeader(ch ColumnHeader) error {
	c.columnHeader = ch
	chType := helper.FilterSimpleAggregate(c.columnHeader.ChType)
	if !helper.IsVariant(chType) {
		return &ErrInvalidType{
			chType:     string(c.columnHeader.ChType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	columnsVariant, err := helper.TypesInParentheses(chType[helper.LenVariantStr : len(chType)-1])
	if err != nil {
		return fmt.Errorf("Variant invalid types %w", err)
	}
	if len(columnsVariant) != len(c.columns) {
		//nolint:goerr113
		return fmt.Errorf("columns number for %s (%s) is not equal to Variant columns number: %d != %d",
			string(c.columnHeader.Name),
			string(c.Type()),
			len(columnsVariant),
			len(c.columns),
		)
	}

	for i, col := range c.columns {
		if err := col.SetColumnHeader(ColumnHeader{
			ChType: columnsVariant[i].ChType,
			Name:   columnsVariant[i].Name,
		}); err != nil {
			return fmt.Errorf("Variant: set column header index %d: %w", i, err)
		}
	}
	return nil
}

// Validate is validate the column  for insert and select.
// it uses internally
func (c *Variant) ValidateInsert() error {
	var columnsRowNumber int
	for _, col := range c.columns {
		columnsRowNumber += col.NumRow()
	}
	expectedRows := c.NumRow() - c.totalNils

	if expectedRows != columnsRowNumber {
		return fmt.Errorf("Variant: The total number of rows (excluding nils) does not match the sum of rows across all columns."+
			" Expected %d rows (total rows: %d, nils: %d), but found %d rows in columns",
			expectedRows, c.NumRow(), c.totalNils, columnsRowNumber)
	}

	for _, col := range c.columns {
		if err := col.ValidateInsert(); err != nil {
			if !isInvalidType(err) {
				return err
			}
			return &ErrInvalidType{
				chType:     string(c.columnHeader.ChType),
				chconnType: c.chconnType(),
				goToChType: c.structType(),
			}
		}
	}
	return nil
}

func (c *Variant) chconnType() string {
	var chConn strings.Builder
	chConn.WriteString("column.Variant(")
	for i, col := range c.columns {
		if i > 0 {
			chConn.WriteString(", ")
		}
		chConn.WriteString(col.chconnType())
	}
	chConn.WriteString(")")
	return chConn.String()
}

func (c *Variant) structType() string {
	var str strings.Builder
	str.WriteString(helper.VariantStr)
	for i, col := range c.columns {
		if i > 0 {
			str.WriteString(", ")
		}
		str.WriteString(col.structType())
	}
	str.WriteString(")")
	return str.String()
}

// WriteTo write data to ClickHouse.
// it uses internally
func (c *Variant) WriteTo(w io.Writer) (int64, error) {
	n, err := c.discriminators.WriteTo(w)
	if err != nil {
		return n, fmt.Errorf("Variant: write discriminators column: %w", err)
	}
	for i, col := range c.columns {
		nw, err := col.WriteTo(w)
		if err != nil {
			return n, fmt.Errorf("Variant: write column index %d: %w", i, err)
		}
		n += nw
	}
	return n, nil
}

// HeaderWriter writes header data to writer
// it uses internally
func (c *Variant) HeaderWriter(w *readerwriter.Writer) {
	w.Uint64(0)
	for _, col := range c.columns {
		col.HeaderWriter(w)
	}
}

func (c *Variant) Elem(arrayLevel int) ColumnCore {
	if arrayLevel > 0 {
		return c.Array().elem(arrayLevel - 1)
	}
	return c
}

// Remove inserted value from index
//
// its equal to data = data[:n]
func (c *Variant) Remove(n int) {
	if c.NumRow() == 0 || c.NumRow() <= n {
		return
	}
	var removes [255]int
	nDelete := 0
	for _, v := range c.discriminators.values[n:] {
		if v == 255 {
			c.totalNils--
			nDelete++
			continue
		}
		removes[v]++
	}
	dd := 0
	for i, col := range c.columns {
		if removes[i] == 0 {
			continue
		}
		col.Remove(col.NumRow() - removes[i])
		dd += removes[i]
	}
	c.discriminators.Remove(n)
}

func (c *Variant) Delete(start, end int) {
	if c.NumRow() == 0 || start >= c.NumRow() {
		return
	}
	if end > c.NumRow() {
		end = c.NumRow()
	}
	if start >= end {
		return
	}

	c.deleteRows(func(row int) bool {
		return row >= start && row < end
	})
}

func (c *Variant) DeleteFunc(del func(row int) bool) {
	if c.NumRow() == 0 {
		return
	}
	c.deleteRows(del)
}

// deleteRows removes rows matching the del predicate from discriminators and sub-columns.
func (c *Variant) deleteRows(del func(row int) bool) {
	// Track which sub-column rows to delete
	var subDeletes [256][]bool
	for i := range c.columns {
		subDeletes[i] = make([]bool, c.columns[i].NumRow())
	}

	keepIndex := 0
	var subRowIndex [256]int
	for row := 0; row < c.NumRow(); row++ {
		disc := c.discriminators.values[row]
		if del(row) {
			if disc == 255 {
				c.totalNils--
			} else {
				subDeletes[disc][subRowIndex[disc]] = true
				subRowIndex[disc]++
			}
			continue
		}
		if disc != 255 {
			subRowIndex[disc]++
		}
		c.discriminators.values[keepIndex] = disc
		keepIndex++
	}

	// Trim discriminators
	clear(c.discriminators.values[keepIndex:])
	c.discriminators.values = c.discriminators.values[:keepIndex]
	c.discriminators.numRow = keepIndex

	// Delete rows from each sub-column
	for i, col := range c.columns {
		col.DeleteFunc(func(row int) bool {
			return subDeletes[i][row]
		})
	}

	// Rebuild discriminatorsIndexPos
	if cap(c.discriminatorsIndexPos) < keepIndex {
		c.discriminatorsIndexPos = make([]int, keepIndex)
	} else {
		c.discriminatorsIndexPos = c.discriminatorsIndexPos[:keepIndex]
	}
	var subRowCount [256]int
	for i := 0; i < keepIndex; i++ {
		disc := c.discriminators.values[i]
		if disc != 255 {
			c.discriminatorsIndexPos[i] = subRowCount[disc]
			subRowCount[disc]++
		}
	}
}

func (c *Variant) startBatchDelete() {
	c.discriminators.startBatchDelete()
	for _, col := range c.columns {
		col.startBatchDelete()
	}
}

func (c *Variant) batchDeleteKeep(start, end int) {
	c.discriminators.batchDeleteKeep(start, end)
	// Sub-column batch delete is handled in endBatchDelete since we need
	// to map parent rows to sub-column rows.
}

func (c *Variant) endBatchDelete() {
	c.discriminators.endBatchDelete()

	// After discriminators are compacted, rebuild sub-column data and index positions.
	// Since the batch delete API only tracks which parent rows to keep,
	// we need to rebuild using deleteRows pattern on the compacted discriminators.
	var subRowCount [256]int
	if cap(c.discriminatorsIndexPos) < c.discriminators.numRow {
		c.discriminatorsIndexPos = make([]int, c.discriminators.numRow)
	} else {
		c.discriminatorsIndexPos = c.discriminatorsIndexPos[:c.discriminators.numRow]
	}
	for i := 0; i < c.discriminators.numRow; i++ {
		disc := c.discriminators.values[i]
		if disc != 255 {
			c.discriminatorsIndexPos[i] = subRowCount[disc]
			subRowCount[disc]++
		}
	}

	for _, col := range c.columns {
		col.endBatchDelete()
	}
}

func (c *Variant) FullType() string {
	var chType string
	if len(c.columnHeader.Name) == 0 {
		chType = "Variant("
	} else {
		chType = string(c.columnHeader.Name) + " Variant("
	}
	for _, col := range c.columns {
		chType += col.FullType() + ", "
	}
	return chType[:len(chType)-2] + ")"
}

func (c *Variant) ToJSON(row int, ignoreDoubleQuotes bool, b []byte) []byte {
	columnIndex, columnRow := c.RowPos(row)
	if columnRow == -1 {
		return append(b, "null"...)
	}
	return c.columns[columnIndex].ToJSON(columnRow, ignoreDoubleQuotes, b)
}

func (c *Variant) writeBinaryDataTo(w *readerwriter.Writer) {
	w.Uint8(uint8(helper.BinaryTypeIndexVariant))
	w.Uvarint(uint64(len(c.columns)))
	for _, col := range c.columns {
		col.writeBinaryDataTo(w)
	}
}
