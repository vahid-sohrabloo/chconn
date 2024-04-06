package column

import (
	"fmt"
	"io"
	"sort"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
)

// Variant is a column of Variant(T1,T2,.....,Tn) ClickHouse data type
type Variant struct {
	column
	discriminators         *Base[uint8]
	discriminatorsIndexPos []int
	columns                []ColumnBasic
	totalNils              int
}

// NewVariant create a new Variant of Variant(T1,T2,.....,Tn) ClickHouse data type
func NewVariant(columns ...ColumnBasic) *Variant {
	if len(columns) < 1 {
		panic("Variant must have at least one column")
	}
	sort.Slice(columns, func(i, j int) bool { return columns[i].FullType() < columns[j].FullType() })
	for i, col := range columns {
		col.setLocationInParent(i)
	}
	v := &Variant{
		columns:        columns,
		discriminators: New[uint8](),
	}
	for _, col := range columns {
		col.setVariantParent(v)
	}
	return v
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
func (c *Variant) ReadRaw(num int, r *readerwriter.Reader) error {
	err := c.discriminators.ReadRaw(num, r)
	if err != nil {
		return fmt.Errorf("Variant: read discriminators column: %w", err)
	}
	if cap(c.discriminatorsIndexPos) < num {
		c.discriminatorsIndexPos = make([]int, num)
	} else {
		c.discriminatorsIndexPos = c.discriminatorsIndexPos[:num]
	}
	var dataLen [256]int
	for i, n := range c.discriminators.b {
		c.discriminatorsIndexPos[i] = dataLen[n]
		dataLen[n]++
	}
	for i, col := range c.columns {
		err := col.ReadRaw(dataLen[i], r)
		if err != nil {
			return fmt.Errorf("Variant: read column index %d: %w", i, err)
		}
	}
	return nil
}

// HeaderReader reads header data from reader.
// it uses internally
func (c *Variant) HeaderReader(r *readerwriter.Reader, readColumn bool, revision uint64) error {
	c.r = r
	err := c.readColumn(readColumn, revision)
	if err != nil {
		return err
	}

	for i, col := range c.columns {
		err = col.HeaderReader(r, false, revision)
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
	// todo grow cap as needed
	for i := 0; i < c.NumRow(); i++ {
		value = append(value, c.Row(i))
	}
	return value
}

// Row return the value of given row
func (c *Variant) Row(row int) any {
	index := c.discriminators.Row(row)
	if index == 255 {
		return nil
	}

	return c.columns[index].RowAny(c.discriminatorsIndexPos[row])
}

// RowAny return the value of given row as any.
func (c *Variant) RowAny(row int) any {
	return c.Row(row)
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
func (c *Variant) Columns() []ColumnBasic {
	return c.columns
}

// Validate is validate the column  for insert and select.
// it uses internally
func (c *Variant) Validate(forInsert bool) error {
	if !helper.IsVariant(c.chType) {
		return &ErrInvalidType{
			chType:     string(c.chType),
			chconnType: c.chconnType(),
			goToChType: c.structType(),
		}
	}
	if forInsert {
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
	}
	chType := helper.FilterSimpleAggregate(c.chType)
	columnsVariant, err := helper.TypesInParentheses(chType[helper.LenVariantStr : len(chType)-1])
	if err != nil {
		return fmt.Errorf("Variant invalid types %w", err)
	}
	if len(columnsVariant) != len(c.columns) {
		//nolint:goerr113
		return fmt.Errorf("columns number for %s (%s) is not equal to Variant columns number: %d != %d",
			string(c.name),
			string(c.Type()),
			len(columnsVariant),
			len(c.columns),
		)
	}

	for i, col := range c.columns {
		col.SetType(columnsVariant[i].ChType)
		col.SetName(columnsVariant[i].Name)
		if err := col.Validate(forInsert); err != nil {
			if !isInvalidType(err) {
				return err
			}
			return &ErrInvalidType{
				chType:     string(c.chType),
				chconnType: c.chconnType(),
				goToChType: c.structType(),
			}
		}
	}
	return nil
}

func (c *Variant) chconnType() string {
	chConn := "column.Variant("
	for _, col := range c.columns {
		chConn += col.chconnType() + ", "
	}
	return chConn[:len(chConn)-2] + ")"
}

func (c *Variant) structType() string {
	str := helper.VariantStr
	for _, col := range c.columns {
		str += col.structType() + ", "
	}
	return str[:len(str)-2] + ")"
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
	for _, col := range c.columns {
		col.HeaderWriter(w)
	}
}

func (c *Variant) Elem(arrayLevel int) ColumnBasic {
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

func (c *Variant) FullType() string {
	var chType string
	if len(c.name) == 0 {
		chType = "Variant("
	} else {
		chType = string(c.name) + " Variant("
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
