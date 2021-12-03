package chconn

import (
	"strconv"
	"strings"

	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

// Column contains details of ClickHouse column with Buffer index in inserting
type Column struct {
	ChType                   string
	Name                     string
	BufferIndex              int
	NumBuffer                int
	HasVersion               bool
	isLowCardinalityNullable bool
	fixedStringSize          int
}

type block struct {
	Columns      []*Column
	NumRows      uint64
	NumColumns   uint64
	NumBuffer    uint64
	info         blockInfo
	headerWriter *readerwriter.Writer
}

func newBlock() *block {
	return &block{
		headerWriter: readerwriter.NewWriter(),
	}
}

func (block *block) read(ch *conn) error {
	if _, err := ch.reader.String(); err != nil { // temporary table
		return err
	}

	ch.reader.SetCompress(ch.compress)
	defer ch.reader.SetCompress(false)
	var err error
	err = block.info.read(ch.reader)
	if err != nil {
		return err
	}

	block.NumColumns, err = ch.reader.Uvarint()
	if err != nil {
		return &readError{"block: read NumColumns", err}
	}

	block.NumRows, err = ch.reader.Uvarint()
	if err != nil {
		return &readError{"block: read NumRows", err}
	}
	return nil
}

func (block *block) initForInsert(ch *conn) error {
	ch.reader.SetCompress(ch.compress)
	defer ch.reader.SetCompress(false)
	block.Columns = make([]*Column, block.NumColumns)
	for i := uint64(0); i < block.NumColumns; i++ {
		column, err := block.nextColumn(ch)
		if err != nil {
			return err
		}

		column.BufferIndex = int(block.NumBuffer)
		block.calcBuffer(column.ChType, column)
		block.NumBuffer += uint64(column.NumBuffer)
		block.Columns[i] = column
	}

	return nil
}

func (block *block) readColumns(ch *conn) error {
	block.Columns = make([]*Column, block.NumColumns)

	for i := uint64(0); i < block.NumColumns; i++ {
		column, err := block.nextColumn(ch)
		if err != nil {
			return err
		}
		block.Columns[i] = column
	}
	return nil
}

func (block *block) nextColumn(ch *conn) (*Column, error) {
	column := &Column{}
	var err error
	if column.Name, err = ch.reader.String(); err != nil {
		return nil, &readError{"block: read column name", err}
	}
	if column.ChType, err = ch.reader.String(); err != nil {
		return column, &readError{"block: read column type", err}
	}

	if strings.Contains(column.ChType, "LowCardinality(") {
		column.HasVersion = true
	}

	return column, nil
}

var preCachedNeedBuffer = map[string]int{
	"Int8":               1,
	"Int16":              1,
	"Int32":              1,
	"Int64":              1,
	"UInt8":              1,
	"UInt16":             1,
	"UInt32":             1,
	"UInt64":             1,
	"Float32":            1,
	"Float64":            1,
	"String":             1,
	"Date":               1,
	"DateTime":           1,
	"UUID":               1,
	"IPv4":               1,
	"IPv6":               1,
	"Array(Int8)":        2,
	"Array(Int16)":       2,
	"Array(Int32)":       2,
	"Array(Int64)":       2,
	"Array(UInt8)":       2,
	"Array(UInt16)":      2,
	"Array(UInt32)":      2,
	"Array(UInt64)":      2,
	"Array(Float32)":     2,
	"Array(Float64)":     2,
	"Array(String)":      2,
	"Array(Date)":        2,
	"Array(DateTime)":    2,
	"Array(UUID)":        2,
	"Array(IPv4)":        2,
	"Array(IPv6)":        2,
	"Nullable(Int8)":     2,
	"Nullable(Int16)":    2,
	"Nullable(Int32)":    2,
	"Nullable(Int64)":    2,
	"Nullable(UInt8)":    2,
	"Nullable(UInt16)":   2,
	"Nullable(UInt32)":   2,
	"Nullable(UInt64)":   2,
	"Nullable(Float32)":  2,
	"Nullable(Float64)":  2,
	"Nullable(String)":   2,
	"Nullable(Date)":     2,
	"Nullable(DateTime)": 2,
	"Nullable(UUID)":     2,
	"Nullable(IPv4)":     2,
	"Nullable(IPv6)":     2,
}

//nolint:gocyclo
func (block *block) calcBuffer(chType string, column *Column) {
	if numBuffer, ok := preCachedNeedBuffer[chType]; ok {
		column.NumBuffer += numBuffer
		return
	}

	if strings.HasPrefix(chType, "Decimal(") ||
		strings.HasPrefix(chType, "DateTime(") ||
		strings.HasPrefix(chType, "DateTime64(") ||
		strings.HasPrefix(chType, "Enum8(") ||
		strings.HasPrefix(chType, "Enum16(") {
		column.NumBuffer++
		return
	}

	if strings.HasPrefix(chType, "FixedString(") {
		column.NumBuffer++
		column.fixedStringSize, _ = strconv.Atoi(chType[len("FixedString(") : len(chType)-1])
		return
	}

	if strings.HasPrefix(chType, "LowCardinality(Nullable(") {
		column.HasVersion = true
		column.isLowCardinalityNullable = true
		// get chtype between `LowCardinality(Nullable` and `))`
		block.calcBuffer(chType[24:len(chType)-2], column)
		return
	}

	if strings.HasPrefix(chType, "LowCardinality(") {
		column.HasVersion = true
		// get chtype between `LowCardinality(` and `)`
		block.calcBuffer(chType[15:len(chType)-1], column)
		return
	}

	if strings.HasPrefix(chType, "Array(") {
		column.NumBuffer++
		// get chtype between `Array(` and `)`
		block.calcBuffer(chType[6:len(chType)-1], column)
		return
	}
	if strings.HasPrefix(chType, "Nullable(") {
		column.NumBuffer++
		// get chtype between `Nullable(` and `)`
		block.calcBuffer(chType[9:len(chType)-1], column)
		return
	}

	if strings.HasPrefix(chType, "Tuple(") {
		var openFunc int
		cur := 0
		// for between `Tuple(` and `)`
		tupleTypes := chType[6 : len(chType)-1]
		for i, char := range tupleTypes {
			if char == ',' {
				if openFunc == 0 {
					block.calcBuffer(tupleTypes[cur:i], column)
					cur = i + 2
				}
				continue
			}
			if char == '(' {
				openFunc++
				continue
			}
			if char == ')' {
				openFunc--
				continue
			}
		}
		block.calcBuffer(tupleTypes[cur:], column)
		return
	}

	if strings.HasPrefix(chType, "SimpleAggregateFunction(") {
		block.calcBuffer(getNestedType(chType[24:]), column)
		return
	}

	panic("NOT Supported " + chType)
}

func getNestedType(chType string) string {
	for i, v := range chType {
		if v == ',' {
			return chType[i+2 : len(chType)-1]
		}
	}
	panic("Cannot found  netsted type of " + chType)
}

func (block *block) writeHeader(ch *conn, numRows uint64) error {
	block.info.write(ch.writer)
	// NumColumns
	ch.writer.Uvarint(block.NumColumns)
	// NumRows
	ch.writer.Uvarint(numRows)
	_, err := ch.writer.WriteTo(ch.writertoCompress)
	if err != nil {
		return &writeError{"block: write block info", err}
	}
	err = ch.flushCompress()
	if err != nil {
		return &writeError{"block: flush block info", err}
	}

	return nil
}

func (block *block) writeColumsBuffer(ch *conn, writer *InsertWriter) error {
	// todo check if the write buffer is enough
	var bufferIndex int
	for _, column := range block.Columns {
		block.headerWriter.Reset()
		block.headerWriter.String(column.Name)
		block.headerWriter.String(column.ChType)
		if column.HasVersion {
			block.headerWriter.Int64(1)
		}
		if _, err := block.headerWriter.WriteTo(ch.writertoCompress); err != nil {
			return &writeError{"block: write header block data for column " + column.Name, err}
		}
		for i := 0; i < column.NumBuffer; i++ {
			if column.isLowCardinalityNullable {
				writer.ColumnsBuffer[bufferIndex].SetStringLowCardinalityNull(column.fixedStringSize)
			}
			if _, err := ch.writertoCompress.Write(writer.ColumnsBuffer[bufferIndex].Bytes()); err != nil {
				return &writeError{"block: write block data for column " + column.Name, err}
			}
			bufferIndex++
		}
	}
	err := ch.flushCompress()
	if err != nil {
		return &writeError{"block: flush block data", err}
	}
	return nil
}

type blockInfo struct {
	field1      uint64
	isOverflows bool
	field2      uint64
	bucketNum   int32
	num3        uint64
}

func (info *blockInfo) read(r *readerwriter.Reader) error {
	var err error
	if info.field1, err = r.Uvarint(); err != nil {
		return &readError{"blockInfo: read field1", err}
	}
	if info.isOverflows, err = r.Bool(); err != nil {
		return &readError{"blockInfo: read isOverflows", err}
	}
	if info.field2, err = r.Uvarint(); err != nil {
		return &readError{"blockInfo: read field2", err}
	}
	if info.bucketNum, err = r.Int32(); err != nil {
		return &readError{"blockInfo: read bucketNum", err}
	}
	if info.num3, err = r.Uvarint(); err != nil {
		return &readError{"blockInfo: read num3", err}
	}
	return nil
}

func (info *blockInfo) write(w *readerwriter.Writer) {
	w.Uvarint(1)
	w.Bool(info.isOverflows)
	w.Uvarint(2)

	if info.bucketNum == 0 {
		info.bucketNum = -1
	}
	w.Int32(info.bucketNum)
	w.Uvarint(0)
}
