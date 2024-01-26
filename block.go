package chconn

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/types"
)

// Column contains details of ClickHouse column
type chColumn struct {
	ChType []byte
	Name   []byte
}

type block struct {
	c            *conn
	Columns      []chColumn
	NumRows      uint64
	NumColumns   uint64
	info         blockInfo
	headerWriter *readerwriter.Writer
}

func newBlock(ch *conn) *block {
	return &block{
		c:            ch,
		headerWriter: readerwriter.NewWriter(),
	}
}

func (b *block) reset() {
	b.headerWriter.Reset()
	b.Columns = b.Columns[:0]
	b.NumRows = 0
	b.NumColumns = 0
}

func (b *block) read() error {
	if _, err := b.c.reader.ByteString(); err != nil { // temporary table
		return &readError{"block: temporary table", err}
	}
	b.c.reader.SetCompress(b.c.compress)
	defer b.c.reader.SetCompress(false)

	var err error
	err = b.info.read(b.c.reader)
	if err != nil {
		return err
	}

	b.NumColumns, err = b.c.reader.Uvarint()
	if err != nil {
		return &readError{"block: read NumColumns", err}
	}

	b.NumRows, err = b.c.reader.Uvarint()
	if err != nil {
		return &readError{"block: read NumRows", err}
	}
	return nil
}

func (b *block) readColumns() error {
	b.c.reader.SetCompress(b.c.compress)
	defer b.c.reader.SetCompress(false)
	b.Columns = make([]chColumn, b.NumColumns)

	for i := uint64(0); i < b.NumColumns; i++ {
		col, err := b.nextColumn()
		if err != nil {
			return err
		}
		b.Columns[i] = col
	}
	return nil
}

func (b *block) readColumnsData(needValidateData bool, columns ...column.ColumnBasic) error {
	b.c.reader.SetCompress(b.c.compress)
	defer b.c.reader.SetCompress(false)
	for _, col := range columns {
		err := col.HeaderReader(b.c.reader, true, b.c.serverInfo.Revision)
		if err != nil {
			return fmt.Errorf("read column header: %w", err)
		}
		if needValidateData {
			if errValidate := col.Validate(); errValidate != nil {
				return fmt.Errorf("validate %q: %w", col.Name(), errValidate)
			}
		}
		if b.NumRows == 0 {
			continue
		}
		err = col.ReadRaw(int(b.NumRows), b.c.reader)
		if err != nil {
			return fmt.Errorf("read data %q: %w", col.Name(), err)
		}
	}
	return nil
}

func (b *block) reorderColumns(columns []column.ColumnBasic) ([]column.ColumnBasic, error) {
	for i, c := range b.Columns {
		// check if already sorted
		if bytes.Equal(columns[i].Name(), b.Columns[i].Name) {
			continue
		}
		index, col := findColumn(columns, c.Name)
		if col == nil {
			return nil, &ColumnNotFoundError{
				Column: string(c.Name),
			}
		}
		columns[index] = columns[i]
		columns[i] = col
	}
	return columns, nil
}

func findColumn(columns []column.ColumnBasic, name []byte) (int, column.ColumnBasic) {
	for i, col := range columns {
		if bytes.Equal(col.Name(), name) {
			return i, col
		}
	}
	return 0, nil
}

func (b *block) nextColumn() (chColumn, error) {
	col := chColumn{}
	var err error
	if col.Name, err = b.c.reader.ByteString(); err != nil {
		return col, &readError{"block: read column name", err}
	}
	if col.ChType, err = b.c.reader.ByteString(); err != nil {
		return col, &readError{"block: read column type", err}
	}
	if b.c.serverInfo.Revision >= helper.DbmsMinProtocolWithCustomSerialization {
		customSerialization, err := b.c.reader.ReadByte()
		if err != nil {
			return col, &readError{"block: read custom serialization", err}
		}
		if customSerialization == 1 {
			return col, &readError{"block: custom serialization not supported", nil}
		}
	}
	return col, nil
}

func (b *block) writeHeader(numRows int) error {
	b.info.write(b.c.writer)
	// NumColumns
	b.c.writer.Uvarint(b.NumColumns)
	// NumRows
	b.c.writer.Uvarint(uint64(numRows))
	_, err := b.c.writer.WriteTo(b.c.writerToCompress)
	if err != nil {
		return &writeError{"write block info", err}
	}
	err = b.c.flushCompress()
	if err != nil {
		return &writeError{"flush block info", err}
	}

	return nil
}

func (b *block) writeColumnsBuffer(columns ...column.ColumnBasic) error {
	numRows := columns[0].NumRow()
	for i, column := range b.Columns {
		if numRows != columns[i].NumRow() {
			return &NumberWriteError{
				FirstNumRow: numRows,
				NumRow:      columns[i].NumRow(),
				Column:      string(column.Name),
				FirstColumn: string(b.Columns[0].Name),
			}
		}
		b.headerWriter.Reset()
		b.headerWriter.ByteString(column.Name)
		b.headerWriter.ByteString(column.ChType)

		if b.c.serverInfo.Revision >= helper.DbmsMinProtocolWithCustomSerialization {
			b.headerWriter.Uint8(0)
		}

		columns[i].HeaderWriter(b.headerWriter)
		if _, err := b.headerWriter.WriteTo(b.c.writerToCompress); err != nil {
			return &writeError{"block: write header block data for column " + string(column.Name), err}
		}
		if _, err := columns[i].WriteTo(b.c.writerToCompress); err != nil {
			return &writeError{"block: write block data for column " + string(column.Name), err}
		}
	}
	err := b.c.flushCompress()
	if err != nil {
		return &writeError{"block: flush block data", err}
	}
	return nil
}

func (b *block) getColumnsByChType() ([]column.ColumnBasic, error) {
	columns := make([]column.ColumnBasic, len(b.Columns))
	for i, col := range b.Columns {
		columnByType, err := b.columnByType(col.ChType, 0, false, false)
		if err != nil {
			return nil, err
		}
		columnByType.SetName(col.Name)
		columnByType.SetType(col.ChType)
		err = columnByType.Validate()
		if err != nil {
			return nil, err
		}
		columns[i] = columnByType
	}
	return columns, nil
}

func (b *block) skipBlock() error {
	err := b.readColumns()
	if err != nil {
		return err
	}
	columnsForRead, err := b.getColumnsByChType()
	if err != nil {
		return err
	}
	return b.readColumnsData(false, columnsForRead...)
}

type blockInfo struct {
	field1      uint64
	isOverflows uint8
	field2      uint64
	bucketNum   int32
	num3        uint64
}

func (info *blockInfo) read(r *readerwriter.Reader) error {
	var err error
	if info.field1, err = r.Uvarint(); err != nil {
		return &readError{"blockInfo: read field1", err}
	}
	if info.isOverflows, err = r.ReadByte(); err != nil {
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
	w.Uint8(info.isOverflows)
	w.Uvarint(2)

	if info.bucketNum == 0 {
		info.bucketNum = -1
	}
	w.Int32(info.bucketNum)
	w.Uvarint(0)
}

//nolint:funlen,gocyclo
func (b *block) columnByType(chType []byte, arrayLevel int, nullable, lc bool) (column.ColumnBasic, error) {
	switch {
	case string(chType) == "Int8" || helper.IsEnum8(chType):
		return column.New[int8]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "Int16" || helper.IsEnum16(chType):
		return column.New[int16]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "Int32":
		return column.New[int32]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "Int64":
		return column.New[int64]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "Int128":
		return column.New[types.Int128]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "Int256":
		return column.New[types.Int256]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "UInt8":
		return column.New[uint8]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "UInt16":
		return column.New[uint16]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "UInt32":
		return column.New[uint32]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "UInt64":
		return column.New[uint64]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "UInt128":
		return column.New[types.Uint128]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "UInt256":
		return column.New[types.Uint256]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "Float32":
		return column.New[float32]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "Float64":
		return column.New[float64]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "String":
		return column.NewString().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "Nothing":
		if lc {
			return nil, fmt.Errorf("LowCardinality is not allowed in nothing")
		}
		return column.NewNothing().Elem(arrayLevel, nullable), nil
	case helper.IsFixedString(chType):
		strLen, err := strconv.Atoi(string(chType[helper.FixedStringStrLen : len(chType)-1]))
		if err != nil {
			return nil, fmt.Errorf("invalid fixed string length: %s: %w", string(chType), err)
		}
		return getFixedType(strLen, arrayLevel, nullable, lc)
	case string(chType) == "Date":
		return column.NewDate[types.Date]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "Date32":
		return column.NewDate[types.Date32]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "DateTime" || helper.IsDateTimeWithParam(chType):
		var params [][]byte
		if bytes.HasPrefix(chType, []byte("DateTime(")) {
			params = bytes.Split(chType[len("DateTime("):len(chType)-1], []byte(", "))
		}
		col := column.NewDate[types.DateTime]()
		if len(params) > 0 && len(params[0]) >= 3 {
			if loc, err := time.LoadLocation(string(params[0][1 : len(params[0])-1])); err == nil {
				col.SetLocation(loc)
			} else if loc, err := time.LoadLocation(b.c.serverInfo.Timezone); err == nil {
				col.SetLocation(loc)
			}
		}
		return col.Elem(arrayLevel, nullable, lc), nil
	case helper.IsDateTime64(chType):
		params := bytes.Split(chType[helper.DateTime64StrLen:len(chType)-1], []byte(", "))
		if len(params) == 0 || len(params[0]) == 0 {
			return nil, fmt.Errorf("DateTime64 invalid params: precision is required: %s", string(chType))
		}
		precision, err := strconv.Atoi(string(params[0]))
		if err != nil {
			return nil, fmt.Errorf("DateTime64 invalid precision (%s): %w", string(chType), err)
		}
		col := column.NewDate[types.DateTime64]()
		col.SetPrecision(precision)
		if len(params) > 1 && len(params[1]) >= 3 {
			if loc, err := time.LoadLocation(string(params[1][1 : len(params[1])-1])); err == nil {
				col.SetLocation(loc)
			} else if loc, err := time.LoadLocation(b.c.serverInfo.Timezone); err == nil {
				col.SetLocation(loc)
			}
		}
		return col.Elem(arrayLevel, nullable, lc), nil

	case helper.IsDecimal(chType):
		params := bytes.Split(chType[helper.DecimalStrLen:len(chType)-1], []byte(", "))
		precision, _ := strconv.Atoi(string(params[0]))

		if precision <= 9 {
			return column.New[types.Decimal32]().Elem(arrayLevel, nullable, lc), nil
		}
		if precision <= 18 {
			return column.New[types.Decimal64]().Elem(arrayLevel, nullable, lc), nil
		}
		if precision <= 38 {
			return column.New[types.Decimal128]().Elem(arrayLevel, nullable, lc), nil
		}
		if precision <= 76 {
			return column.New[types.Decimal256]().Elem(arrayLevel, nullable, lc), nil
		}
		return nil, fmt.Errorf("max precision is 76 but got %d: %s", precision, string(chType))

	case string(chType) == "UUID":
		return column.New[types.UUID]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "IPv4":
		return column.New[types.IPv4]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "IPv6":
		return column.New[types.IPv6]().Elem(arrayLevel, nullable, lc), nil

	case helper.IsNullable(chType):
		return b.columnByType(chType[helper.LenNullableStr:len(chType)-1], arrayLevel, true, lc)

	case bytes.HasPrefix(chType, []byte("SimpleAggregateFunction(")):
		return b.columnByType(helper.FilterSimpleAggregate(chType), arrayLevel, nullable, lc)
	case helper.IsArray(chType):
		if arrayLevel == 3 {
			return nil, fmt.Errorf("max array level is 3")
		}
		if nullable {
			return nil, fmt.Errorf("array is not allowed in nullable")
		}
		if lc {
			return nil, fmt.Errorf("LowCardinality is not allowed in nullable")
		}
		return b.columnByType(chType[helper.LenArrayStr:len(chType)-1], arrayLevel+1, nullable, lc)
	case helper.IsLowCardinality(chType):
		return b.columnByType(chType[helper.LenLowCardinalityStr:len(chType)-1], arrayLevel, nullable, true)
	case helper.IsTuple(chType):
		columnsTuple, err := helper.TypesInParentheses(chType[helper.LenTupleStr : len(chType)-1])
		if err != nil {
			return nil, fmt.Errorf("tuple invalid types: %w", err)
		}
		columns := make([]column.ColumnBasic, len(columnsTuple))
		for i, c := range columnsTuple {
			col, err := b.columnByType(c.ChType, 0, false, false)
			if err != nil {
				return nil, err
			}
			col.SetName(c.Name)
			columns[i] = col
		}
		return column.NewTuple(columns...).Elem(arrayLevel), nil
	case helper.IsMap(chType):
		columnsMap, err := helper.TypesInParentheses(chType[helper.LenMapStr : len(chType)-1])
		if err != nil {
			return nil, fmt.Errorf("map invalid types: %w", err)
		}
		if len(columnsMap) != 2 {
			return nil, fmt.Errorf("map must have 2 columns")
		}
		columns := make([]column.ColumnBasic, len(columnsMap))
		for i, col := range columnsMap {
			col, err := b.columnByType(col.ChType, arrayLevel, nullable, lc)
			if err != nil {
				return nil, err
			}
			columns[i] = col
		}
		return column.NewMapBase(columns[0], columns[1]), nil
	case helper.IsNested(chType):
		return b.columnByType(helper.NestedToArrayType(chType), arrayLevel, nullable, lc)
	}
	return nil, fmt.Errorf("unknown type: %s", chType)
}

//nolint:funlen,gocyclo
func getFixedType(fixedLen, arrayLevel int, nullable, lc bool) (column.ColumnBasic, error) {
	switch fixedLen {
	case 1:
		return column.New[[1]byte]().Elem(arrayLevel, nullable, lc), nil
	case 2:
		return column.New[[2]byte]().Elem(arrayLevel, nullable, lc), nil
	case 3:
		return column.New[[3]byte]().Elem(arrayLevel, nullable, lc), nil
	case 4:
		return column.New[[4]byte]().Elem(arrayLevel, nullable, lc), nil
	case 5:
		return column.New[[5]byte]().Elem(arrayLevel, nullable, lc), nil
	case 6:
		return column.New[[6]byte]().Elem(arrayLevel, nullable, lc), nil
	case 7:
		return column.New[[7]byte]().Elem(arrayLevel, nullable, lc), nil
	case 8:
		return column.New[[8]byte]().Elem(arrayLevel, nullable, lc), nil
	case 9:
		return column.New[[9]byte]().Elem(arrayLevel, nullable, lc), nil
	case 10:
		return column.New[[10]byte]().Elem(arrayLevel, nullable, lc), nil
	case 11:
		return column.New[[11]byte]().Elem(arrayLevel, nullable, lc), nil
	case 12:
		return column.New[[12]byte]().Elem(arrayLevel, nullable, lc), nil
	case 13:
		return column.New[[13]byte]().Elem(arrayLevel, nullable, lc), nil
	case 14:
		return column.New[[14]byte]().Elem(arrayLevel, nullable, lc), nil
	case 15:
		return column.New[[15]byte]().Elem(arrayLevel, nullable, lc), nil
	case 16:
		return column.New[[16]byte]().Elem(arrayLevel, nullable, lc), nil
	case 17:
		return column.New[[17]byte]().Elem(arrayLevel, nullable, lc), nil
	case 18:
		return column.New[[18]byte]().Elem(arrayLevel, nullable, lc), nil
	case 19:
		return column.New[[19]byte]().Elem(arrayLevel, nullable, lc), nil
	case 20:
		return column.New[[20]byte]().Elem(arrayLevel, nullable, lc), nil
	case 21:
		return column.New[[21]byte]().Elem(arrayLevel, nullable, lc), nil
	case 22:
		return column.New[[22]byte]().Elem(arrayLevel, nullable, lc), nil
	case 23:
		return column.New[[23]byte]().Elem(arrayLevel, nullable, lc), nil
	case 24:
		return column.New[[24]byte]().Elem(arrayLevel, nullable, lc), nil
	case 25:
		return column.New[[25]byte]().Elem(arrayLevel, nullable, lc), nil
	case 26:
		return column.New[[26]byte]().Elem(arrayLevel, nullable, lc), nil
	case 27:
		return column.New[[27]byte]().Elem(arrayLevel, nullable, lc), nil
	case 28:
		return column.New[[28]byte]().Elem(arrayLevel, nullable, lc), nil
	case 29:
		return column.New[[29]byte]().Elem(arrayLevel, nullable, lc), nil
	case 30:
		return column.New[[30]byte]().Elem(arrayLevel, nullable, lc), nil
	case 31:
		return column.New[[31]byte]().Elem(arrayLevel, nullable, lc), nil
	case 32:
		return column.New[[32]byte]().Elem(arrayLevel, nullable, lc), nil
	case 33:
		return column.New[[33]byte]().Elem(arrayLevel, nullable, lc), nil
	case 34:
		return column.New[[34]byte]().Elem(arrayLevel, nullable, lc), nil
	case 35:
		return column.New[[35]byte]().Elem(arrayLevel, nullable, lc), nil
	case 36:
		return column.New[[36]byte]().Elem(arrayLevel, nullable, lc), nil
	case 37:
		return column.New[[37]byte]().Elem(arrayLevel, nullable, lc), nil
	case 38:
		return column.New[[38]byte]().Elem(arrayLevel, nullable, lc), nil
	case 39:
		return column.New[[39]byte]().Elem(arrayLevel, nullable, lc), nil
	case 40:
		return column.New[[40]byte]().Elem(arrayLevel, nullable, lc), nil
	case 41:
		return column.New[[41]byte]().Elem(arrayLevel, nullable, lc), nil
	case 42:
		return column.New[[42]byte]().Elem(arrayLevel, nullable, lc), nil
	case 43:
		return column.New[[43]byte]().Elem(arrayLevel, nullable, lc), nil
	case 44:
		return column.New[[44]byte]().Elem(arrayLevel, nullable, lc), nil
	case 45:
		return column.New[[45]byte]().Elem(arrayLevel, nullable, lc), nil
	case 46:
		return column.New[[46]byte]().Elem(arrayLevel, nullable, lc), nil
	case 47:
		return column.New[[47]byte]().Elem(arrayLevel, nullable, lc), nil
	case 48:
		return column.New[[48]byte]().Elem(arrayLevel, nullable, lc), nil
	case 49:
		return column.New[[49]byte]().Elem(arrayLevel, nullable, lc), nil
	case 50:
		return column.New[[50]byte]().Elem(arrayLevel, nullable, lc), nil
	case 51:
		return column.New[[51]byte]().Elem(arrayLevel, nullable, lc), nil
	case 52:
		return column.New[[52]byte]().Elem(arrayLevel, nullable, lc), nil
	case 53:
		return column.New[[53]byte]().Elem(arrayLevel, nullable, lc), nil
	case 54:
		return column.New[[54]byte]().Elem(arrayLevel, nullable, lc), nil
	case 55:
		return column.New[[55]byte]().Elem(arrayLevel, nullable, lc), nil
	case 56:
		return column.New[[56]byte]().Elem(arrayLevel, nullable, lc), nil
	case 57:
		return column.New[[57]byte]().Elem(arrayLevel, nullable, lc), nil
	case 58:
		return column.New[[58]byte]().Elem(arrayLevel, nullable, lc), nil
	case 59:
		return column.New[[59]byte]().Elem(arrayLevel, nullable, lc), nil
	case 60:
		return column.New[[60]byte]().Elem(arrayLevel, nullable, lc), nil
	case 61:
		return column.New[[61]byte]().Elem(arrayLevel, nullable, lc), nil
	case 62:
		return column.New[[62]byte]().Elem(arrayLevel, nullable, lc), nil
	case 63:
		return column.New[[63]byte]().Elem(arrayLevel, nullable, lc), nil
	case 64:
		return column.New[[64]byte]().Elem(arrayLevel, nullable, lc), nil
	case 65:
		return column.New[[65]byte]().Elem(arrayLevel, nullable, lc), nil
	case 66:
		return column.New[[66]byte]().Elem(arrayLevel, nullable, lc), nil
	case 67:
		return column.New[[67]byte]().Elem(arrayLevel, nullable, lc), nil
	case 68:
		return column.New[[68]byte]().Elem(arrayLevel, nullable, lc), nil
	case 69:
		return column.New[[69]byte]().Elem(arrayLevel, nullable, lc), nil
	case 70:
		return column.New[[70]byte]().Elem(arrayLevel, nullable, lc), nil
	}

	return nil, fmt.Errorf("fixed length %d is not supported", fixedLen)
}
