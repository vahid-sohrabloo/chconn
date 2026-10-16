package column

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
	"github.com/vahid-sohrabloo/chconn/v3/types"
)

// ColumnCore is the base interface that all column types implement.
// It provides low-level operations for reading, writing, and manipulating column data
// used by the ClickHouse native protocol.
type ColumnCore interface {
	ReadRaw(num int) error
	ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error
	HeaderWriter(*readerwriter.Writer)
	WriteTo(io.Writer) (int64, error)
	NumRow() int
	Reset()
	SetType(v []byte)
	SetColumnHeader(ColumnHeader) error
	Type() []byte
	SetName(v []byte)
	Name() []byte
	ValidateInsert() error
	structType() string
	chconnType() string
	SetWriteBufferSize(int)
	RowAny(int) any
	Scan(row int, dest any) error
	AppendAny(any) error
	canAppend(any) bool
	FullType() string
	Remove(n int)
	Delete(start, end int)
	DeleteFunc(del func(row int) bool)
	startBatchDelete()
	batchDeleteKeep(start, end int)
	endBatchDelete()
	ToJSON(row int, stringQuotes bool, b []byte) []byte
	setLocationInParent(locationInParent int)
	getLocationInParent() uint8
	setVariantParent(p *Variant)
	writeBinaryDataTo(w *readerwriter.Writer)
}

// Column is a generic interface for typed ClickHouse columns.
// It extends [ColumnCore] with type-safe data access methods for reading and appending values.
type Column[T any] interface {
	ColumnCore
	Data() []T
	Read([]T) []T
	Row(int) T
	Append(T)
	AppendMulti(...T)
}

// NullableColumn extends [Column] with nullable value support.
// It wraps values as pointers where nil represents a ClickHouse NULL.
type NullableColumn[T any] interface {
	Column[T]
	DataP() []*T
	ReadP([]*T) []*T
	RowP(int) *T
	AppendP(*T)
	AppendMultiP(...*T)
	RowIsNil(row int) bool
}

type column struct {
	r                *readerwriter.Reader
	columnHeader     ColumnHeader
	LocationInParent uint8
	variantParent    *Variant
	hasVariantParent bool
	appendErr        error
	itemsTotalSparse uint64
	sparseIndexes    []uint64
}

// ColumnHeader holds the metadata for a column as received from the ClickHouse server.
type ColumnHeader struct {
	ChType   []byte
	Name     []byte
	IsSparse bool
}

func (c *column) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	c.r = r
	return nil
}

// setReader points the column at the reader for the current query. Header-less
// internal columns (e.g. LowCardinality indices) are cached across blocks and
// reused, so they need their reader refreshed rather than a full ReadHeader.
func (c *column) setReader(r *readerwriter.Reader) {
	c.r = r
}

// Name get name of the column
func (c *column) Name() []byte {
	return c.columnHeader.Name
}

// Type get clickhouse type
func (c *column) Type() []byte {
	return c.columnHeader.ChType
}

// SetName set name of the column
func (c *column) SetName(v []byte) {
	c.columnHeader.Name = v
}

// SetType set clickhouse type
func (c *column) SetType(v []byte) {
	c.columnHeader.ChType = v
}

func (c *column) setVariantParent(p *Variant) {
	c.variantParent = p
	c.hasVariantParent = true
}

func (c *column) setLocationInParent(locationInParent int) {
	c.LocationInParent = uint8(locationInParent)
}

func (c *column) getLocationInParent() uint8 {
	return c.LocationInParent
}

func (c *column) preHookAppend() {
	if c.hasVariantParent {
		c.variantParent.AppendDiscriminators(c.LocationInParent)
	}
}

// TODO: find a more efficient way
func (c *column) preHookAppendMulti(n int) {
	if c.hasVariantParent {
		for range n {
			c.variantParent.AppendDiscriminators(c.LocationInParent)
		}
	}
}

func (c *column) AppendErr() error {
	return c.appendErr
}

func (c *column) readSparse() (int, error) {
	const EndOfGranuleFlag uint64 = 1 << 62

	c.sparseIndexes = c.sparseIndexes[:0]
	c.itemsTotalSparse = 0
	nonDefaultItems := 0
	endOfGranule := false

	for !endOfGranule {
		groupSize, err := c.r.Uvarint()
		if err != nil {
			return 0, err
		}
		endOfGranule = (groupSize & EndOfGranuleFlag) != 0
		groupSize &= ^EndOfGranuleFlag

		c.itemsTotalSparse += groupSize + 1
		if !endOfGranule {
			nonDefaultItems++
			c.sparseIndexes = append(c.sparseIndexes, c.itemsTotalSparse)
		}
	}
	return nonDefaultItems, nil
}

func type2Column(rtype reflect.Type, arrayLevel int, nullable bool) (ColumnCore, error) {
	switch rtype.Kind() {
	case reflect.Bool:
		return elemOfBase(New[bool](), arrayLevel, nullable, false), nil
	case reflect.Int8:
		return elemOfBase(New[int8](), arrayLevel, nullable, false), nil
	case reflect.Int16:
		return elemOfBase(New[int16](), arrayLevel, nullable, false), nil
	case reflect.Int32:
		return elemOfBase(New[int32](), arrayLevel, nullable, false), nil
	case reflect.Int64:
		return elemOfBase(New[int64](), arrayLevel, nullable, false), nil
	case reflect.Uint8:
		return elemOfBase(New[uint8](), arrayLevel, nullable, false), nil
	case reflect.Uint16:
		return elemOfBase(New[uint16](), arrayLevel, nullable, false), nil
	case reflect.Uint32:
		return elemOfBase(New[uint32](), arrayLevel, nullable, false), nil
	case reflect.Uint64:
		return elemOfBase(New[uint64](), arrayLevel, nullable, false), nil
	case reflect.Float32:
		return elemOfBase(New[float32](), arrayLevel, nullable, false), nil
	case reflect.Float64:
		return elemOfBase(New[float64](), arrayLevel, nullable, false), nil
	case reflect.String:
		return NewString().Elem(arrayLevel, nullable, false), nil
	case reflect.Slice:
		return type2Column(rtype.Elem(), arrayLevel+1, false)
	case reflect.Pointer:
		return type2Column(rtype.Elem(), arrayLevel, true)
	// TODO: support map and struct types
	default:
		return nil, fmt.Errorf("unsupported type: %s", rtype.Kind())
	}
}

// ColumnByType creates a [ColumnCore] from a ClickHouse type string.
// It handles all built-in types including Array, Nullable, LowCardinality, Map, Tuple, and Variant.
//
//nolint:funlen,gocyclo
func ColumnByType(chType []byte, arrayLevel int, nullable, lc bool, serverTimeZone string) (ColumnCore, error) {
	switch {
	case string(chType) == "Bool":
		c := elemOfBase(New[bool](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int8" || helper.IsEnum8(chType):
		c := elemOfBase(New[int8](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int16" || helper.IsEnum16(chType):
		c := elemOfBase(New[int16](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int32":
		c := elemOfBase(New[int32](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int64":
		c := elemOfBase(New[int64](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int128":
		c := elemOfBase(New[types.Int128](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int256":
		c := elemOfBase(New[types.Int256](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt8":
		c := elemOfBase(New[uint8](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt16":
		c := elemOfBase(New[uint16](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt32":
		c := elemOfBase(New[uint32](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt64":
		c := elemOfBase(New[uint64](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt128":
		c := elemOfBase(New[types.Uint128](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt256":
		c := elemOfBase(New[types.Uint256](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Float32":
		c := elemOfBase(New[float32](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Float64":
		c := elemOfBase(New[float64](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "String":
		c := NewString().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Nothing":
		if lc {
			return nil, fmt.Errorf("LowCardinality is not allowed in nothing")
		}
		c := NewNothing().Elem(arrayLevel, nullable)
		c.SetType(chType)
		return c, nil
	case helper.IsFixedString(chType):
		strLen, err := strconv.Atoi(string(chType[helper.FixedStringStrLen : len(chType)-1]))
		if err != nil {
			return nil, fmt.Errorf("invalid fixed string length: %s: %w", string(chType), err)
		}
		c, err := getFixedType(strLen, arrayLevel, nullable, lc)
		if err != nil {
			return nil, err
		}
		c.SetType(chType)
		return c, nil
	case string(chType) == "Date":
		c := elemOfDate(NewDate[types.Date](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Date32":
		c := elemOfDate(NewDate[types.Date32](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "DateTime" || helper.IsDateTimeWithParam(chType):
		var params [][]byte
		if bytes.HasPrefix(chType, []byte("DateTime(")) {
			params = bytes.Split(chType[len("DateTime("):len(chType)-1], []byte(", "))
		}
		col := NewDate[types.DateTime]()
		if len(params) > 0 && len(params[0]) >= 3 {
			if loc, err := time.LoadLocation(string(params[0][1 : len(params[0])-1])); err == nil {
				col.SetLocation(loc)
			} else if loc, err := time.LoadLocation(serverTimeZone); err == nil {
				col.SetLocation(loc)
			}
		} else if loc, err := time.LoadLocation(serverTimeZone); err == nil {
			col.SetLocation(loc)
		}
		col.SetType(chType)
		return elemOfDate(col, arrayLevel, nullable, lc), nil
	case helper.IsDateTime64(chType):
		params := bytes.Split(chType[helper.DateTime64StrLen:len(chType)-1], []byte(", "))
		if len(params) == 0 || len(params[0]) == 0 {
			return nil, fmt.Errorf("DateTime64 invalid params: precision is required: %s", string(chType))
		}
		precision, err := strconv.Atoi(string(params[0]))
		if err != nil {
			return nil, fmt.Errorf("DateTime64 invalid precision (%s): %w", string(chType), err)
		}
		col := NewDate[types.DateTime64]()
		col.SetPrecision(precision)
		if len(params) > 1 && len(params[1]) >= 3 {
			if loc, err := time.LoadLocation(string(params[1][1 : len(params[1])-1])); err == nil {
				col.SetLocation(loc)
			} else if loc, err := time.LoadLocation(serverTimeZone); err == nil {
				col.SetLocation(loc)
			}
		}
		col.SetType(chType)
		return elemOfDate(col, arrayLevel, nullable, lc), nil

	case helper.IsDecimal(chType):
		params := bytes.Split(chType[helper.DecimalStrLen:len(chType)-1], []byte(", "))
		precision, _ := strconv.Atoi(string(params[0]))

		if precision <= 9 {
			c := elemOfBase(New[types.Decimal32](), arrayLevel, nullable, lc)
			c.SetType(chType)
			return c, nil
		}
		if precision <= 18 {
			c := elemOfBase(New[types.Decimal64](), arrayLevel, nullable, lc)
			c.SetType(chType)
			return c, nil
		}
		if precision <= 38 {
			c := elemOfBase(New[types.Decimal128](), arrayLevel, nullable, lc)
			c.SetType(chType)
			return c, nil
		}
		if precision <= 76 {
			c := elemOfBase(New[types.Decimal256](), arrayLevel, nullable, lc)
			c.SetType(chType)
			return c, nil
		}
		return nil, fmt.Errorf("max precision is 76 but got %d: %s", precision, string(chType))

	case string(chType) == "UUID":
		c := elemOfBase(New[types.UUID](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "IPv4":
		c := elemOfBase(New[types.IPv4](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "IPv6":
		c := elemOfBase(New[types.IPv6](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "BFloat16":
		c := elemOfBase(New[types.BFloat16](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Time":
		c := elemOfBase(New[types.ChTime](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case helper.IsTime64(chType):
		c := elemOfBase(New[types.ChTime64](), arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Point":
		var c ColumnCore = NewPoint()
		if arrayLevel > 0 {
			c = NewPoint().Array()
		}
		c.SetType(chType)
		return c, nil
	case string(chType) == "Ring":
		c := NewPoint().Array()
		if arrayLevel > 0 {
			c2 := c.Array()
			c2.SetType(chType)
			return c2, nil
		}
		c.SetType(chType)
		return c, nil
	case string(chType) == "Polygon":
		c := NewPoint().Array().Array()
		if arrayLevel > 0 {
			c2 := c.Array()
			c2.SetType(chType)
			return c2, nil
		}
		c.SetType(chType)
		return c, nil
	case string(chType) == "MultiPolygon":
		c := NewPoint().Array().Array().Array()
		c.SetType(chType)
		return c, nil

	case helper.IsNullable(chType):
		c, err := ColumnByType(chType[helper.LenNullableStr:len(chType)-1], arrayLevel, true, lc, serverTimeZone)
		if err != nil {
			return nil, fmt.Errorf("nullable invalid type: %w", err)
		}
		c.SetType(chType)
		return c, nil
	case bytes.HasPrefix(chType, []byte("SimpleAggregateFunction(")):
		c, err := ColumnByType(helper.FilterSimpleAggregate(chType), arrayLevel, nullable, lc, serverTimeZone)
		if err != nil {
			return nil, fmt.Errorf("simple aggregate function invalid type: %w", err)
		}
		c.SetType(chType)
		return c, nil
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

		c, err := ColumnByType(chType[helper.LenArrayStr:len(chType)-1], arrayLevel+1, nullable, lc, serverTimeZone)
		if err != nil {
			return nil, fmt.Errorf("array invalid type: %w", err)
		}
		c.SetType(chType)
		return c, nil
	case helper.IsLowCardinality(chType):
		c, err := ColumnByType(chType[helper.LenLowCardinalityStr:len(chType)-1], arrayLevel, nullable, true, serverTimeZone)
		if err != nil {
			return nil, fmt.Errorf("low cardinality invalid type: %w", err)
		}
		c.SetType(chType)
		return c, nil
	case helper.IsTuple(chType):
		columnsTuple, err := helper.TypesInParentheses(chType[helper.LenTupleStr : len(chType)-1])
		if err != nil {
			return nil, fmt.Errorf("tuple invalid types: %w", err)
		}
		columns := make([]ColumnCore, len(columnsTuple))
		for i, c := range columnsTuple {
			col, err := ColumnByType(c.ChType, 0, false, false, serverTimeZone)
			if err != nil {
				return nil, err
			}
			col.SetType(c.ChType)
			col.SetName(c.Name)
			columns[i] = col
		}

		if len(columns) == 0 {
			// for empty tuple clickhouse is using Nothing type internally
			columns = append(columns, NewNothing())
		}

		c := NewTuple(columns...).Elem(arrayLevel)
		c.SetType(chType)
		return c, nil
	case helper.IsVariant(chType):
		columnsVariant, err := helper.TypesInParentheses(chType[helper.LenVariantStr : len(chType)-1])
		if err != nil {
			return nil, fmt.Errorf("variant invalid types: %w", err)
		}
		columns := make([]ColumnCore, len(columnsVariant))
		for i, c := range columnsVariant {
			col, err := ColumnByType(c.ChType, 0, false, false, serverTimeZone)
			if err != nil {
				return nil, err
			}
			col.SetName(c.Name)
			col.SetType(c.ChType)
			columns[i] = col
		}

		c := NewVariant(columns...).Elem(arrayLevel)
		c.SetType(chType)
		return c, nil

	case helper.IsDynamic(chType):
		c := NewDynamic().Elem(arrayLevel)
		c.SetType(chType)
		return c, nil
	case helper.IsJSON(chType):
		if nullable {
			return nil, fmt.Errorf("nullable JSON is not supported yet")
		}
		if arrayLevel > 0 {
			return nil, fmt.Errorf("array of JSON is not supported yet")
		}
		c := NewJSON()
		c.SetType(chType)
		return c, nil
	case helper.IsMap(chType):
		columnsMap, err := helper.TypesInParentheses(chType[helper.LenMapStr : len(chType)-1])
		if err != nil {
			return nil, fmt.Errorf("map invalid types: %w", err)
		}
		if len(columnsMap) != 2 {
			return nil, fmt.Errorf("map must have 2 columns")
		}
		columns := make([]ColumnCore, len(columnsMap))
		for i, col := range columnsMap {
			c, err := ColumnByType(col.ChType, arrayLevel, nullable, lc, serverTimeZone)
			if err != nil {
				return nil, err
			}
			c.SetType(col.ChType)
			columns[i] = c
		}
		c := NewMapBase(columns[0], columns[1])
		c.SetType(chType)
		return c, nil
	case helper.IsNested(chType):
		c, err := ColumnByType(helper.NestedToArrayType(chType), arrayLevel, nullable, lc, serverTimeZone)
		if err != nil {
			return nil, fmt.Errorf("nested invalid type: %w", err)
		}
		c.SetType(chType)
		return c, nil
	}
	return nil, fmt.Errorf("unknown type: %s", chType)
}

//nolint:funlen,gocyclo
func getFixedType(fixedLen, arrayLevel int, nullable, lc bool) (ColumnCore, error) {
	switch fixedLen {
	case 1:
		return elemOfBase(New[[1]byte](), arrayLevel, nullable, lc), nil
	case 2:
		return elemOfBase(New[[2]byte](), arrayLevel, nullable, lc), nil
	case 3:
		return elemOfBase(New[[3]byte](), arrayLevel, nullable, lc), nil
	case 4:
		return elemOfBase(New[[4]byte](), arrayLevel, nullable, lc), nil
	case 5:
		return elemOfBase(New[[5]byte](), arrayLevel, nullable, lc), nil
	case 6:
		return elemOfBase(New[[6]byte](), arrayLevel, nullable, lc), nil
	case 7:
		return elemOfBase(New[[7]byte](), arrayLevel, nullable, lc), nil
	case 8:
		return elemOfBase(New[[8]byte](), arrayLevel, nullable, lc), nil
	case 9:
		return elemOfBase(New[[9]byte](), arrayLevel, nullable, lc), nil
	case 10:
		return elemOfBase(New[[10]byte](), arrayLevel, nullable, lc), nil
	case 11:
		return elemOfBase(New[[11]byte](), arrayLevel, nullable, lc), nil
	case 12:
		return elemOfBase(New[[12]byte](), arrayLevel, nullable, lc), nil
	case 13:
		return elemOfBase(New[[13]byte](), arrayLevel, nullable, lc), nil
	case 14:
		return elemOfBase(New[[14]byte](), arrayLevel, nullable, lc), nil
	case 15:
		return elemOfBase(New[[15]byte](), arrayLevel, nullable, lc), nil
	case 16:
		return elemOfBase(New[[16]byte](), arrayLevel, nullable, lc), nil
	case 17:
		return elemOfBase(New[[17]byte](), arrayLevel, nullable, lc), nil
	case 18:
		return elemOfBase(New[[18]byte](), arrayLevel, nullable, lc), nil
	case 19:
		return elemOfBase(New[[19]byte](), arrayLevel, nullable, lc), nil
	case 20:
		return elemOfBase(New[[20]byte](), arrayLevel, nullable, lc), nil
	case 21:
		return elemOfBase(New[[21]byte](), arrayLevel, nullable, lc), nil
	case 22:
		return elemOfBase(New[[22]byte](), arrayLevel, nullable, lc), nil
	case 23:
		return elemOfBase(New[[23]byte](), arrayLevel, nullable, lc), nil
	case 24:
		return elemOfBase(New[[24]byte](), arrayLevel, nullable, lc), nil
	case 25:
		return elemOfBase(New[[25]byte](), arrayLevel, nullable, lc), nil
	case 26:
		return elemOfBase(New[[26]byte](), arrayLevel, nullable, lc), nil
	case 27:
		return elemOfBase(New[[27]byte](), arrayLevel, nullable, lc), nil
	case 28:
		return elemOfBase(New[[28]byte](), arrayLevel, nullable, lc), nil
	case 29:
		return elemOfBase(New[[29]byte](), arrayLevel, nullable, lc), nil
	case 30:
		return elemOfBase(New[[30]byte](), arrayLevel, nullable, lc), nil
	case 31:
		return elemOfBase(New[[31]byte](), arrayLevel, nullable, lc), nil
	case 32:
		return elemOfBase(New[[32]byte](), arrayLevel, nullable, lc), nil
	case 33:
		return elemOfBase(New[[33]byte](), arrayLevel, nullable, lc), nil
	case 34:
		return elemOfBase(New[[34]byte](), arrayLevel, nullable, lc), nil
	case 35:
		return elemOfBase(New[[35]byte](), arrayLevel, nullable, lc), nil
	case 36:
		return elemOfBase(New[[36]byte](), arrayLevel, nullable, lc), nil
	case 37:
		return elemOfBase(New[[37]byte](), arrayLevel, nullable, lc), nil
	case 38:
		return elemOfBase(New[[38]byte](), arrayLevel, nullable, lc), nil
	case 39:
		return elemOfBase(New[[39]byte](), arrayLevel, nullable, lc), nil
	case 40:
		return elemOfBase(New[[40]byte](), arrayLevel, nullable, lc), nil
	case 41:
		return elemOfBase(New[[41]byte](), arrayLevel, nullable, lc), nil
	case 42:
		return elemOfBase(New[[42]byte](), arrayLevel, nullable, lc), nil
	case 43:
		return elemOfBase(New[[43]byte](), arrayLevel, nullable, lc), nil
	case 44:
		return elemOfBase(New[[44]byte](), arrayLevel, nullable, lc), nil
	case 45:
		return elemOfBase(New[[45]byte](), arrayLevel, nullable, lc), nil
	case 46:
		return elemOfBase(New[[46]byte](), arrayLevel, nullable, lc), nil
	case 47:
		return elemOfBase(New[[47]byte](), arrayLevel, nullable, lc), nil
	case 48:
		return elemOfBase(New[[48]byte](), arrayLevel, nullable, lc), nil
	case 49:
		return elemOfBase(New[[49]byte](), arrayLevel, nullable, lc), nil
	case 50:
		return elemOfBase(New[[50]byte](), arrayLevel, nullable, lc), nil
	case 51:
		return elemOfBase(New[[51]byte](), arrayLevel, nullable, lc), nil
	case 52:
		return elemOfBase(New[[52]byte](), arrayLevel, nullable, lc), nil
	case 53:
		return elemOfBase(New[[53]byte](), arrayLevel, nullable, lc), nil
	case 54:
		return elemOfBase(New[[54]byte](), arrayLevel, nullable, lc), nil
	case 55:
		return elemOfBase(New[[55]byte](), arrayLevel, nullable, lc), nil
	case 56:
		return elemOfBase(New[[56]byte](), arrayLevel, nullable, lc), nil
	case 57:
		return elemOfBase(New[[57]byte](), arrayLevel, nullable, lc), nil
	case 58:
		return elemOfBase(New[[58]byte](), arrayLevel, nullable, lc), nil
	case 59:
		return elemOfBase(New[[59]byte](), arrayLevel, nullable, lc), nil
	case 60:
		return elemOfBase(New[[60]byte](), arrayLevel, nullable, lc), nil
	case 61:
		return elemOfBase(New[[61]byte](), arrayLevel, nullable, lc), nil
	case 62:
		return elemOfBase(New[[62]byte](), arrayLevel, nullable, lc), nil
	case 63:
		return elemOfBase(New[[63]byte](), arrayLevel, nullable, lc), nil
	case 64:
		return elemOfBase(New[[64]byte](), arrayLevel, nullable, lc), nil
	case 65:
		return elemOfBase(New[[65]byte](), arrayLevel, nullable, lc), nil
	case 66:
		return elemOfBase(New[[66]byte](), arrayLevel, nullable, lc), nil
	case 67:
		return elemOfBase(New[[67]byte](), arrayLevel, nullable, lc), nil
	case 68:
		return elemOfBase(New[[68]byte](), arrayLevel, nullable, lc), nil
	case 69:
		return elemOfBase(New[[69]byte](), arrayLevel, nullable, lc), nil
	case 70:
		return elemOfBase(New[[70]byte](), arrayLevel, nullable, lc), nil
	}

	return nil, fmt.Errorf("fixed length %d is not supported", fixedLen)
}
