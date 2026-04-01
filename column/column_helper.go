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

type Column[T any] interface {
	ColumnCore
	Data() []T
	Read([]T) []T
	Row(int) T
	Append(T)
	AppendMulti(...T)
}

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

type ColumnHeader struct {
	ChType   []byte
	Name     []byte
	IsSparse bool
}

func (c *column) ReadHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) error {
	c.r = r
	return nil
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

// todo find a more efficient way
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
		return New[bool]().Elem(arrayLevel, nullable, false), nil
	case reflect.Int8:
		return New[int8]().Elem(arrayLevel, nullable, false), nil
	case reflect.Int16:
		return New[int16]().Elem(arrayLevel, nullable, false), nil
	case reflect.Int32:
		return New[int32]().Elem(arrayLevel, nullable, false), nil
	case reflect.Int64:
		return New[int64]().Elem(arrayLevel, nullable, false), nil
	case reflect.Uint8:
		return New[uint8]().Elem(arrayLevel, nullable, false), nil
	case reflect.Uint16:
		return New[uint16]().Elem(arrayLevel, nullable, false), nil
	case reflect.Uint32:
		return New[uint32]().Elem(arrayLevel, nullable, false), nil
	case reflect.Uint64:
		return New[uint64]().Elem(arrayLevel, nullable, false), nil
	case reflect.Float32:
		return New[float32]().Elem(arrayLevel, nullable, false), nil
	case reflect.Float64:
		return New[float64]().Elem(arrayLevel, nullable, false), nil
	case reflect.String:
		return NewString().Elem(arrayLevel, nullable, false), nil
	case reflect.Slice:
		return type2Column(rtype.Elem(), arrayLevel+1, false)
	case reflect.Ptr:
		return type2Column(rtype.Elem(), arrayLevel, true)
	// todo for other types like map struct ....
	default:
		// todo return error
		return nil, fmt.Errorf("unsupported type: %s", rtype.Kind())
	}
}

//nolint:funlen,gocyclo
func ColumnByType(chType []byte, arrayLevel int, nullable, lc bool, serverTimeZone string) (ColumnCore, error) {
	switch {
	case string(chType) == "Bool":
		c := New[bool]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int8" || helper.IsEnum8(chType):
		c := New[int8]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int16" || helper.IsEnum16(chType):
		c := New[int16]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int32":
		c := New[int32]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int64":
		c := New[int64]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int128":
		c := New[types.Int128]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Int256":
		c := New[types.Int256]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt8":
		c := New[uint8]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt16":
		c := New[uint16]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt32":
		c := New[uint32]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt64":
		c := New[uint64]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt128":
		c := New[types.Uint128]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "UInt256":
		c := New[types.Uint256]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Float32":
		c := New[float32]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Float64":
		c := New[float64]().Elem(arrayLevel, nullable, lc)
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
		c := NewDate[types.Date]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "Date32":
		c := NewDate[types.Date32]().Elem(arrayLevel, nullable, lc)
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
		return col.Elem(arrayLevel, nullable, lc), nil

	case helper.IsDecimal(chType):
		params := bytes.Split(chType[helper.DecimalStrLen:len(chType)-1], []byte(", "))
		precision, _ := strconv.Atoi(string(params[0]))

		if precision <= 9 {
			c := New[types.Decimal32]().Elem(arrayLevel, nullable, lc)
			c.SetType(chType)
			return c, nil
		}
		if precision <= 18 {
			c := New[types.Decimal64]().Elem(arrayLevel, nullable, lc)
			c.SetType(chType)
			return c, nil
		}
		if precision <= 38 {
			c := New[types.Decimal128]().Elem(arrayLevel, nullable, lc)
			c.SetType(chType)
			return c, nil
		}
		if precision <= 76 {
			c := New[types.Decimal256]().Elem(arrayLevel, nullable, lc)
			c.SetType(chType)
			return c, nil
		}
		return nil, fmt.Errorf("max precision is 76 but got %d: %s", precision, string(chType))

	case string(chType) == "UUID":
		c := New[types.UUID]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "IPv4":
		c := New[types.IPv4]().Elem(arrayLevel, nullable, lc)
		c.SetType(chType)
		return c, nil
	case string(chType) == "IPv6":
		c := New[types.IPv6]().Elem(arrayLevel, nullable, lc)
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
		return New[[1]byte]().Elem(arrayLevel, nullable, lc), nil
	case 2:
		return New[[2]byte]().Elem(arrayLevel, nullable, lc), nil
	case 3:
		return New[[3]byte]().Elem(arrayLevel, nullable, lc), nil
	case 4:
		return New[[4]byte]().Elem(arrayLevel, nullable, lc), nil
	case 5:
		return New[[5]byte]().Elem(arrayLevel, nullable, lc), nil
	case 6:
		return New[[6]byte]().Elem(arrayLevel, nullable, lc), nil
	case 7:
		return New[[7]byte]().Elem(arrayLevel, nullable, lc), nil
	case 8:
		return New[[8]byte]().Elem(arrayLevel, nullable, lc), nil
	case 9:
		return New[[9]byte]().Elem(arrayLevel, nullable, lc), nil
	case 10:
		return New[[10]byte]().Elem(arrayLevel, nullable, lc), nil
	case 11:
		return New[[11]byte]().Elem(arrayLevel, nullable, lc), nil
	case 12:
		return New[[12]byte]().Elem(arrayLevel, nullable, lc), nil
	case 13:
		return New[[13]byte]().Elem(arrayLevel, nullable, lc), nil
	case 14:
		return New[[14]byte]().Elem(arrayLevel, nullable, lc), nil
	case 15:
		return New[[15]byte]().Elem(arrayLevel, nullable, lc), nil
	case 16:
		return New[[16]byte]().Elem(arrayLevel, nullable, lc), nil
	case 17:
		return New[[17]byte]().Elem(arrayLevel, nullable, lc), nil
	case 18:
		return New[[18]byte]().Elem(arrayLevel, nullable, lc), nil
	case 19:
		return New[[19]byte]().Elem(arrayLevel, nullable, lc), nil
	case 20:
		return New[[20]byte]().Elem(arrayLevel, nullable, lc), nil
	case 21:
		return New[[21]byte]().Elem(arrayLevel, nullable, lc), nil
	case 22:
		return New[[22]byte]().Elem(arrayLevel, nullable, lc), nil
	case 23:
		return New[[23]byte]().Elem(arrayLevel, nullable, lc), nil
	case 24:
		return New[[24]byte]().Elem(arrayLevel, nullable, lc), nil
	case 25:
		return New[[25]byte]().Elem(arrayLevel, nullable, lc), nil
	case 26:
		return New[[26]byte]().Elem(arrayLevel, nullable, lc), nil
	case 27:
		return New[[27]byte]().Elem(arrayLevel, nullable, lc), nil
	case 28:
		return New[[28]byte]().Elem(arrayLevel, nullable, lc), nil
	case 29:
		return New[[29]byte]().Elem(arrayLevel, nullable, lc), nil
	case 30:
		return New[[30]byte]().Elem(arrayLevel, nullable, lc), nil
	case 31:
		return New[[31]byte]().Elem(arrayLevel, nullable, lc), nil
	case 32:
		return New[[32]byte]().Elem(arrayLevel, nullable, lc), nil
	case 33:
		return New[[33]byte]().Elem(arrayLevel, nullable, lc), nil
	case 34:
		return New[[34]byte]().Elem(arrayLevel, nullable, lc), nil
	case 35:
		return New[[35]byte]().Elem(arrayLevel, nullable, lc), nil
	case 36:
		return New[[36]byte]().Elem(arrayLevel, nullable, lc), nil
	case 37:
		return New[[37]byte]().Elem(arrayLevel, nullable, lc), nil
	case 38:
		return New[[38]byte]().Elem(arrayLevel, nullable, lc), nil
	case 39:
		return New[[39]byte]().Elem(arrayLevel, nullable, lc), nil
	case 40:
		return New[[40]byte]().Elem(arrayLevel, nullable, lc), nil
	case 41:
		return New[[41]byte]().Elem(arrayLevel, nullable, lc), nil
	case 42:
		return New[[42]byte]().Elem(arrayLevel, nullable, lc), nil
	case 43:
		return New[[43]byte]().Elem(arrayLevel, nullable, lc), nil
	case 44:
		return New[[44]byte]().Elem(arrayLevel, nullable, lc), nil
	case 45:
		return New[[45]byte]().Elem(arrayLevel, nullable, lc), nil
	case 46:
		return New[[46]byte]().Elem(arrayLevel, nullable, lc), nil
	case 47:
		return New[[47]byte]().Elem(arrayLevel, nullable, lc), nil
	case 48:
		return New[[48]byte]().Elem(arrayLevel, nullable, lc), nil
	case 49:
		return New[[49]byte]().Elem(arrayLevel, nullable, lc), nil
	case 50:
		return New[[50]byte]().Elem(arrayLevel, nullable, lc), nil
	case 51:
		return New[[51]byte]().Elem(arrayLevel, nullable, lc), nil
	case 52:
		return New[[52]byte]().Elem(arrayLevel, nullable, lc), nil
	case 53:
		return New[[53]byte]().Elem(arrayLevel, nullable, lc), nil
	case 54:
		return New[[54]byte]().Elem(arrayLevel, nullable, lc), nil
	case 55:
		return New[[55]byte]().Elem(arrayLevel, nullable, lc), nil
	case 56:
		return New[[56]byte]().Elem(arrayLevel, nullable, lc), nil
	case 57:
		return New[[57]byte]().Elem(arrayLevel, nullable, lc), nil
	case 58:
		return New[[58]byte]().Elem(arrayLevel, nullable, lc), nil
	case 59:
		return New[[59]byte]().Elem(arrayLevel, nullable, lc), nil
	case 60:
		return New[[60]byte]().Elem(arrayLevel, nullable, lc), nil
	case 61:
		return New[[61]byte]().Elem(arrayLevel, nullable, lc), nil
	case 62:
		return New[[62]byte]().Elem(arrayLevel, nullable, lc), nil
	case 63:
		return New[[63]byte]().Elem(arrayLevel, nullable, lc), nil
	case 64:
		return New[[64]byte]().Elem(arrayLevel, nullable, lc), nil
	case 65:
		return New[[65]byte]().Elem(arrayLevel, nullable, lc), nil
	case 66:
		return New[[66]byte]().Elem(arrayLevel, nullable, lc), nil
	case 67:
		return New[[67]byte]().Elem(arrayLevel, nullable, lc), nil
	case 68:
		return New[[68]byte]().Elem(arrayLevel, nullable, lc), nil
	case 69:
		return New[[69]byte]().Elem(arrayLevel, nullable, lc), nil
	case 70:
		return New[[70]byte]().Elem(arrayLevel, nullable, lc), nil
	}

	return nil, fmt.Errorf("fixed length %d is not supported", fixedLen)
}
