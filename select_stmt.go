package chconn

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/vahid-sohrabloo/chconn/v2/column"
	"github.com/vahid-sohrabloo/chconn/v2/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v2/types"
)

// Select executes a query and return select stmt.
// NOTE: only use for select query
func (ch *conn) Select(ctx context.Context, query string, columns ...column.ColumnBasic) (SelectStmt, error) {
	return ch.SelectWithOption(ctx, query, nil, columns...)
}

// Select executes a query with the the query options and return select stmt.
// NOTE: only use for select query
func (ch *conn) SelectWithOption(
	ctx context.Context,
	query string,
	queryOptions *QueryOptions,
	columns ...column.ColumnBasic,
) (SelectStmt, error) {
	err := ch.lock()
	if err != nil {
		return nil, err
	}

	var hasError bool
	defer func() {
		if hasError {
			ch.Close()
		}
	}()

	if ctx != context.Background() {
		select {
		case <-ctx.Done():
			return nil, newContextAlreadyDoneError(ctx)
		default:
		}
		ch.contextWatcher.Watch(ctx)
	}

	if queryOptions == nil {
		queryOptions = emptyQueryOptions
	}

	err = ch.sendQueryWithOption(query, queryOptions.QueryID, queryOptions.Settings, queryOptions.Parameters)
	if err != nil {
		hasError = true
		return nil, preferContextOverNetTimeoutError(ctx, err)
	}
	s := &selectStmt{
		conn:           ch,
		query:          query,
		queryOptions:   queryOptions,
		clientInfo:     nil,
		ctx:            ctx,
		columnsForRead: columns,
	}
	res, err := s.conn.receiveAndProcessData(nil)
	if err != nil {
		s.lastErr = err
		s.Close()
		return nil, err
	}
	if block, ok := res.(*block); ok {
		if block.NumRows == 0 {
			err = s.readEmptyBlock(block)
			if err != nil {
				return nil, err
			}
			return s, nil
		}
	}
	return nil, &unexpectedPacket{expected: "serverData with zero len", actual: res}
}

// SelectStmt is a interface for select statement
type SelectStmt interface {
	// Next read the next block of data for reading.
	// It returns true on success, or false if there is no next result row or an error happened while preparing it.
	// Err should be consulted to distinguish between the two cases.
	Next() bool
	// Err returns the error, if any, that was encountered during iteration.
	// Err may be called after an explicit or implicit Close.
	Err() error
	// RowsInBlock return number of rows in this current block
	RowsInBlock() int
	// Columns return the columns of this select statement.
	Columns() []column.ColumnBasic
	// Close close the statement and release the connection
	// If Next is called and returns false and there are no further blocks,
	// the Rows are closed automatically and it will suffice to check the result of Err.
	// Close is idempotent and does not affect the result of Err.
	Close()
}

type selectStmt struct {
	block          *block
	conn           *conn
	query          string
	queryOptions   *QueryOptions
	clientInfo     *ClientInfo
	lastErr        error
	closed         bool
	columnsForRead []column.ColumnBasic
	ctx            context.Context
	finishSelect   bool
	validateData   bool
}

var _ SelectStmt = &selectStmt{}

func (s *selectStmt) readEmptyBlock(b *block) error {
	err := b.readColumns(s.conn)
	if err != nil {
		s.lastErr = err
		s.Close()
		return err
	}
	if len(s.columnsForRead) == 0 {
		s.columnsForRead, err = s.getColumnsByChType(b)
		if err != nil {
			s.lastErr = err
			s.Close()
			return err
		}
	} else if len(s.columnsForRead[0].Name()) != 0 {
		s.columnsForRead, err = b.reorderColumns(s.columnsForRead)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *selectStmt) Next() bool {
	// protect after close
	if s.closed {
		return false
	}
	s.conn.reader.SetCompress(false)
	res, err := s.conn.receiveAndProcessData(nil)
	if err != nil {
		s.lastErr = err
		s.Close()
		return false
	}

	if block, ok := res.(*block); ok {
		if block.NumRows == 0 {
			err = s.readEmptyBlock(block)
			if err != nil {
				return false
			}
			return s.Next()
		}
		s.block = block

		needValidateData := !s.validateData
		s.validateData = false
		if needValidateData {
			if errValidate := s.validate(); errValidate != nil {
				s.lastErr = errValidate
				s.Close()
				return false
			}
		}

		err = block.readColumnsData(s.conn, needValidateData, s.columnsForRead...)
		if err != nil {
			s.lastErr = preferContextOverNetTimeoutError(s.ctx, err)
			s.Close()
			return false
		}
		return true
	}

	if profile, ok := res.(*Profile); ok {
		if s.queryOptions.OnProfile != nil {
			s.queryOptions.OnProfile(profile)
		}
		return s.Next()
	}
	if progress, ok := res.(*Progress); ok {
		if s.queryOptions.OnProgress != nil {
			s.queryOptions.OnProgress(progress)
		}
		return s.Next()
	}

	if profileEvent, ok := res.(*ProfileEvent); ok {
		if s.queryOptions.OnProfileEvent != nil {
			s.queryOptions.OnProfileEvent(profileEvent)
		}
		return s.Next()
	}

	if res == nil {
		s.finishSelect = true
		s.columnsForRead = nil
		s.Close()
		return false
	}

	s.lastErr = &unexpectedPacket{expected: "serverData", actual: res}
	s.Close()
	return false
}

func (s *selectStmt) validate() error {
	if int(s.block.NumColumns) != len(s.columnsForRead) {
		return &ColumnNumberReadError{
			Read:      len(s.columnsForRead),
			Available: s.block.NumColumns,
		}
	}
	return nil
}

// RowsInBlock return number of rows in this current block
func (s *selectStmt) RowsInBlock() int {
	return int(s.block.NumRows)
}

// Err returns the error, if any, that was encountered during iteration.
// Err may be called after an explicit or implicit Close.
func (s *selectStmt) Err() error {
	return preferContextOverNetTimeoutError(s.ctx, s.lastErr)
}

// Close close the statement and release the connection
// If Next is called and returns false and there are no further blocks,
// the Rows are closed automatically and it will suffice to check the result of Err.
// Close is idempotent and does not affect the result of Err.
func (s *selectStmt) Close() {
	s.conn.reader.SetCompress(false)
	if !s.closed {
		s.closed = true
		s.conn.contextWatcher.Unwatch()
		s.conn.unlock()
		if s.Err() != nil || !s.finishSelect {
			s.conn.Close()
		}
	}
}

func (s *selectStmt) Columns() []column.ColumnBasic {
	return s.columnsForRead
}

func (s *selectStmt) getColumnsByChType(b *block) ([]column.ColumnBasic, error) {
	columns := make([]column.ColumnBasic, len(b.Columns))
	for i, col := range b.Columns {
		columnByType, err := s.columnByType(col.ChType, 0, false, false)
		if err != nil {
			return nil, err
		}
		columns[i] = columnByType
	}
	return columns, nil
}

//nolint:funlen,gocyclo
func (s *selectStmt) columnByType(chType []byte, arrayLevel int, nullable, lc bool) (column.ColumnBasic, error) {
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
	case helper.IsFixedString(chType):
		strLen, err := strconv.Atoi(string(chType[helper.FixedStringStrLen : len(chType)-1]))
		if err != nil {
			return nil, fmt.Errorf("invalid fixed string length: %s: %w", string(chType), err)
		}
		return getFixedType(strLen, arrayLevel, nullable, lc)
	case string(chType) == "Date":
		if !s.queryOptions.UseGoTime {
			return column.New[types.Date]().Elem(arrayLevel, nullable, lc), nil
		}
		return column.NewDate[types.Date]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "Date32":
		if !s.queryOptions.UseGoTime {
			return column.New[types.Date32]().Elem(arrayLevel, nullable, lc), nil
		}
		return column.NewDate[types.Date32]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "DateTime" || helper.IsDateTimeWithParam(chType):
		if !s.queryOptions.UseGoTime {
			return column.New[types.DateTime]().Elem(arrayLevel, nullable, lc), nil
		}
		var params [][]byte
		if bytes.HasPrefix(chType, []byte("DateTime(")) {
			params = bytes.Split(chType[len("DateTime("):len(chType)-1], []byte(", "))
		}
		col := column.NewDate[types.DateTime]()
		if len(params) > 0 && len(params[0]) >= 3 {
			if loc, err := time.LoadLocation(string(params[0][1 : len(params[0])-1])); err == nil {
				col.SetLocation(loc)
			} else if loc, err := time.LoadLocation(s.conn.serverInfo.Timezone); err == nil {
				col.SetLocation(loc)
			}
		}
		return col.Elem(arrayLevel, nullable, lc), nil
	case helper.IsDateTime64(chType):
		if !s.queryOptions.UseGoTime {
			return column.New[types.DateTime64]().Elem(arrayLevel, nullable, lc), nil
		}
		params := bytes.Split(chType[helper.DateTime64StrLen:len(chType)-1], []byte(", "))
		if len(params) == 0 {
			panic("DateTime64 invalid params")
		}
		precision, err := strconv.Atoi(string(params[0]))
		if err != nil {
			panic("DateTime64 invalid precision: " + err.Error())
		}
		col := column.NewDate[types.DateTime64]()
		col.SetPrecision(precision)
		if len(params) > 1 && len(params[1]) >= 3 {
			if loc, err := time.LoadLocation(string(params[1][1 : len(params[1])-1])); err == nil {
				col.SetLocation(loc)
			} else if loc, err := time.LoadLocation(s.conn.serverInfo.Timezone); err == nil {
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
		panic("Decimal invalid precision: " + string(chType))

	case string(chType) == "UUID":
		return column.New[types.UUID]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "IPv4":
		return column.New[types.IPv4]().Elem(arrayLevel, nullable, lc), nil
	case string(chType) == "IPv6":
		return column.New[types.IPv6]().Elem(arrayLevel, nullable, lc), nil

	case helper.IsNullable(chType):
		return s.columnByType(chType[helper.LenNullableStr:len(chType)-1], arrayLevel, true, lc)

	case bytes.HasPrefix(chType, []byte("SimpleAggregateFunction(")):
		return s.columnByType(helper.FilterSimpleAggregate(chType), arrayLevel, nullable, lc)
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
		return s.columnByType(chType[helper.LenArrayStr:len(chType)-1], arrayLevel+1, nullable, lc)
	case helper.IsLowCardinality(chType):
		return s.columnByType(chType[helper.LenLowCardinalityStr:len(chType)-1], arrayLevel, nullable, true)
	case helper.IsTuple(chType):
		columnsTuple := helper.TypesInParentheses(chType[helper.LenTupleStr : len(chType)-1])
		columns := make([]column.ColumnBasic, len(columnsTuple))
		for i, col := range columnsTuple {
			col, err := s.columnByType(col, arrayLevel, nullable, lc)
			if err != nil {
				return nil, err
			}
			columns[i] = col
		}
		// todo check if need Elem or not
		return column.NewTuple(columns...), nil
	case helper.IsMap(chType):
		columnsMap := helper.TypesInParentheses(chType[helper.LenMapStr : len(chType)-1])
		if len(columnsMap) != 2 {
			return nil, fmt.Errorf("map must have 2 columns")
		}
		columns := make([]column.ColumnBasic, len(columnsMap))
		for i, col := range columnsMap {
			col, err := s.columnByType(col, arrayLevel, nullable, lc)
			if err != nil {
				return nil, err
			}
			columns[i] = col
		}
		return column.NewMapBase(columns[0], columns[1]), nil
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
