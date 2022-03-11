package chconn

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/vahid-sohrabloo/chconn/column"
)

// SelectStmt is a interface for select statement
type SelectStmt interface {
	// Next get the next block, if available return true else return false
	// if the server sends an error return false and we can get the last error with Err() function
	Next() bool
	// Err When calls Next() func, if server send an error, we can get error from this function
	Err() error
	// RowsInBlock return number of rows in this current block
	RowsInBlock() int
	// Close after reads all data should call this function to unlock connection
	// NOTE: You shoud read all data and then call this function
	Close()
	// ReadColumns read all columns of block
	ReadColumns(columns ...column.Column) error
	// GetColumns get and read all columns of block
	// If you know the columns  it's better to use ReadColumns func
	GetColumns() ([]column.Column, error)
}
type selectStmt struct {
	block          *block
	conn           *conn
	query          string
	queryID        string
	clientInfo     *ClientInfo
	onProgress     func(*Progress)
	onProfile      func(*Profile)
	lastErr        error
	ProfileInfo    *Profile
	Progress       *Progress
	closed         bool
	columnsForRead []column.Column
	ctx            context.Context
	needReadData   bool
	finishSelect   bool
}

var _ SelectStmt = &selectStmt{}

// Next get the next block, if available return true else return false
// if the server sends an error return false and we can get the last error with Err() function
func (s *selectStmt) Next() bool {
	s.conn.reader.SetCompress(false)
	res, err := s.conn.receiveAndProccessData(nil)
	if err != nil {
		s.lastErr = err
		s.Close()
		return false
	}
	s.conn.reader.SetCompress(s.conn.compress)
	if block, ok := res.(*block); ok {
		if block.NumRows == 0 {
			err = block.readColumns(s.conn)
			if err != nil {
				s.lastErr = err
				s.Close()
				return false
			}
			return s.Next()
		}
		s.needReadData = true
		s.block = block
		return true
	}

	if profile, ok := res.(*Profile); ok {
		s.ProfileInfo = profile
		if s.onProfile != nil {
			s.onProfile(profile)
		}
		return s.Next()
	}
	if progress, ok := res.(*Progress); ok {
		s.Progress = progress
		if s.onProgress != nil {
			s.onProgress(progress)
		}
		return s.Next()
	}

	if res == nil {
		s.finishSelect = true
		s.columnsForRead = nil
		return false
	}

	s.lastErr = &unexpectedPacket{expected: "serverData", actual: res}
	return false
}

// RowsInBlock return number of rows in this current block
func (s *selectStmt) RowsInBlock() int {
	return int(s.block.NumRows)
}

// Err When calls Next() func, if server send an error, we can get error from this function
func (s *selectStmt) Err() error {
	return preferContextOverNetTimeoutError(s.ctx, s.lastErr)
}

// Close after reads all data should call this function to unlock connection
// NOTE: You should read all blocks and then call this function
func (s *selectStmt) Close() {
	s.conn.reader.SetCompress(false)
	if !s.closed {
		s.closed = true
		s.conn.contextWatcher.Unwatch()
		s.conn.unlock()
		if s.Err() != nil || s.needReadData || !s.finishSelect {
			s.conn.Close()
		}
	}
}

// ReadColumns read all columns of block
func (s *selectStmt) ReadColumns(columns ...column.Column) error {
	if int(s.block.NumColumns) != len(columns) {
		s.Close()
		s.conn.Close()
		return &ColumnNumberReadError{
			Read:      len(columns),
			Available: s.block.NumColumns,
		}
	}
	// todo: validate number of bytes

	if !s.needReadData {
		return nil
	}

	s.needReadData = false
	for _, col := range columns {
		err := col.HeaderReader(s.conn.reader, true)
		if err != nil {
			s.Close()
			s.conn.Close()
			return preferContextOverNetTimeoutError(s.ctx, err)
		}
		err = col.ReadRaw(s.RowsInBlock(), s.conn.reader)
		if err != nil {
			s.Close()
			s.conn.Close()
			return preferContextOverNetTimeoutError(s.ctx, err)
		}
	}
	return nil
}

// GetColumns get and read all columns of block
// If you know the columns  it's better to use ReadColumns func
func (s *selectStmt) GetColumns() ([]column.Column, error) {
	if s.block == nil {
		return nil, nil
	}

	if !s.needReadData {
		return nil, nil
	}

	if len(s.columnsForRead) > 0 {
		return s.columnsForRead, s.ReadColumns(s.columnsForRead...)
	}

	s.needReadData = false

	numberColumnRead := int(s.block.NumColumns)
	columns := make([]column.Column, 0, numberColumnRead)
	columnsForRead := make([]column.Column, 0, numberColumnRead)
	for i := 0; i < int(s.block.NumColumns); i++ {
		chColumn, err := s.block.nextColumn(s.conn)
		if err != nil {
			s.Close()
			s.conn.Close()
			return nil, preferContextOverNetTimeoutError(s.ctx, err)
		}
		s.columnByType(&columns, chColumn.ChType, false)
		readColumn := len(columns) - 1
		columns[readColumn].SetName(chColumn.Name)
		columns[readColumn].SetType(chColumn.ChType)
		err = columns[readColumn].HeaderReader(s.conn.reader, false)
		if err != nil {
			s.Close()
			s.conn.Close()
			return nil, preferContextOverNetTimeoutError(s.ctx, err)
		}
		err = columns[readColumn].ReadRaw(s.RowsInBlock(), s.conn.reader)
		if err != nil {
			s.Close()
			s.conn.Close()
			return nil, preferContextOverNetTimeoutError(s.ctx, err)
		}
		columnsForRead = append(columnsForRead, columns[readColumn])
	}
	s.columnsForRead = columnsForRead
	return s.columnsForRead, nil
}

//nolint:funlen,gocyclo
func (s *selectStmt) columnByType(columns *[]column.Column, chType []byte, nullable bool) {
	switch {
	case string(chType) == "Int8":
		*columns = append(*columns, column.NewInt8(nullable))
	case string(chType) == "Int16":
		*columns = append(*columns, column.NewInt16(nullable))
	case string(chType) == "Int32":
		*columns = append(*columns, column.NewInt32(nullable))
	case string(chType) == "Int64":
		*columns = append(*columns, column.NewInt64(nullable))

	case string(chType) == "UInt8":
		*columns = append(*columns, column.NewUint8(nullable))
	case string(chType) == "UInt16":
		*columns = append(*columns, column.NewUint16(nullable))
	case string(chType) == "UInt32":
		*columns = append(*columns, column.NewUint32(nullable))
	case string(chType) == "UInt64":
		*columns = append(*columns, column.NewUint64(nullable))

	case string(chType) == "Float32":
		*columns = append(*columns, column.NewFloat32(nullable))
	case string(chType) == "Float64":
		*columns = append(*columns, column.NewFloat64(nullable))

	case string(chType) == "String":
		*columns = append(*columns, column.NewString(nullable))
	case bytes.HasPrefix(chType, []byte("FixedString(")):
		strlen, _ := strconv.Atoi(string(chType[len("FixedString(") : len(chType)-1]))
		*columns = append(*columns, column.NewFixedString(strlen, nullable))
	case string(chType) == "Date":
		*columns = append(*columns, column.NewDate(nullable))
	case string(chType) == "Date32":
		*columns = append(*columns, column.NewDate32(nullable))
	case string(chType) == "DateTime" || bytes.HasPrefix(chType, []byte("DateTime(")):
		var params [][]byte
		if bytes.HasPrefix(chType, []byte("DateTime(")) {
			params = bytes.Split(chType[len("DateTime("):len(chType)-1], []byte(", "))
		}
		col := column.NewDateTime(nullable)
		if len(params) > 0 && len(params[0]) >= 3 {
			if loc, err := time.LoadLocation(string(params[0][1 : len(params[0])-1])); err == nil {
				col.SetLocation(loc)
			}
		}
		*columns = append(*columns, col)
	case bytes.HasPrefix(chType, []byte("DateTime64(")):
		params := bytes.Split(chType[len("DateTime64("):len(chType)-1], []byte(", "))
		if len(params) == 0 {
			panic("DateTime64 invalid params")
		}
		precision, err := strconv.Atoi(string(params[0]))
		if err != nil {
			panic("DateTime64 invalid precision: " + err.Error())
		}
		col := column.NewDateTime64(precision, nullable)
		if len(params) > 1 && len(params[1]) >= 3 {
			if loc, err := time.LoadLocation(string(params[1][1 : len(params[1])-1])); err == nil {
				col.SetLocation(loc)
			}
		}
		*columns = append(*columns, col)
	case bytes.HasPrefix(chType, []byte("Decimal(")):
		params := bytes.Split(chType[len("Decimal("):len(chType)-1], []byte(", "))
		precision, _ := strconv.Atoi(string(params[0]))
		scale, _ := strconv.Atoi(string(params[1]))
		if precision <= 9 {
			*columns = append(*columns, column.NewDecimal32(scale, nullable))
		} else if precision <= 18 {
			*columns = append(*columns, column.NewDecimal64(scale, nullable))
		} else if precision <= 38 {
			*columns = append(*columns, column.NewDecimal128(nullable))
		} else if precision <= 76 {
			*columns = append(*columns, column.NewDecimal256(nullable))
		} else {
			panic("Decimal invalid precision: " + string(chType))
		}

	case string(chType) == "UUID":
		*columns = append(*columns, column.NewUUID(nullable))
	case string(chType) == "IPv4":
		*columns = append(*columns, column.NewIPv4(nullable))
	case string(chType) == "IPv6":
		*columns = append(*columns, column.NewIPv6(nullable))
	case bytes.HasPrefix(chType, []byte("Enum8(")):
		*columns = append(*columns, column.NewEnum8(nullable))
	case bytes.HasPrefix(chType, []byte("Enum16(")):
		*columns = append(*columns, column.NewEnum16(nullable))
	case bytes.HasPrefix(chType, []byte("Nullable(")):
		s.columnByType(columns, chType[len("Nullable("):len(chType)-1], true)
	case bytes.HasPrefix(chType, []byte("SimpleAggregateFunction(")):
		s.columnByType(columns, getNestedType(chType[len("SimpleAggregateFunction("):]), nullable)
	case bytes.HasPrefix(chType, []byte("Array(")):
		s.columnByType(columns, chType[len("Array("):len(chType)-1], nullable)
		*columns = append(*columns, column.NewArray((*columns)[len(*columns)-1]))
	case bytes.HasPrefix(chType, []byte("LowCardinality(")):
		s.columnByType(columns, chType[len("LowCardinality("):len(chType)-1], nullable)
		lcDict := (*columns)[len(*columns)-1].(column.LCDictColumn)
		*columns = append(*columns, column.NewLowCardinality(lcDict))
	case bytes.HasPrefix(chType, []byte("Tuple(")):
		var openFunc int
		cur := 0
		// for between `Tuple(` and `)`
		idx := 1
		tupleTypes := chType[6 : len(chType)-1]
		columnsTuple := make([]column.Column, 0)
		for i, char := range tupleTypes {
			if char == ',' {
				if openFunc == 0 {
					s.columnByType(columns, tupleTypes[cur:i], nullable)
					columnsTuple = append(columnsTuple, (*columns)[len(*columns)-1])
					idx++
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
		s.columnByType(columns, tupleTypes[cur:], nullable)
		columnsTuple = append(columnsTuple, (*columns)[len(*columns)-1])
		*columns = append(*columns, column.NewTuple(columnsTuple...))

	default:
		panic("unknown type: " + string(chType))
	}
}

func getNestedType(chType []byte) []byte {
	for i, v := range chType {
		if v == ',' {
			return chType[i+2 : len(chType)-1]
		}
	}
	panic("Cannot found  netsted type of " + string(chType))
}
