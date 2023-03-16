package export

import (
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
)

type JSON struct {
	out        []byte
	FlushEvery int
	onData     func([]byte, []column.Column)
}

// NewJSON returns a new JSON exporter.
func NewJSON(flushEvery int, onData func([]byte, []column.Column)) *JSON {
	return &JSON{
		FlushEvery: flushEvery,
		onData:     onData,
	}
}

func (j *JSON) Read(stmt chconn.SelectStmt) error {
	var addCommaColumn bool
	var addCommaRows bool
	j.out = j.out[:0]
	for stmt.Next() {
		columns, err := stmt.GetColumns()
		if err != nil {
			return err
		}
		if cap(j.out) <= 0 {
			j.out = make([]byte, 0, len(columns)*8*stmt.RowsInBlock())
		}
		rows := stmt.RowsInBlock()
		i := 0
		for i < rows {
			if addCommaRows {
				j.out = append(j.out, semiColonJSON)
			}
			addCommaRows = true
			j.out = append(j.out, '{')
			addCommaColumn = false
			for _, col := range columns {
				if addCommaColumn {
					j.out = append(j.out, semiColonJSON)
				}
				addCommaColumn = true
				j.out = append(j.out, doubleQuoteJSON)
				j.out = append(j.out, col.Name()...)
				j.out = append(j.out, doubleQuoteJSON, ':')
				j.writeColumn(col, i)
			}
			j.out = append(j.out, '}')
			i++
			if i%j.FlushEvery == 0 {
				j.onData(j.out, columns)
				j.out = j.out[:0]
			}
		}

		if len(j.out) > 0 {
			j.onData(j.out, columns)
			j.out = j.out[:0]
		}
	}
	return nil
}

func (j *JSON) ReadCompact(stmt chconn.SelectStmt) error {
	var addCommaColumn bool
	var addCommaRows bool
	j.out = j.out[:0]
	for stmt.Next() {
		columns, err := stmt.GetColumns()
		if err != nil {
			return err
		}
		if cap(j.out) <= 0 {
			j.out = make([]byte, 0, len(columns)*8*stmt.RowsInBlock())
		}
		rows := stmt.RowsInBlock()
		i := 0
		for i < rows {
			if addCommaRows {
				j.out = append(j.out, semiColonJSON)
			}
			addCommaRows = true
			j.out = append(j.out, '[')
			addCommaColumn = false
			for _, col := range columns {
				if addCommaColumn {
					j.out = append(j.out, semiColonJSON)
				}
				addCommaColumn = true
				j.writeColumn(col, i)
			}
			j.out = append(j.out, ']')
			i++
			if i%j.FlushEvery == 0 {
				j.onData(j.out, columns)
				j.out = j.out[:0]
			}
		}

		if len(j.out) > 0 {
			j.onData(j.out, columns)
			j.out = j.out[:0]
		}
	}
	return nil
}

func (j *JSON) ReadEachRow(stmt chconn.SelectStmt) error {
	var addCommaColumn bool
	j.out = j.out[:0]
	for stmt.Next() {
		columns, err := stmt.GetColumns()
		if err != nil {
			return err
		}
		if cap(j.out) <= 0 {
			j.out = make([]byte, 0, len(columns)*8*stmt.RowsInBlock())
		}
		rows := stmt.RowsInBlock()
		i := 0
		for i < rows {
			j.out = append(j.out, '{')
			addCommaColumn = false
			for _, col := range columns {
				if addCommaColumn {
					j.out = append(j.out, semiColonJSON)
				}
				addCommaColumn = true
				j.out = append(j.out, doubleQuoteJSON)
				j.out = append(j.out, col.Name()...)
				j.out = append(j.out, doubleQuoteJSON, ':')
				j.writeColumn(col, i)
			}
			j.out = append(j.out, '}')
			i++

			j.onData(j.out, columns)
			j.out = j.out[:0]
		}
	}
	return nil
}

//nolint:funlen,gocyclo
func (j *JSON) writeColumn(col column.Column, row int) {
	switch v := col.(type) {
	case *column.Array:
		j.out = append(j.out, '[')
		lenDara := v.Row(row)
		dataColumn := v.DataColumn()
		iRow := 0
		for i := 0; i < lenDara; i++ {
			if i > 0 {
				j.out = append(j.out, semiColonJSON)
			}
			j.writeColumn(dataColumn, iRow)
			iRow++
		}
		j.out = append(j.out, ']')
	case *column.Tuple:
		j.out = append(j.out, '[')
		columns := v.Columns()
		for i, col := range columns {
			if i > 0 {
				j.out = append(j.out, semiColonJSON)
			}
			j.writeColumn(col, row)
		}
		j.out = append(j.out, ']')
	case *column.Int8:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = strconv.AppendInt(j.out, int64(v.Row(row)), 10)
	case *column.Int16:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = strconv.AppendInt(j.out, int64(v.Row(row)), 10)
	case *column.Int32:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = strconv.AppendInt(j.out, int64(v.Row(row)), 10)
	case *column.Int64:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = append(j.out, doubleQuoteJSON)
		j.out = strconv.AppendInt(j.out, v.Row(row), 10)
		j.out = append(j.out, doubleQuoteJSON)
	case *column.Uint8:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = strconv.AppendUint(j.out, uint64(v.Row(row)), 10)
	case *column.Uint16:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = strconv.AppendUint(j.out, uint64(v.Row(row)), 10)
	case *column.Uint32:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = strconv.AppendUint(j.out, uint64(v.Row(row)), 10)
	case *column.Uint64:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = append(j.out, doubleQuoteJSON)
		j.out = strconv.AppendUint(j.out, v.Row(row), 10)
		j.out = append(j.out, doubleQuoteJSON)
	case *column.String:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = jsonString(j.out, bytesToStr(v.Row(row)))
	case *column.FixedString:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = jsonString(j.out, bytesToStr(v.Row(row)))
	case *column.Float32:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		val := float64(v.Row(row))
		// NaN
		if val != val {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = strconv.AppendFloat(j.out, val, 'g', -1, 32)
	case *column.Float64:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		val := v.Row(row)
		// NaN
		if val != val {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = strconv.AppendFloat(j.out, val, 'g', -1, 64)
	case *column.Decimal32:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = strconv.AppendFloat(j.out, v.Row(row), 'f', v.Scale, 64)
	case *column.Decimal64:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = strconv.AppendFloat(j.out, v.Row(row), 'f', v.Scale, 64)
	case *column.Date:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		// TODO: performance improvement
		j.out = append(j.out, doubleQuoteJSON)
		j.out = v.Row(row).AppendFormat(j.out, "2006-01-02")
		j.out = append(j.out, doubleQuoteJSON)
	case *column.Date32:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		// TODO: performance improvement
		j.out = append(j.out, doubleQuoteJSON)
		j.out = v.Row(row).AppendFormat(j.out, "2006-01-02")
		j.out = append(j.out, doubleQuoteJSON)
	case *column.DateTime:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		// TODO: performance improvement
		j.out = append(j.out, doubleQuoteJSON)
		j.out = v.Row(row).AppendFormat(j.out, "2006-01-02 15:04:05")
		j.out = append(j.out, doubleQuoteJSON)
	case *column.DateTime64:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		// TODO: performance improvement
		j.out = append(j.out, doubleQuoteJSON)
		j.out = v.Row(row).AppendFormat(j.out, "2006-01-02 15:04:05")
		j.out = append(j.out, doubleQuoteJSON)
	case *column.Enum8:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		// it should return error
		m, _ := v.IntToStringMap()
		j.out = jsonString(j.out, m[v.Row(row)])
	case *column.Enum16:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		// it should return error
		m, _ := v.IntToStringMap()
		j.out = jsonString(j.out, m[v.Row(row)])
	case *column.UUID:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = append(j.out, doubleQuoteJSON)
		j.out = append(j.out, uuid.UUID(v.Row(row)).String()...)
		j.out = append(j.out, doubleQuoteJSON)
	case *column.IPv4:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = append(j.out, doubleQuoteJSON)
		j.out = append(j.out, v.Row(row).String()...)
		j.out = append(j.out, doubleQuoteJSON)
	case *column.IPv6:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
		j.out = append(j.out, doubleQuoteJSON)
		j.out = append(j.out, v.Row(row).String()...)
		j.out = append(j.out, doubleQuoteJSON)
	case *column.Nothing:
		if col.RowIsNil(row) {
			j.out = append(j.out, nullJSON...)
			return
		}
	case *column.LC:
		// currently only support String and FixedString
		switch dict := v.DictColumn.(type) {
		case *column.String:
			i := v.Row(row)
			if i == 0 && dict.IsNullable() {
				j.out = append(j.out, nullJSON...)
				return
			}
			j.out = jsonString(j.out, bytesToStr(dict.Row(i)))
		case *column.FixedString:
			i := v.Row(row)
			if i == 0 && dict.IsNullable() {
				j.out = append(j.out, nullJSON...)
				return
			}
			j.out = jsonString(j.out, bytesToStr(dict.Row(i)))
		default:
			panic(fmt.Sprintf("unsupported column type %s", string(col.Type())))
		}
	default:
		panic(fmt.Sprintf("unsupported column type %s", string(col.Type())))
	}
}
