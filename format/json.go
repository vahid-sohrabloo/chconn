package format

import (
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// JSON exports ClickHouse query results as JSON.
// It processes column data row by row and calls onData with the serialized JSON bytes.
type JSON struct {
	out        []byte
	FlushEvery int
	onData     func([]byte, []column.ColumnCore)
}

const doubleQuoteJSON = '"'
const semiColonJSON = ','

// NewJSON returns a new JSON exporter.
func NewJSON(flushEvery int, onData func([]byte, []column.ColumnCore)) *JSON {
	return &JSON{
		FlushEvery: flushEvery,
		onData:     onData,
	}
}

// Read reads all rows from the select statement and serializes them as a JSON object array.
func (j *JSON) Read(stmt chconn.SelectStmt) error {
	var addCommaColumn bool
	var addCommaRows bool
	j.out = j.out[:0]
	for stmt.Next() {
		columns := stmt.Columns()

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
				j.out = col.ToJSON(i, false, j.out)
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

// ReadCompact reads all rows and serializes them as compact JSON arrays (no column names).
func (j *JSON) ReadCompact(stmt chconn.SelectStmt) error {
	var addCommaColumn bool
	var addCommaRows bool
	j.out = j.out[:0]
	for stmt.Next() {
		columns := stmt.Columns()

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
				j.out = col.ToJSON(i, false, j.out)
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

// ReadEachRow reads all rows and calls onData for each individual row as a JSON object.
func (j *JSON) ReadEachRow(stmt chconn.SelectStmt) error {
	var addCommaColumn bool
	j.out = j.out[:0]
	for stmt.Next() {
		columns := stmt.Columns()

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
				j.out = col.ToJSON(i, false, j.out)
			}
			j.out = append(j.out, '}')
			i++

			j.onData(j.out, columns)
			j.out = j.out[:0]
		}
	}
	return nil
}
