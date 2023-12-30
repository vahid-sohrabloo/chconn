package chpool

import (
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

type errRows struct {
	err error
}

func (errRows) Close()                          {}
func (e errRows) Err() error                    { return e.err }
func (errRows) Next() bool                      { return false }
func (e errRows) Scan(dest ...any) error        { return e.err }
func (e errRows) Values() []any                 { return nil }
func (e errRows) Conn() chconn.Conn             { return nil }
func (e errRows) Columns() []column.ColumnBasic { return nil }
func (e errRows) CurrentRow() int               { return 0 }

type errRow struct {
	err error
}

func (e errRow) Scan(dest ...any) error { return e.err }

type poolRows struct {
	r   chconn.Rows
	c   Conn
	err error
}

func (rows *poolRows) Close() {
	rows.r.Close()
	if rows.c != nil {
		rows.c.Release()
		rows.c = nil
	}
}

func (rows *poolRows) Err() error {
	if rows.err != nil {
		return rows.err
	}
	return rows.r.Err()
}

func (rows *poolRows) Next() bool {
	if rows.err != nil {
		return false
	}

	n := rows.r.Next()
	if !n {
		rows.Close()
	}
	return n
}

func (rows *poolRows) Scan(dest ...any) error {
	err := rows.r.Scan(dest...)
	if err != nil {
		rows.Close()
	}
	return err
}

func (rows *poolRows) Values() []any {
	return rows.r.Values()
}

func (rows *poolRows) Conn() chconn.Conn {
	return rows.r.Conn()
}

func (rows *poolRows) CurrentRow() int {
	return rows.r.CurrentRow()
}

func (rows *poolRows) Columns() []column.ColumnBasic {
	return rows.r.Columns()
}

type poolRow struct {
	r   chconn.Row
	c   Conn
	err error
}

func (row *poolRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}

	panicked := true
	defer func() {
		if panicked && row.c != nil {
			row.c.Release()
		}
	}()
	err := row.r.Scan(dest...)
	panicked = false
	if row.c != nil {
		row.c.Release()
	}
	return err
}
