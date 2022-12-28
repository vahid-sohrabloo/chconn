package chconn

import (
	"fmt"
)

// type Rows interface {
// 	Next() bool
// 	Scan(dest ...any) error
// 	Err() error
// 	Close()
// }

type Rows struct {
	selectStmt *selectStmt
	totalRow   int
	currentRow int
}

func (r *Rows) Next() bool {
	r.currentRow++
	if r.totalRow <= r.currentRow {
		if !r.selectStmt.Next() {
			return false
		}
		r.totalRow = r.selectStmt.RowsInBlock()
		r.currentRow = 0
		return true
	}
	return true
}

func (r *Rows) Scan(dest ...any) error {
	columns := r.selectStmt.Columns()
	if len(columns) != len(dest) {
		err := fmt.Errorf("number of columns must equal number of destinations, got %d and %d", len(columns), len(dest))
		r.fatal(err)
		return err
	}
	for i, dst := range dest {
		if err := columns[i].Scan(r.currentRow, dst); err != nil {
			r.fatal(fmt.Errorf("scan column at index %d (%s): %w", i, string(columns[i].Name()), err))
			return err
		}
	}
	// for i, col := range columns {
	// 	// if dest[i] == nil {
	// 	// 	continue
	// 	// }
	// 	if err := col.Scan(r.currentRow, dest[i]); err != nil {
	// 		r.fatal(err)
	// 		return err
	// 	}
	// 	// if err := col.Scan(r.currentRow, reflect.ValueOf(dest[i]).Elem()); err != nil {
	// 	// 	r.fatal(err)
	// 	// 	return err
	// 	// }

	// 	// if rows.scanTypes[i] != reflect.TypeOf(dst) {
	// 	// 	rows.scanPlans[i] = m.PlanScan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, dest[i])
	// 	// 	rows.scanTypes[i] = reflect.TypeOf(dest[i])
	// 	// }

	// 	// err := rows.scanPlans[i].Scan(values[i], dst)
	// 	// if err != nil {
	// 	// 	err = ScanArgError{ColumnIndex: i, Err: err}
	// 	// 	rows.fatal(err)
	// 	// 	return err
	// 	// }
	// }
	return nil
}

func (r *Rows) fatal(err error) {
	r.selectStmt.lastErr = err
	r.Close()
}

func (r *Rows) Err() error {
	return r.selectStmt.Err()
}

func (r *Rows) Close() {
	r.selectStmt.Close()
}
