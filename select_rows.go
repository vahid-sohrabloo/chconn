package chconn

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// Query sends a select query to the server and returns a Rows to read the results. Only errors encountered sending the query
// and initializing Rows will be returned. Err() on the returned Rows must be checked after the Rows is closed to
// determine if the query executed successfully.
//
// For better performance use Select instead of Query when possible. specially when you want to read al lot of data.
//
// The returned Rows must be closed before the connection can be used again. It is safe to attempt to read from the
// returned Rows even if an error is returned. The error will be the available in rows.Err() after rows are closed. It
// is allowed to ignore the error returned from Query and handle it in Rows.
//
// It is possible for a query to return one or more rows before encountering an error. In most cases the rows should be
// collected before processing rather than processed while receiving each row. This avoids the possibility of the
// application processing rows from a query that the server rejected. The CollectRows function is useful here.
//
// NOTE: Only use this function for select queries (or any other queries that return rows).
func (ch *conn) Query(ctx context.Context, sql string, args ...Parameter) (Rows, error) {
	return ch.QueryWithOption(ctx, sql, nil, args...)
}

// QueryWithOption is the same as Query but with QueryOptions
func (ch *conn) QueryWithOption(ctx context.Context, sql string, queryOption *QueryOptions, args ...Parameter) (Rows, error) {
	rows := &baseRows{}
	if queryOption == nil {
		queryOption = &QueryOptions{}
	}
	queryOption.Parameters = NewParameters(args...)
	stmt, err := ch.SelectWithOption(ctx, sql, queryOption)
	rows.selectStmt = stmt.(*selectStmt)

	return rows, err
}

// QueryRow is a convenience wrapper over Query. Any error that occurs while
// querying is deferred until calling Scan on the returned Row. That Row will
// error with ErrNoRows if no rows are returned.
func (ch *conn) QueryRow(ctx context.Context, sql string, args ...Parameter) Row {
	rows, _ := ch.Query(ctx, sql, args...)
	return (*connRow)(rows.(*baseRows))
}

// QueryRowWithOptions is the same as QueryRow but with QueryOptions
func (ch *conn) QueryRowWithOption(ctx context.Context, sql string, queryOption *QueryOptions, args ...Parameter) Row {
	rows, _ := ch.QueryWithOption(ctx, sql, queryOption, args...)
	return (*connRow)(rows.(*baseRows))
}

// Rows is the result set returned from Conn.Query. Rows must be closed before
// the Conn can be used again. Rows are closed by explicitly calling Close(),
// calling Next() until it returns false, or when a fatal error occurs.
//
// Once a Rows is closed the only methods that may be called are Close() and Err()
//
// Rows is an interface instead of a struct to allow tests to mock Query. However,
// adding a method to an interface is technically a breaking change. Because of this
// the Rows interface is partially excluded from semantic version requirements.
// Methods will not be removed or changed, but new methods may be added.
type Rows interface {
	// Close closes the rows, making the connection ready for use again. It is safe
	// to call Close after rows is already closed.
	Close()

	// Err returns any error that occurred while reading. Err must only be called after the Rows is closed (either by
	// calling Close or by Next returning false). If it is called early it may return nil even if there was an error
	// executing the query.
	Err() error

	// Next prepares the next row for reading. It returns true if there is another
	// row and false if no more rows are available or a fatal error has occurred.
	// It automatically closes rows when all rows are read.
	//
	// Callers should check rows.Err() after rows.Next() returns false to detect
	// whether result-set reading ended prematurely due to an error. See
	// Conn.Query for details.
	//
	// For simpler error handling, consider using the higher-level
	// CollectRows() and ForEachRow() helpers instead.
	Next() bool

	// Scan reads the values from the current row into dest values positionally.
	// dest can include pointers to core types, values implementing the Scanner
	// interface, and nil. nil will skip the value entirely. It is an error to
	// call Scan without first calling Next() and checking that it returned true.
	Scan(dest ...any) error

	// Values returns the decoded row values. As with Scan(), it is an error to
	// call Values without first calling Next() and checking that it returned
	// true.
	Values() []any

	// Columns returns the columns
	Columns() []column.ColumnBasic

	// CurrentRow returns the current row number (start from 0)
	CurrentRow() int

	// Conn returns the underlying Conn on which the query was executed
	Conn() Conn
}

// Row is a convenience wrapper over Rows that is returned by QueryRow.
//
// Row is an interface instead of a struct to allow tests to mock QueryRow. However,
// adding a method to an interface is technically a breaking change. Because of this
// the Row interface is partially excluded from semantic version requirements.
// Methods will not be removed or changed, but new methods may be added.
type Row interface {
	// Scan works the same as Rows. with the following exceptions. If no
	// rows were found it returns ErrNoRows. If multiple rows are returned it
	// ignores all but the first.
	Scan(dest ...any) error
}

type baseRows struct {
	selectStmt *selectStmt
	totalRow   int
	currentRow int
}

func (r *baseRows) Next() bool {
	if r.selectStmt.lastErr != nil {
		return false
	}
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

type ScanArgError struct {
	ColumnIndex int
	Err         error
}

func (e ScanArgError) Error() string {
	return fmt.Sprintf("can't scan into dest[%d]: %v", e.ColumnIndex, e.Err)
}

func (e ScanArgError) Unwrap() error {
	return e.Err
}

func (r *baseRows) Scan(dest ...any) error {
	columns := r.selectStmt.Columns()

	if len(dest) == 1 {
		if rc, ok := dest[0].(RowScanner); ok {
			err := rc.ScanRow(r)
			if err != nil {
				r.fatal(err)
			}
			return err
		}
	}

	if len(columns) != len(dest) {
		err := fmt.Errorf("number of columns must equal number of destinations, got %d and %d", len(columns), len(dest))
		r.fatal(err)
		return err
	}
	for i, dst := range dest {
		if err := columns[i].Scan(r.currentRow, dst); err != nil {
			err := ScanArgError{ColumnIndex: i, Err: err}
			r.fatal(err)
			return err
		}
	}
	return nil
}

func (r *baseRows) Columns() []column.ColumnBasic {
	return r.selectStmt.Columns()
}

func (r *baseRows) Values() []any {
	columns := r.selectStmt.Columns()
	values := make([]any, len(columns))
	for i, c := range columns {
		values[i] = c.RowAny(r.currentRow)
	}
	return values
}

func (r *baseRows) CurrentRow() int {
	return r.currentRow
}

func (r *baseRows) fatal(err error) {
	r.selectStmt.lastErr = err
	r.Close()
}

func (r *baseRows) Err() error {
	return r.selectStmt.Err()
}

func (r *baseRows) Close() {
	r.selectStmt.Close()
}

func (r *baseRows) Conn() Conn {
	return r.selectStmt.conn
}

// RowScanner scans an entire row at a time into the RowScanner.
type RowScanner interface {
	// ScanRows scans the row.
	ScanRow(rows Rows) error
}

// connRow implements the Row interface for Conn.QueryRow.
type connRow baseRows

func (r *connRow) Scan(dest ...any) (err error) {
	rows := (*baseRows)(r)

	if rows.Err() != nil {
		return rows.Err()
	}

	if !rows.Next() {
		if rows.Err() == nil {
			return ErrNoRows
		}
		return rows.Err()
	}
	//nolint:errcheck // it checks the error in rows.Err() line
	rows.Scan(dest...)
	rows.Close()
	return rows.Err()
}

// ForEachRow iterates through rows. For each row it scans into the elements of scans and calls fn. If any row
// fails to scan or fn returns an error the query will be aborted and the error will be returned. Rows will be closed
// when ForEachRow returns.
func ForEachRow(rows Rows, scans []any, fn func() error) error {
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(scans...)
		if err != nil {
			return err
		}

		err = fn()
		if err != nil {
			return err
		}
	}

	return rows.Err()
}

// CollectableRow is the subset of Rows methods that a RowToFunc is allowed to call.
type CollectableRow interface {
	Scan(dest ...any) error
}

// RowToFunc is a function that scans or otherwise converts row to a T.
type RowToFunc[T any] func(row CollectableRow) (T, error)

// AppendRows iterates through rows, calling fn for each row, and appending the results into a slice of T.
func AppendRows[T any, S ~[]T](slice S, rows Rows, fn RowToFunc[T]) (S, error) {
	defer rows.Close()

	for rows.Next() {
		value, err := fn(rows)
		if err != nil {
			return nil, err
		}
		slice = append(slice, value)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return slice, nil
}

// CollectRows iterates through rows, calling fn for each row, and collecting the results into a slice of T.
func CollectRows[T any](rows Rows, fn RowToFunc[T]) ([]T, error) {
	return AppendRows([]T{}, rows, fn)
}

// CollectOneRow calls fn for the first row in rows and returns the result.
// If no rows are found returns an error where errors.Is(ErrNoRows) is true.
// CollectOneRow is to CollectRows as QueryRow is to Query.
func CollectOneRow[T any](rows Rows, fn RowToFunc[T]) (T, error) {
	defer rows.Close()

	var value T
	var err error

	if !rows.Next() {
		//nolint:gocritic
		if err = rows.Err(); err != nil {
			return value, err
		}
		return value, ErrNoRows
	}

	value, err = fn(rows)
	if err != nil {
		return value, err
	}

	rows.Close()
	return value, rows.Err()
}

// CollectExactlyOneRow calls fn for the first row in rows and returns the result.
//   - If no rows are found returns an error where errors.Is(ErrNoRows) is true.
//   - If more than 1 row is found returns an error where errors.Is(ErrTooManyRows) is true.
func CollectExactlyOneRow[T any](rows Rows, fn RowToFunc[T]) (T, error) {
	defer rows.Close()

	var (
		err   error
		value T
	)

	if !rows.Next() {
		//nolint:gocritic
		if err = rows.Err(); err != nil {
			return value, err
		}

		return value, ErrNoRows
	}

	value, err = fn(rows)
	if err != nil {
		return value, err
	}

	if rows.Next() {
		var zero T

		return zero, ErrTooManyRows
	}

	return value, rows.Err()
}

// RowTo returns a T scanned from row.
func RowTo[T any](row CollectableRow) (T, error) {
	var value T
	err := row.Scan(&value)
	return value, err
}

// RowTo returns a the address of a T scanned from row.
func RowToAddrOf[T any](row CollectableRow) (*T, error) {
	var value T
	err := row.Scan(&value)
	return &value, err
}

// RowToMap returns a map scanned from row.
func RowToMap(row CollectableRow) (map[string]any, error) {
	var value map[string]any
	err := row.Scan((*mapRowScanner)(&value))
	return value, err
}

type mapRowScanner map[string]any

func (rs *mapRowScanner) ScanRow(rows Rows) error {
	columns := rows.Columns()
	*rs = make(mapRowScanner, len(columns))

	for _, c := range columns {
		(*rs)[string(c.Name())] = c.RowAny(rows.CurrentRow())
	}

	return nil
}

// RowToStructByPos returns a T scanned from row. T must be a struct. T must have the same number a public fields as row
// has fields. The row and T fields will be matched by position. If the "db" struct tag is "-" then the field will be
// ignored.
func RowToStructByPos[T any](row CollectableRow) (T, error) {
	var value T
	err := row.Scan(&positionalStructRowScanner{ptrToStruct: &value})
	return value, err
}

// RowToAddrOfStructByPos returns the address of a T scanned from row. T must be a struct. T must have the same number a
// public fields as row has fields. The row and T fields will be matched by position. If the "db" struct tag is "-" then
// the field will be ignored.
func RowToAddrOfStructByPos[T any](row CollectableRow) (*T, error) {
	var value T
	err := row.Scan(&positionalStructRowScanner{ptrToStruct: &value})
	return &value, err
}

type positionalStructRowScanner struct {
	ptrToStruct any
}

func (rs *positionalStructRowScanner) ScanRow(rows Rows) error {
	dst := rs.ptrToStruct
	dstValue := reflect.ValueOf(dst)
	if dstValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dst not a pointer")
	}

	dstElemValue := dstValue.Elem()
	scanTargets := rs.appendScanTargets(dstElemValue, nil)

	if len(rows.Columns()) > len(scanTargets) {
		return fmt.Errorf("got %d values, but dst struct has only %d fields", len(rows.Columns()), len(scanTargets))
	}

	return rows.Scan(scanTargets...)
}

func (rs *positionalStructRowScanner) appendScanTargets(dstElemValue reflect.Value, scanTargets []any) []any {
	dstElemType := dstElemValue.Type()

	if scanTargets == nil {
		scanTargets = make([]any, 0, dstElemType.NumField())
	}

	for i := 0; i < dstElemType.NumField(); i++ {
		sf := dstElemType.Field(i)
		// Handle anonymous struct embedding, but do not try to handle embedded pointers.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			scanTargets = rs.appendScanTargets(dstElemValue.Field(i), scanTargets)
		} else if sf.PkgPath == "" {
			dbTag, _ := sf.Tag.Lookup(structTagKey)
			if dbTag == "-" {
				// Field is ignored, skip it.
				continue
			}
			scanTargets = append(scanTargets, dstElemValue.Field(i).Addr().Interface())
		}
	}

	return scanTargets
}

// RowToStructByName returns a T scanned from row. T must be a struct. T must have the same number of named public
// fields as row has fields. The row and T fields will be matched by name. The match is case-insensitive. The database
// column name can be overridden with a "db" struct tag. If the "db" struct tag is "-" then the field will be ignored.
func RowToStructByName[T any](row CollectableRow) (T, error) {
	var value T
	err := row.Scan(&namedStructRowScanner{ptrToStruct: &value})
	return value, err
}

// RowToAddrOfStructByName returns the address of a T scanned from row. T must be a struct. T must have the same number
// of named public fields as row has fields. The row and T fields will be matched by name. The match is
// case-insensitive. The database column name can be overridden with a "db" struct tag. If the "db" struct tag is "-"
// then the field will be ignored.
func RowToAddrOfStructByName[T any](row CollectableRow) (*T, error) {
	var value T
	err := row.Scan(&namedStructRowScanner{ptrToStruct: &value})
	return &value, err
}

// RowToStructByNameLax returns a T scanned from row. T must be a struct. T must have greater than or equal number of named public
// fields as row has fields. The row and T fields will be matched by name. The match is case-insensitive. The database
// column name can be overridden with a "db" struct tag. If the "db" struct tag is "-" then the field will be ignored.
func RowToStructByNameLax[T any](row CollectableRow) (T, error) {
	var value T
	err := row.Scan(&namedStructRowScanner{ptrToStruct: &value, lax: true})
	return value, err
}

// RowToAddrOfStructByNameLax returns the address of a T scanned from row. T must be a struct. T must have greater than or
// equal number of named public fields as row has fields. The row and T fields will be matched by name. The match is
// case-insensitive. The database column name can be overridden with a "db" struct tag. If the "db" struct tag is "-"
// then the field will be ignored.
func RowToAddrOfStructByNameLax[T any](row CollectableRow) (*T, error) {
	var value T
	err := row.Scan(&namedStructRowScanner{ptrToStruct: &value, lax: true})
	return &value, err
}

type namedStructRowScanner struct {
	ptrToStruct any
	lax         bool
}

func (rs *namedStructRowScanner) ScanRow(rows Rows) error {
	dst := rs.ptrToStruct
	dstValue := reflect.ValueOf(dst)
	if dstValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dst not a pointer")
	}
	columns := rows.Columns()
	dstElemValue := dstValue.Elem()
	scanTargets, err := rs.appendScanTargets(dstElemValue, nil, columns)
	if err != nil {
		return err
	}

	for i, t := range scanTargets {
		if t == nil {
			return fmt.Errorf("struct doesn't have corresponding row field %s", string(columns[i].Name()))
		}
	}

	return rows.Scan(scanTargets...)
}

const structTagKey = "db"

func fieldPosByName(columns []column.ColumnBasic, field string) (i int) {
	i = -1
	for i, c := range columns {
		if strings.EqualFold(string(c.Name()), field) {
			return i
		}
	}
	return
}

func (rs *namedStructRowScanner) appendScanTargets(
	dstElemValue reflect.Value,
	scanTargets []any,
	columns []column.ColumnBasic,
) ([]any, error) {
	var err error
	dstElemType := dstElemValue.Type()

	if scanTargets == nil {
		scanTargets = make([]any, len(columns))
	}

	for i := 0; i < dstElemType.NumField(); i++ {
		sf := dstElemType.Field(i)
		if sf.PkgPath != "" && !sf.Anonymous {
			// Field is unexported, skip it.
			continue
		}
		// Handle anonymous struct embedding, but do not try to handle embedded pointers.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			scanTargets, err = rs.appendScanTargets(dstElemValue.Field(i), scanTargets, columns)
			if err != nil {
				return nil, err
			}
		} else {
			dbTag, dbTagPresent := sf.Tag.Lookup(structTagKey)
			if dbTagPresent {
				dbTag, _, _ = strings.Cut(dbTag, ",")
			}
			if dbTag == "-" {
				// Field is ignored, skip it.
				continue
			}
			colName := dbTag
			if !dbTagPresent {
				colName = sf.Name
			}
			fpos := fieldPosByName(columns, colName)
			if fpos == -1 {
				if rs.lax {
					continue
				}
				return nil, fmt.Errorf("cannot find field %s in returned row", colName)
			}
			if fpos >= len(scanTargets) && !rs.lax {
				return nil, fmt.Errorf("cannot find field %s in returned row", colName)
			}
			scanTargets[fpos] = dstElemValue.Field(i).Addr().Interface()
		}
	}

	return scanTargets, err
}
