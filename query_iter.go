package chconn

import (
	"context"
	"iter"
)

// QueryIterWithOptionWith returns an iterator that yields T for each row using a custom RowToFunc.
// This is the core implementation — all other QueryIter* variants delegate to this.
func QueryIterWithOptionWith[T any](ctx context.Context, q Querier, fn RowToFunc[T], sql string, opts *QueryOptions, args ...Parameter) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		rows, err := q.QueryWithOption(ctx, sql, opts, args...)
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			value, err := fn(rows)
			if !yield(value, err) {
				return
			}
			if err != nil {
				return
			}
		}
		if err := rows.Err(); err != nil {
			var zero T
			yield(zero, err)
		}
	}
}

// QueryIterWithOption returns an iterator that yields T for each row with QueryOptions.
// T auto-detection: struct -> by name, map[string]any -> map, scalar -> single column.
func QueryIterWithOption[T any](ctx context.Context, q Querier, sql string, opts *QueryOptions, args ...Parameter) iter.Seq2[T, error] {
	return QueryIterWithOptionWith(ctx, q, RowTo[T], sql, opts, args...)
}

// QueryIter returns an iterator that yields T for each row.
// T auto-detection: struct -> by name, map[string]any -> map, scalar -> single column.
func QueryIter[T any](ctx context.Context, q Querier, sql string, args ...Parameter) iter.Seq2[T, error] {
	return QueryIterWithOptionWith(ctx, q, RowTo[T], sql, nil, args...)
}

// QueryIterWith returns an iterator that yields T for each row using a custom RowToFunc.
func QueryIterWith[T any](ctx context.Context, q Querier, fn RowToFunc[T], sql string, args ...Parameter) iter.Seq2[T, error] {
	return QueryIterWithOptionWith(ctx, q, fn, sql, nil, args...)
}

// QueryAllWithOptionWith collects all rows into a slice of T using a custom RowToFunc and QueryOptions.
func QueryAllWithOptionWith[T any](ctx context.Context, q Querier, fn RowToFunc[T], sql string, opts *QueryOptions, args ...Parameter) ([]T, error) {
	var result []T
	for v, err := range QueryIterWithOptionWith(ctx, q, fn, sql, opts, args...) {
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, nil
}

// QueryAll collects all rows into a slice of T.
// T auto-detection: struct -> by name, map[string]any -> map, scalar -> single column.
func QueryAll[T any](ctx context.Context, q Querier, sql string, args ...Parameter) ([]T, error) {
	return QueryAllWithOptionWith(ctx, q, RowTo[T], sql, nil, args...)
}

// QueryAllWithOption collects all rows into a slice of T with QueryOptions.
func QueryAllWithOption[T any](ctx context.Context, q Querier, sql string, opts *QueryOptions, args ...Parameter) ([]T, error) {
	return QueryAllWithOptionWith(ctx, q, RowTo[T], sql, opts, args...)
}

// QueryAllWith collects all rows into a slice of T using a custom RowToFunc.
func QueryAllWith[T any](ctx context.Context, q Querier, fn RowToFunc[T], sql string, args ...Parameter) ([]T, error) {
	return QueryAllWithOptionWith(ctx, q, fn, sql, nil, args...)
}

// QueryOneWithOptionWith returns the first row as T using a custom RowToFunc and QueryOptions.
// Returns ErrNoRows if no rows found.
func QueryOneWithOptionWith[T any](ctx context.Context, q Querier, fn RowToFunc[T], sql string, opts *QueryOptions, args ...Parameter) (T, error) {
	var zero T
	for v, err := range QueryIterWithOptionWith(ctx, q, fn, sql, opts, args...) {
		if err != nil {
			return zero, err
		}
		return v, nil
	}
	return zero, ErrNoRows
}

// QueryOne returns the first row as T. Returns ErrNoRows if no rows found.
// T auto-detection: struct -> by name, map[string]any -> map, scalar -> single column.
func QueryOne[T any](ctx context.Context, q Querier, sql string, args ...Parameter) (T, error) {
	return QueryOneWithOptionWith(ctx, q, RowTo[T], sql, nil, args...)
}

// QueryOneWithOption returns the first row as T with QueryOptions.
func QueryOneWithOption[T any](ctx context.Context, q Querier, sql string, opts *QueryOptions, args ...Parameter) (T, error) {
	return QueryOneWithOptionWith(ctx, q, RowTo[T], sql, opts, args...)
}

// QueryOneWith returns the first row as T using a custom RowToFunc.
func QueryOneWith[T any](ctx context.Context, q Querier, fn RowToFunc[T], sql string, args ...Parameter) (T, error) {
	return QueryOneWithOptionWith(ctx, q, fn, sql, nil, args...)
}

// QueryExactlyOneWithOptionWith returns exactly one row as T using a custom RowToFunc and QueryOptions.
// Returns ErrNoRows if no rows found, ErrTooManyRows if more than one row.
func QueryExactlyOneWithOptionWith[T any](ctx context.Context, q Querier, fn RowToFunc[T], sql string, opts *QueryOptions, args ...Parameter) (T, error) {
	var zero T
	rows, err := q.QueryWithOption(ctx, sql, opts, args...)
	if err != nil {
		return zero, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return zero, err
		}
		return zero, ErrNoRows
	}

	value, err := fn(rows)
	if err != nil {
		return zero, err
	}

	if rows.Next() {
		return zero, ErrTooManyRows
	}

	rows.Close()
	return value, rows.Err()
}

// QueryExactlyOne returns exactly one row as T.
// Returns ErrNoRows if no rows found, ErrTooManyRows if more than one row.
// T auto-detection: struct -> by name, map[string]any -> map, scalar -> single column.
func QueryExactlyOne[T any](ctx context.Context, q Querier, sql string, args ...Parameter) (T, error) {
	return QueryExactlyOneWithOptionWith(ctx, q, RowTo[T], sql, nil, args...)
}

// QueryExactlyOneWithOption returns exactly one row as T with QueryOptions.
func QueryExactlyOneWithOption[T any](ctx context.Context, q Querier, sql string, opts *QueryOptions, args ...Parameter) (T, error) {
	return QueryExactlyOneWithOptionWith(ctx, q, RowTo[T], sql, opts, args...)
}

// QueryExactlyOneWith returns exactly one row as T using a custom RowToFunc.
func QueryExactlyOneWith[T any](ctx context.Context, q Querier, fn RowToFunc[T], sql string, args ...Parameter) (T, error) {
	return QueryExactlyOneWithOptionWith(ctx, q, fn, sql, nil, args...)
}
