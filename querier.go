package chconn

import "context"

// Querier is the common query interface satisfied by both Conn and chpool.Pool.
// It allows generic functions like QueryIter, QueryAll, QueryOne, and QueryExactlyOne
// to work with both direct connections and connection pools.
type Querier interface {
	Query(ctx context.Context, sql string, args ...Parameter) (Rows, error)
	QueryWithOption(ctx context.Context, sql string, opts *QueryOptions, args ...Parameter) (Rows, error)
}
