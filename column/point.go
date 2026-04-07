package column

import "github.com/vahid-sohrabloo/chconn/v3/types"

// NewPoint creates a new column for the ClickHouse Point type (a tuple of two float64 coordinates).
func NewPoint() *Tuple2[types.Point, float64, float64] {
	return NewTuple2[types.Point, float64, float64](New[float64](), New[float64]())
}
