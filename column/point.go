package column

import "github.com/vahid-sohrabloo/chconn/v3/types"

func NewPoint() *Tuple2[types.Point, float64, float64] {
	return NewTuple2[types.Point, float64, float64](New[float64](), New[float64]())
}
