package types

// Point represents a ClickHouse Point type as a pair of float64 coordinates (x, y).
type Point Tuple2[float64, float64]

// Tuple2 is a generic 2-element tuple, corresponding to ClickHouse Tuple(T1, T2).
type Tuple2[T1, T2 any] struct {
	Col1 T1
	Col2 T2
}

// Tuple3 is a generic 3-element tuple, corresponding to ClickHouse Tuple(T1, T2, T3).
type Tuple3[T1, T2, T3 any] struct {
	Col1 T1
	Col2 T2
	Col3 T3
}

// Tuple4 is a generic 4-element tuple, corresponding to ClickHouse Tuple(T1, T2, T3, T4).
type Tuple4[T1, T2, T3, T4 any] struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
}

// Tuple5 is a generic 5-element tuple, corresponding to ClickHouse Tuple(T1, T2, T3, T4, T5).
type Tuple5[T1, T2, T3, T4, T5 any] struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
	Col5 T5
}
