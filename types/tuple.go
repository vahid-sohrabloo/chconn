package types

type Point Tuple2[float64, float64]

type Tuple2[T1, T2 any] struct {
	Col1 T1
	Col2 T2
}

type Tuple3[T1, T2, T3 any] struct {
	Col1 T1
	Col2 T2
	Col3 T3
}

type Tuple4[T1, T2, T3, T4 any] struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
}

type Tuple5[T1, T2, T3, T4, T5 any] struct {
	Col1 T1
	Col2 T2
	Col3 T3
	Col4 T4
	Col5 T5
}
