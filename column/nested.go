package column

// NewNested create a new nested of Nested(T1,T2,.....,Tn) ClickHouse data type
//
// this is actually an alias for NewTuple(T1,T2,.....,Tn).Array()
func NewNested(columns ...ColumnBasic) *ArrayBase {
	return NewTuple(columns...).Array()
}
