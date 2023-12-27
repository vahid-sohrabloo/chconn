package helper

const (
	TupleStr    = "Tuple("
	LenTupleStr = len(TupleStr)
	PointStr    = "Point"
)

var PointMainTypeStr = []byte("Tuple(Float64, Float64)")

const PolygonStr = "Polygon"

var PolygonMainTypeStr = []byte("Array(Array(Tuple(Float64, Float64)))")

const MultiPolygonStr = "MultiPolygon"

var MultiPolygonMainTypeStr = []byte("Array(Array(Array(Tuple(Float64, Float64))))")

const (
	ArrayStr          = "Array("
	LenArrayStr       = len(ArrayStr)
	ArrayTypeStr      = "Array(<type>)"
	NestedStr         = "Nested("
	LenNestedStr      = len(NestedStr)
	NestedToArrayTube = "Array(Nested("
	RingStr           = "Ring"
)

var RingMainTypeStr = []byte("Array(Tuple(Float64, Float64))")

const (
	Enum8Str              = "Enum8("
	Enum8StrLen           = len(Enum8Str)
	Enum16Str             = "Enum16("
	Enum16StrLen          = len(Enum16Str)
	DateTimeStr           = "DateTime("
	DateTimeStrLen        = len(DateTimeStr)
	DateTime64Str         = "DateTime64("
	DateTime64StrLen      = len(DateTime64Str)
	DecimalStr            = "Decimal("
	DecimalStrLen         = len(DecimalStr)
	FixedStringStr        = "FixedString("
	FixedStringStrLen     = len(FixedStringStr)
	SimpleAggregateStr    = "SimpleAggregateFunction("
	SimpleAggregateStrLen = len(SimpleAggregateStr)
)

const (
	LowCardinalityStr             = "LowCardinality("
	LenLowCardinalityStr          = len(LowCardinalityStr)
	LowCardinalityTypeStr         = "LowCardinality(<type>)"
	LowCardinalityNullableStr     = "LowCardinality(Nullable("
	LenLowCardinalityNullableStr  = len(LowCardinalityNullableStr)
	LowCardinalityNullableTypeStr = "LowCardinality(Nullable(<type>))"
)

const (
	MapStr     = "Map("
	LenMapStr  = len(MapStr)
	MapTypeStr = "Map(<key>, <value>)"
)

const (
	NullableStr     = "Nullable("
	LenNullableStr  = len(NullableStr)
	NullableTypeStr = "Nullable(<type>)"
)

const (
	StringStr = "String"
)

const (
	NothingStr = "Nothing"
)
