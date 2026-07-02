package testdata

// TupleAddress is a struct for Tuple sub-columns.
type TupleAddress struct {
	City    string `db:"city" chtype:"String"`
	ZipCode int32  `db:"zip_code" chtype:"Int32"`
}

// TuplePhone is a struct for Nested sub-columns.
type TuplePhone struct {
	Number string `db:"number" chtype:"String"`
	Type   int8   `db:"type" chtype:"Int8"`
}

//go:generate go tool chgen columns
type TupleModel struct {
	Name    string       `db:"name" chtype:"String"`
	Address TupleAddress `db:"address" chtype:"Tuple(city String, zip_code Int32)"`
	Phones  []TuplePhone `db:"phones" chtype:"Nested(number String, type Int8)"`
}
