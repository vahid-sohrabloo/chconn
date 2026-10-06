package errcases

// BadElem has a field that cannot map.
type BadElem struct {
	X string `db:"x" chtype:"UInt8"`
}

// OneElem has a single element x.
type OneElem struct {
	X string `db:"x" chtype:"String"`
}

func multiResult(v int8) (int8, error) { return v, nil }

func toString(v int8) string { return "" }

// TwoElem has elements a and b.
type TwoElem struct {
	A string `db:"a" chtype:"String"`
	B int8   `db:"b" chtype:"Int8"`
}
