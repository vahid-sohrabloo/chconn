package errcases

type DupDB struct {
	A int8 `db:"a" chtype:"Int8"`
	B int8 `db:"a" chtype:"Int8"`
}
