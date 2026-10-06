package errcases

type ConvMismatch struct {
	V int8 `db:"v" chtype:"Int8" chconv:"toString"`
}
