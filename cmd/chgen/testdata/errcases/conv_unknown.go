package errcases

type ConvUnknown struct {
	V int8 `db:"v" chtype:"Int8" chconv:"noSuchFunc"`
}
