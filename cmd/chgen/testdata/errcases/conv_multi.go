package errcases

type ConvMulti struct {
	V int8 `db:"v" chtype:"Int8" chconv:"multiResult"`
}
