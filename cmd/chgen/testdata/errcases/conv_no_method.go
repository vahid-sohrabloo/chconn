package errcases

type ConvNoMethod struct {
	V int8 `db:"v" chtype:"Int8" chconv:".Nope"`
}
