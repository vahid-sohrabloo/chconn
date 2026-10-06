package errcases

func takesInt64(v int64) int8 { return int8(v) }

type ConvArgType struct {
	V int32 `db:"v" chtype:"Int8" chconv:"takesInt64"`
}
