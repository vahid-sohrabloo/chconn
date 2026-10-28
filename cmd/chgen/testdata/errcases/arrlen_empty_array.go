package errcases

type ArrLenEmptyArray struct {
	V []string `db:"v" chtype:"Array(String)" charrlen:""`
}
