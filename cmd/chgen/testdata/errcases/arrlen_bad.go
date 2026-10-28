package errcases

type ArrLenBad struct {
	V []string `db:"v" chtype:"Array(String)" charrlen:"0"`
}
