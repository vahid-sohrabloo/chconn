package errcases

type SubMissing struct {
	T OneElem `db:"t" chtype:"Tuple(y String)"`
}
