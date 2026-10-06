package errcases

type SubTypeMismatch struct {
	T OneElem `db:"t" chtype:"Tuple(x Int8)"`
}
