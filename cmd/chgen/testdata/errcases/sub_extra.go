package errcases

type SubExtra struct {
	T OneElem `db:"t" chtype:"Tuple(x String, z Int8)"`
}
