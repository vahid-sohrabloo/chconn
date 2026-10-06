package errcases

type SubOrder struct {
	T TwoElem `db:"t" chtype:"Tuple(b Int8, a String)"`
}
