package errcases

type Embedded struct {
	OneElem `db:"one" chtype:"Tuple(x String)"`
}
