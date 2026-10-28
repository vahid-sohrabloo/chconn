package errcases

type SubDup struct {
	T DupElem `db:"t" chtype:"Tuple(x String)"`
}

type DupElem struct {
	X string `db:"x" chtype:"String"`
	Y string `db:"x" chtype:"String"`
}
