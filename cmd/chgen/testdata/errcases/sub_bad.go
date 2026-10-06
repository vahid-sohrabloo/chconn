package errcases

type SubBad struct {
	T BadElem `db:"t" chtype:"Tuple(x UInt8)"`
}
