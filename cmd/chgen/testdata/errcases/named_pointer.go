package errcases

type PtrInt8 *int8

type NamedPointer struct {
	V PtrInt8 `db:"v" chtype:"Nullable(Int8)"`
}
