package chgentest

import "github.com/vahid-sohrabloo/chconn/v3/types"

// StrictModel covers Go types that differ from the column type
// (SetStrict(false)), alone and wrapped, chconn's date types and charrlen.
//
//go:generate go tool chgen columns --input strict_model.go
type StrictModel struct {
	Flag     bool           `db:"flag" chtype:"UInt8"`
	NullFlag *bool          `db:"null_flag" chtype:"Nullable(UInt8)"`
	Flags    []bool         `db:"flags" chtype:"Array(UInt8)"`
	NullAt   *uint32        `db:"null_at" chtype:"Nullable(DateTime)"`
	At       types.DateTime `db:"at" chtype:"DateTime"`
	Day      types.Date     `db:"day" chtype:"Date"`
	Nums     []uint16       `db:"nums" chtype:"Array(UInt16)" charrlen:"2"`
}
