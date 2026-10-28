package testdata

import chtypes "github.com/vahid-sohrabloo/chconn/v3/types"

// AliasModel imports chconn's types under another name.
type AliasModel struct {
	Day32    chtypes.Date32   `db:"day32" chtype:"Date32"`
	At       chtypes.DateTime `db:"at" chtype:"DateTime"`
	NullDay  *chtypes.Date    `db:"null_day" chtype:"Nullable(Date)"`
	ArrayDay []chtypes.Date   `db:"array_day" chtype:"Array(Date)"`
}
