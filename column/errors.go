package column

import (
	"fmt"
)

type ErrInvalidType struct {
	column     ColumnBasic
	ColumnType string
}

func (e ErrInvalidType) Error() string {
	return fmt.Sprintf("mismatch column type: ClickHouse Type: %s, column types: %s", string(e.column.Type()), string(e.column.columnType()))
}
