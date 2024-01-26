package column

import "fmt"

type ErrInvalidType struct {
	chType     string
	structType string
}

func (e ErrInvalidType) Error() string {
	return fmt.Sprintf("invalid type: expected clickhouse type '%s' for struct type '%s'", e.chType, e.structType)
}
