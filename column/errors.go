package column

import (
	"fmt"
)

type ErrInvalidType struct {
	chType     string
	goToChType string
	chconnType string
}

func (e ErrInvalidType) Error() string {
	return fmt.Sprintf("the chconn type '%s' is mapped to ClickHouse type '%s', which does not match the expected ClickHouse type '%s'",
		e.chconnType,
		e.goToChType,
		e.chType)
}

func isInvalidType(err error) bool {
	_, ok := err.(*ErrInvalidType)
	return ok
}

type ErrScanType struct {
	destType   string
	columnType string
}

func (e ErrScanType) Error() string {
	return fmt.Sprintf("cannot scan type '%s' into dest type '%s'", e.columnType, e.destType)
}
