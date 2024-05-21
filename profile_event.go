package chconn

import (
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

// Profile detail of profile select query
type ProfileEvent struct {
	Host     *column.String
	Time     *column.Base[uint32]
	ThreadID *column.Base[uint64]
	Type     *column.Base[int8]
	Name     *column.String
	Value    *column.Base[int64]
}

func newProfileEvent() *ProfileEvent {
	return &ProfileEvent{
		Host:     column.NewString(),
		Time:     column.New[uint32]().SetStrict(false),
		ThreadID: column.New[uint64]().SetStrict(false),
		Type:     column.New[int8]().SetStrict(false),
		Name:     column.NewString(),
		Value:    column.New[int64]().SetStrict(false),
	}
}

func (p ProfileEvent) read(ch *conn) error {
	return ch.block.readColumnsData(true, true, p.Host, p.Time, p.ThreadID, p.Type, p.Name, p.Value)
}
