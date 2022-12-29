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
		Time:     column.New[uint32](),
		ThreadID: column.New[uint64](),
		Type:     column.New[int8](),
		Name:     column.NewString(),
		Value:    column.New[int64](),
	}
}

func (p ProfileEvent) read(c *conn) error {
	return c.block.readColumnsData(c, true, p.Host, p.Time, p.ThreadID, p.Type, p.Name, p.Value)
}
