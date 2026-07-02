package testdata

import "time"

//go:generate go tool chgen columns
type SimpleModel struct {
	ID        uint64    `db:"id" chtype:"UInt64"`
	Name      string    `db:"name" chtype:"LowCardinality(String)"`
	Score     *int64    `db:"score" chtype:"Nullable(Int64)"`
	Tags      []string  `db:"tags" chtype:"Array(LowCardinality(String))"`
	CreatedAt time.Time `db:"created_at" chtype:"DateTime"`
	IgnoreMe  int       // no db tag — skipped
}
