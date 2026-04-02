package chgentest

import (
	"time"

	"github.com/vahid-sohrabloo/chconn/v3/types"
)

// TestModelStatus is the status enum type.
type TestModelStatus int8

const (
	TestModelStatusActive   TestModelStatus = 1
	TestModelStatusInactive TestModelStatus = 2
)

//go:generate go tool chgen columns --input model.go
// TestModel is a representative model with common ClickHouse column types.
type TestModel struct {
	// Primitives
	ID       uint64  `db:"id" chtype:"UInt64"`
	Name     string  `db:"name" chtype:"String"`
	Score    float64 `db:"score" chtype:"Float64"`
	Active   bool    `db:"active" chtype:"Bool"`
	SmallNum int8    `db:"small_num" chtype:"Int8"`

	// LowCardinality
	Category string `db:"category" chtype:"LowCardinality(String)"`

	// Nullable
	NullScore *int64  `db:"null_score" chtype:"Nullable(Int64)"`
	NullName  *string `db:"null_name" chtype:"Nullable(String)"`

	// DateTime as time.Time (native)
	CreatedAt time.Time `db:"created_at" chtype:"DateTime"`

	// DateTime as uint (SetStrict(false))
	UpdatedAt uint32 `db:"updated_at" chtype:"DateTime"`

	// FixedString
	CountryCode [2]byte `db:"country_code" chtype:"FixedString(2)"`

	// LowCardinality FixedString
	LangCode [2]byte `db:"lang_code" chtype:"LowCardinality(FixedString(2))"`

	// Array
	Tags []string `db:"tags" chtype:"Array(String)"`

	// Map
	Metadata map[string]string `db:"metadata" chtype:"Map(String, String)"`

	// Enum
	Status TestModelStatus `db:"status" chtype:"Enum8('active' = 1, 'inactive' = 2)"`

	// UUID
	UUID types.UUID `db:"uuid" chtype:"UUID"`
}
