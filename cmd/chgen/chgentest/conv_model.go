package chgentest

import (
	"strconv"
	"time"
)

// ConvFlag is a named type whose underlying type maps to Int8.
type ConvFlag int8

// ConvCode is a named type whose underlying type maps to LowCardinality(String).
type ConvCode string

// ConvUID is written through its String method.
type ConvUID uint8

func (u ConvUID) String() string { return "uid-" + strconv.Itoa(int(u)) }

func unixTime(sec int64) time.Time { return time.Unix(sec, 0).UTC() }

// ConvModel covers underlying-type casts, chconv and an extra template.
//
//go:generate go tool chgen columns --input conv_model.go --template numrows.tmpl
type ConvModel struct {
	ID      uint64   `db:"id" chtype:"UInt64"`
	Flag    ConvFlag `db:"flag" chtype:"Int8"`
	Code    ConvCode `db:"code" chtype:"LowCardinality(String)"`
	UID     ConvUID  `db:"uid" chtype:"String" chconv:".String"`
	Created int64    `db:"created" chtype:"DateTime" chconv:"unixTime"`
}

// ConvOpt is a generic named type.
type ConvOpt[T any] int8

// ConvAlias is an alias of a basic type.
type ConvAlias = uint16

func codeOf(v int8) ConvCode { return ConvCode(strconv.Itoa(int(v))) }

// CastModel covers basic aliases, generic named types and a chconv result
// that is itself a named type.
type CastModel struct {
	Byte  byte         `db:"byte" chtype:"UInt8"`
	Alias ConvAlias    `db:"alias" chtype:"UInt16"`
	Opt   ConvOpt[int] `db:"opt" chtype:"Int8"`
	Code  int8         `db:"code" chtype:"LowCardinality(String)" chconv:"codeOf"`
}
