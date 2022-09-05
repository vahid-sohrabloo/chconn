package chconn

import (
	"strconv"
	"strings"

	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
)

// Setting is a setting for the clickhouse query.
//
// The list of setting is here: https://clickhouse.com/docs/en/operations/settings/settings/
// Some of settings doesn't have effect. for example `http_zlib_compression_level`
// because chconn use TCP connection to send data not HTTP.
type Setting struct {
	Name, Value                 string
	Important, Custom, Obsolete bool
}

const (
	settingFlagImportant = 0x01
	settingFlagCustom    = 0x02
	settingFlagObsolete  = 0x04
)

// Settings is a list of settings for the clickhouse query.
type Settings []Setting

func (st Setting) write(w *readerwriter.Writer) {
	w.String(st.Name)

	var flag uint8
	if st.Important {
		flag |= settingFlagImportant
	}
	if st.Custom {
		flag |= settingFlagCustom
	}
	if st.Obsolete {
		flag |= settingFlagObsolete
	}
	w.Uint8(flag)

	w.String(st.Value)
}

func (s Settings) write(w *readerwriter.Writer) {
	for _, st := range s {
		st.write(w)
	}
}

// Parameters is a list of params for the clickhouse query.
type parameters struct {
	params []Setting
}

type Parameter func() Setting

func NewParameters(input ...Parameter) *parameters {
	params := make([]Setting, 0, cap(input))
	for _, p := range input {
		params = append(params, p())
	}
	return &parameters{
		params: params,
	}
}

// IntParameter get int query parameter.
func IntParameter(name string, v int64) Parameter {
	return func() Setting {
		return Setting{
			Name:   name,
			Value:  "'" + strconv.FormatInt(v, 10) + "'",
			Custom: true,
		}
	}
}

// UintParameter get uint query parameter.
func UintParameter(name string, v uint64) Parameter {
	return func() Setting {
		return Setting{
			Name:   name,
			Value:  "'" + strconv.FormatUint(v, 10) + "'",
			Custom: true,
		}
	}
}

// UintParameter get uint query parameter.
func StringParameter(name string, v string) Parameter {
	return func() Setting {
		return Setting{
			Name:   name,
			Value:  "'" + strings.ReplaceAll(v, "'", "\\'") + "'",
			Custom: true,
		}
	}
}

func (p *parameters) hasParam() bool {
	return p != nil && len(p.params) > 0
}

func (p *parameters) write(w *readerwriter.Writer) {
	if p == nil {
		return
	}
	for _, st := range p.params {
		st.write(w)
	}
}
