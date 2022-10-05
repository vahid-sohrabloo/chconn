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
type Parameters struct {
	params []Setting
}

type Parameter func() Setting

func NewParameters(input ...Parameter) *Parameters {
	params := make([]Setting, len(input))
	for i, p := range input {
		params[i] = p()
	}
	return &Parameters{
		params: params,
	}
}

// IntParameter get int query parameter.
func IntParameter[T ~int | ~int8 | ~int16 | ~int32 | ~int64](name string, v T) Parameter {
	return func() Setting {
		return Setting{
			Name:   name,
			Value:  "'" + strconv.FormatInt(int64(v), 10) + "'",
			Custom: true,
		}
	}
}

// IntSliceParameter get int query parameter.
func IntSliceParameter[T ~int | ~int8 | ~int16 | ~int32 | ~int64](name string, v []T) Parameter {
	return func() Setting {
		var b strings.Builder
		b.WriteString("[")
		for i, v := range v {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(strconv.FormatInt(int64(v), 10))
		}
		b.WriteString("]")
		return Setting{
			Name:   name,
			Value:  "'" + b.String() + "'",
			Custom: true,
		}
	}
}

// UintParameter get uint query parameter.
func UintParameter[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](name string, v T) Parameter {
	return func() Setting {
		return Setting{
			Name:   name,
			Value:  "'" + strconv.FormatUint(uint64(v), 10) + "'",
			Custom: true,
		}
	}
}

// IntParameter get uint slice query parameter.
func UintSliceParameter[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](name string, v []T) Parameter {
	return func() Setting {
		var b strings.Builder
		b.WriteString("[")
		for i, v := range v {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(strconv.FormatUint(uint64(v), 10))
		}
		b.WriteString("]")

		return Setting{
			Name:   name,
			Value:  "'" + b.String() + "'",
			Custom: true,
		}
	}
}

func addSlashes(str string) string {
	var tmpRune []rune
	for _, ch := range str {
		switch ch {
		case '\\', '\'':
			tmpRune = append(tmpRune, '\\', ch)
		default:
			tmpRune = append(tmpRune, ch)
		}
	}
	return string(tmpRune)
}

// StringParameter get string query parameter.
func StringParameter(name, v string) Parameter {
	return func() Setting {
		return Setting{
			Name:   name,
			Value:  "'" + addSlashes(v) + "'",
			Custom: true,
		}
	}
}

// StringSliceParameter get string array query parameter.
func StringSliceParameter(name string, v []string) Parameter {
	return func() Setting {
		var b strings.Builder
		b.WriteString("[")
		for i, v := range v {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("'" + addSlashes(v) + "'")
		}
		b.WriteString("]")
		return Setting{
			Name:   name,
			Value:  "'" + addSlashes(b.String()) + "'",
			Custom: true,
		}
	}
}

func (p *Parameters) Params() []Setting {
	return p.params
}

func (p *Parameters) hasParam() bool {
	return p != nil && len(p.params) > 0
}

func (p *Parameters) write(w *readerwriter.Writer) {
	if p == nil {
		return
	}
	for _, st := range p.params {
		st.write(w)
	}
}
