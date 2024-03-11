package helper

import "unicode/utf8"

// copy from easyjson
const chars = "0123456789abcdef"

func getTable(falseValues ...int) [128]bool {
	table := [128]bool{}

	for i := 0; i < 128; i++ {
		table[i] = true
	}

	for _, v := range falseValues {
		table[v] = false
	}

	return table
}

var (
	htmlNoEscapeTable = getTable(
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
		'"', '\\')
)

func AppendJSONSting(out []byte, ignoreDoubleQuotes bool, s []byte) []byte {
	if !ignoreDoubleQuotes {
		out = append(out, '"')
	}

	// Portions of the string that contain no escapes are appended as
	// byte slices.

	p := 0 // last non-escape symbol

	escapeTable := &htmlNoEscapeTable

	for i := 0; i < len(s); {
		c := s[i]

		if c < utf8.RuneSelf {
			if escapeTable[c] {
				// single-width character, no escaping is required
				i++
				continue
			}

			out = append(out, s[p:i]...)
			switch c {
			case '\t':
				out = append(out, `\t`...)
			case '\r':
				out = append(out, `\r`...)
			case '\n':
				out = append(out, `\n`...)
			case '\\':
				out = append(out, `\\`...)
			case '"':
				out = append(out, `\"`...)
			default:
				out = append(out, `\u00`...)
				out = append(out, chars[c>>4], chars[c&0xf])
			}

			i++
			p = i
			continue
		}

		// broken utf
		runeValue, runeWidth := utf8.DecodeRune(s[i:])
		if runeValue == utf8.RuneError && runeWidth == 1 {
			out = append(out, s[p:i]...)
			out = append(out, `\ufffd`...)
			i++
			p = i
			continue
		}

		// jsonp stuff - tab separator and line separator
		if runeValue == '\u2028' || runeValue == '\u2029' {
			out = append(out, s[p:i]...)
			out = append(out, `\u202`...)
			out = append(out, chars[runeValue&0xf])
			i += runeWidth
			p = i
			continue
		}
		i += runeWidth
	}
	out = append(out, s[p:]...)
	if !ignoreDoubleQuotes {
		out = append(out, '"')
	}
	return out
}
