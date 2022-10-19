// sqlbuilder is a builder for SQL statements for clickhouse.
// copy from https://github.com/huandu/go-sqlbuilder
// change for chconn
package sqlbuilder

import (
	"bytes"
	"strings"
)

// injection is a helper type to manage injected SQLs in all builders.
type injection struct {
	markerSQLs map[injectionMarker][]string
}

type injectionMarker int

// newInjection creates a new injection.
func newInjection() *injection {
	return &injection{
		markerSQLs: map[injectionMarker][]string{},
	}
}

// SQL adds sql to injection's sql list.
// All sqls inside injection is ordered by marker in ascending order.
func (injection *injection) SQL(marker injectionMarker, sql string) {
	injection.markerSQLs[marker] = append(injection.markerSQLs[marker], sql)
}

// WriteTo joins all SQL strings at the same marker value with blank (" ")
// and writes the joined value to buf.
func (injection *injection) WriteTo(buf *bytes.Buffer, marker injectionMarker) {
	sqls := injection.markerSQLs[marker]
	empty := buf.Len() == 0

	if len(sqls) == 0 {
		return
	}

	if !empty {
		buf.WriteByte(' ')
	}

	s := strings.Join(sqls, " ")
	buf.WriteString(s)

	if empty {
		buf.WriteByte(' ')
	}
}
