// sqlbuilder is a builder for SQL statements for clickhouse.
// copy from https://github.com/huandu/go-sqlbuilder
// change for chconn
package sqlbuilder

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/vahid-sohrabloo/chconn/v2"
)

const (
	selectMarkerInit injectionMarker = iota
	selectMarkerAfterSelect
	selectMarkerAfterFrom
	selectMarkerAfterArrayJoin
	selectMarkerAfterJoin
	selectMarkerAfterPreWhere
	selectMarkerAfterWhere
	selectMarkerAfterGroupBy
	selectMarkerAfterOrderBy
	selectMarkerAfterLimit
	selectMarkerAfterFor
)

// JoinOption is the option in JOIN.
type JoinOption string

// Join options.
const (
	InnerJoin      JoinOption = "INNER"
	LeftJoin       JoinOption = "LEFT"
	LeftOuterJoin  JoinOption = "LEFT OUTER"
	LeftSemiJoin   JoinOption = "LEFT SEMI"
	LeftAntiJoin   JoinOption = "LEFT ANTI"
	RightJoin      JoinOption = "RIGHT"
	RightOuterJoin JoinOption = "RIGHT OUTER"
	RightSemiJoin  JoinOption = "RIGHT SEMI"
	RightAntiJoin  JoinOption = "RIGHT ANTI"
	FullJoin       JoinOption = "FULL"
	FullOuterJoin  JoinOption = "FULL OUTER"
	CrossJoin      JoinOption = "CROSS"
)

func NewSelectBuilder() *SelectBuilder {
	return &SelectBuilder{
		limit:     -1,
		offset:    -1,
		injection: newInjection(),
	}
}

// SelectBuilder is a builder to build SELECT.
type SelectBuilder struct {
	parameters    []chconn.Parameter
	distinct      bool
	final         bool
	tables        []string
	selectCols    []string
	leftArrayJoin bool
	arrayJoin     []string
	joinOptions   []JoinOption
	joinTables    []string
	joinExprs     [][]string
	whereExprs    []string
	preWhereExprs []string
	havingExprs   []string
	groupByCols   []string
	orderByCols   []string
	limit         int
	offset        int

	injection *injection
	marker    injectionMarker
}

// var _ Builder = new(SelectBuilder)

// Select sets columns in SELECT.
func Select(col ...string) *SelectBuilder {
	return NewSelectBuilder().Select(col...)
}

// Select sets columns in SELECT.
func (sb *SelectBuilder) Select(col ...string) *SelectBuilder {
	sb.selectCols = col
	sb.marker = selectMarkerAfterSelect
	return sb
}

// Select add columns in SELECT.
func (sb *SelectBuilder) Column(col ...string) *SelectBuilder {
	sb.selectCols = append(sb.selectCols, col...)
	sb.marker = selectMarkerAfterSelect
	return sb
}

// Distinct marks this SELECT as DISTINCT.
func (sb *SelectBuilder) Distinct() *SelectBuilder {
	sb.distinct = true
	sb.marker = selectMarkerAfterSelect
	return sb
}

// Final marks this SELECT as FINAL.
func (sb *SelectBuilder) Final() *SelectBuilder {
	sb.final = true
	sb.marker = selectMarkerAfterSelect
	return sb
}

// From sets table names in SELECT.
func (sb *SelectBuilder) From(table ...string) *SelectBuilder {
	sb.tables = table
	sb.marker = selectMarkerAfterFrom
	return sb
}

// arrayJoin sets expressions of Array Join in SELECT.
//
// It builds a ARRAY JOIN expression like
//
//	Array JOIN onExpr[0], onExpr[1] ...
func (sb *SelectBuilder) ArrayJoin(onExpr ...string) *SelectBuilder {
	sb.marker = selectMarkerAfterArrayJoin
	sb.arrayJoin = append(sb.arrayJoin, onExpr...)
	return sb
}

// LeftArrayJoin marks this SELECT as LEFT ARRAY JOIN.
func (sb *SelectBuilder) LeftArrayJoin() *SelectBuilder {
	sb.leftArrayJoin = true
	return sb
}

// Join sets expressions of JOIN in SELECT.
//
// It builds a JOIN expression like
//
//	JOIN table ON onExpr[0] AND onExpr[1] ...
func (sb *SelectBuilder) Join(table string, onExpr ...string) *SelectBuilder {
	sb.marker = selectMarkerAfterJoin
	return sb.JoinWithOption("", table, onExpr...)
}

// JoinWithOption sets expressions of JOIN with an option.
//
// It builds a JOIN expression like
//
//	option JOIN table ON onExpr[0] AND onExpr[1] ...
//
// Here is a list of supported options.
//   - FullJoin: FULL JOIN
//   - FullOuterJoin: FULL OUTER JOIN
//   - InnerJoin: INNER JOIN
//   - LeftJoin: LEFT JOIN
//   - LeftOuterJoin: LEFT OUTER JOIN
//   - RightJoin: RIGHT JOIN
//   - RightOuterJoin: RIGHT OUTER JOIN
func (sb *SelectBuilder) JoinWithOption(option JoinOption, table string, onExpr ...string) *SelectBuilder {
	sb.joinOptions = append(sb.joinOptions, option)
	sb.joinTables = append(sb.joinTables, table)
	sb.joinExprs = append(sb.joinExprs, onExpr)
	sb.marker = selectMarkerAfterJoin
	return sb
}

// Where sets expressions of WHERE in SELECT.
func (sb *SelectBuilder) Where(andExpr ...string) *SelectBuilder {
	sb.whereExprs = append(sb.whereExprs, andExpr...)
	sb.marker = selectMarkerAfterWhere
	return sb
}

// PreWhere sets expressions of PREWHERE in SELECT.
func (sb *SelectBuilder) PreWhere(andExpr ...string) *SelectBuilder {
	sb.marker = selectMarkerAfterPreWhere
	sb.preWhereExprs = append(sb.preWhereExprs, andExpr...)
	return sb
}

func (sb *SelectBuilder) Parameters(p chconn.Parameter) *SelectBuilder {
	sb.parameters = append(sb.parameters, p)
	return sb
}

// Having sets expressions of HAVING in SELECT.
func (sb *SelectBuilder) Having(andExpr ...string) *SelectBuilder {
	sb.havingExprs = append(sb.havingExprs, andExpr...)
	sb.marker = selectMarkerAfterGroupBy
	return sb
}

// GroupBy sets columns of GROUP BY in SELECT.
func (sb *SelectBuilder) GroupBy(col ...string) *SelectBuilder {
	sb.groupByCols = append(sb.groupByCols, col...)
	sb.marker = selectMarkerAfterGroupBy
	return sb
}

// OrderBy sets columns of ORDER BY in SELECT.
func (sb *SelectBuilder) OrderBy(col ...string) *SelectBuilder {
	sb.orderByCols = append(sb.orderByCols, col...)
	sb.marker = selectMarkerAfterOrderBy
	return sb
}

// Limit sets the LIMIT in SELECT.
func (sb *SelectBuilder) Limit(limit int) *SelectBuilder {
	sb.limit = limit
	sb.marker = selectMarkerAfterLimit
	return sb
}

// Offset sets the LIMIT offset in SELECT.
func (sb *SelectBuilder) Offset(offset int) *SelectBuilder {
	sb.offset = offset
	sb.marker = selectMarkerAfterLimit
	return sb
}

// As returns an AS expression.
func As(name, alias string) string {
	return fmt.Sprintf("%s AS %s", name, alias)
}

// String returns the compiled SELECT string.
func (sb *SelectBuilder) String() string {
	s, _ := sb.Build()
	return s
}

// Build returns compiled SELECT string and args.
// They can be used in `Select` directly.
func (sb *SelectBuilder) Build(initialArg ...interface{}) (sql string, params *chconn.Parameters) {
	buf := &bytes.Buffer{}
	sb.injection.WriteTo(buf, selectMarkerInit)
	buf.WriteString("SELECT ")

	if sb.distinct {
		buf.WriteString("DISTINCT ")
	}

	buf.WriteString(strings.Join(sb.selectCols, ", "))
	sb.injection.WriteTo(buf, selectMarkerAfterSelect)

	buf.WriteString(" FROM ")
	buf.WriteString(strings.Join(sb.tables, ", "))
	sb.injection.WriteTo(buf, selectMarkerAfterFrom)

	if sb.final {
		buf.WriteString(" FINAL")
	}

	if len(sb.arrayJoin) > 0 {
		if sb.leftArrayJoin {
			buf.WriteString(" LEFT")
		}
		buf.WriteString(" ARRAY JOIN ")
		buf.WriteString(strings.Join(sb.arrayJoin, " , "))
		sb.injection.WriteTo(buf, selectMarkerAfterArrayJoin)
	}

	for i := range sb.joinTables {
		if option := sb.joinOptions[i]; option != "" {
			buf.WriteByte(' ')
			buf.WriteString(string(option))
		}

		buf.WriteString(" JOIN ")
		buf.WriteString(sb.joinTables[i])

		if exprs := sb.joinExprs[i]; len(exprs) > 0 {
			buf.WriteString(" ON ")
			buf.WriteString(strings.Join(sb.joinExprs[i], " AND "))
		}
	}

	if len(sb.joinTables) > 0 {
		sb.injection.WriteTo(buf, selectMarkerAfterJoin)
	}

	if len(sb.preWhereExprs) > 0 {
		buf.WriteString(" PREWHERE ")
		buf.WriteString(strings.Join(sb.preWhereExprs, " AND "))
		sb.injection.WriteTo(buf, selectMarkerAfterPreWhere)
	}

	if len(sb.whereExprs) > 0 {
		buf.WriteString(" WHERE ")
		buf.WriteString(strings.Join(sb.whereExprs, " AND "))

		sb.injection.WriteTo(buf, selectMarkerAfterWhere)
	}

	if len(sb.groupByCols) > 0 {
		buf.WriteString(" GROUP BY ")
		buf.WriteString(strings.Join(sb.groupByCols, ", "))

		if len(sb.havingExprs) > 0 {
			buf.WriteString(" HAVING ")
			buf.WriteString(strings.Join(sb.havingExprs, " AND "))
		}

		sb.injection.WriteTo(buf, selectMarkerAfterGroupBy)
	}

	if len(sb.orderByCols) > 0 {
		buf.WriteString(" ORDER BY ")
		buf.WriteString(strings.Join(sb.orderByCols, ", "))

		sb.injection.WriteTo(buf, selectMarkerAfterOrderBy)
	}
	if sb.limit >= 0 {
		buf.WriteString(" LIMIT ")
		buf.WriteString(strconv.Itoa(sb.limit))
	}

	if sb.offset >= 0 {
		buf.WriteString(" OFFSET ")
		buf.WriteString(strconv.Itoa(sb.offset))
	}

	if sb.limit >= 0 {
		sb.injection.WriteTo(buf, selectMarkerAfterLimit)
	}
	return buf.String(), chconn.NewParameters(sb.parameters...)
}

// SQL adds an arbitrary sql to current position.
func (sb *SelectBuilder) SQL(sql string) *SelectBuilder {
	sb.injection.SQL(sb.marker, sql)
	return sb
}
