package sqlbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
)

func TestSelectBuilder(t *testing.T) {
	sb := Select("id", "name", As("COUNT(*)", "t")).Distinct()
	sb.Column("age", "birthday")
	sb.From("user").Final()
	sb.SQL("/* before */")
	sb.ArrayJoin("roles").LeftArrayJoin()
	sb.SQL("/* after */")
	sb.PreWhere("id > 0")
	sb.Where(
		"id > {id: Int32}",
		"name LIKE {name: String}",
	)
	sb.Parameters(chconn.IntParameter("id", 1))
	sb.Parameters(chconn.StringParameter("name", "vahid"))
	sb.Join("contract c",
		"u.id = c.user_id",
		"c.status = {status: Array(Int64)}",
	)
	sb.Parameters(chconn.IntSliceParameter("status", []int64{1, 2, 3}))
	sb.JoinWithOption(RightOuterJoin, "person p",
		"u.id = p.user_id",
		"p.surname  = {surname: String}",
	)
	sb.Parameters(chconn.StringParameter("surname", "sohrabloo"))
	sb.GroupBy("status").Having("status > 0")
	sb.OrderBy("modified_at ASC", "created_at DESC")
	sb.Limit(10).Offset(5)

	s, args := sb.Build()

	assert.Equal(t, "SELECT DISTINCT id, name, COUNT(*) AS t, age, birthday /* before */ FROM user FINAL "+
		"LEFT ARRAY JOIN roles /* after */ "+
		"JOIN contract c ON u.id = c.user_id AND c.status = {status: Array(Int64)} "+
		"RIGHT OUTER JOIN person p ON u.id = p.user_id AND p.surname  = {surname: String} "+
		"PREWHERE id > 0 "+
		"WHERE id > {id: Int32} AND name LIKE {name: String} "+
		"GROUP BY status HAVING status > 0 "+
		"ORDER BY modified_at ASC, created_at DESC "+
		"LIMIT 10 OFFSET 5",
		s,
	)
	require.Len(t, args.Params(), 4)
	assert.Equal(t, "id", args.Params()[0].Name)
	assert.Equal(t, "'1'", args.Params()[0].Value)
	assert.Equal(t, "name", args.Params()[1].Name)
	assert.Equal(t, "'vahid'", args.Params()[1].Value)
	assert.Equal(t, "status", args.Params()[2].Name)
	assert.Equal(t, "'[1,2,3]'", args.Params()[2].Value)
	assert.Equal(t, "surname", args.Params()[3].Name)
	assert.Equal(t, "'sohrabloo'", args.Params()[3].Value)
}
