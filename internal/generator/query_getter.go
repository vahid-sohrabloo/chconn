package generator

import (
	"strings"

	"github.com/dave/jennifer/jen"
	"github.com/vahid-sohrabloo/chconn"
)

func GenerateSelectQueryGetter(f *jen.File, structName string, columns []*chconn.Column) {
	var dbFields []string
	for _, c := range columns {
		dbFields = append(dbFields, c.Name)
	}
	f.Func().
		Id("GetSelect" + structName + "Query").
		Params(jen.Id("tableName").String()).
		Params(jen.String()).Block(
		jen.Return().Qual("fmt", "Sprintf").Call(
			jen.Id("`Select "+strings.Join(dbFields, ",\n")+" FROM %s`"),
			jen.Id("tableName"),
		),
	).Line()
}

func GenerateInsertQueryGetter(f *jen.File, structName string, columns []*chconn.Column) {
	var dbFields []string
	for _, c := range columns {
		dbFields = append(dbFields, c.Name)
	}
	f.Func().
		Id("GetInsert" + structName + "Query").
		Params(jen.Id("tableName").String()).
		Params(jen.String()).Block(
		jen.Return().Qual("fmt", "Sprintf").Call(
			jen.Id("`INSERT INTO %s ("+strings.Join(dbFields, ",\n")+") VALUES `"),
			jen.Id("tableName"),
		),
	).Line()
}
