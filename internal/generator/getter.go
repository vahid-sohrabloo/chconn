package generator

import (
	"github.com/dave/jennifer/jen"
	"github.com/vahid-sohrabloo/chconn"
)

func GenerateGetter(f *jen.File, st *jen.Statement, structName string, columns []*chconn.Column) {
	for _, c := range columns {
		field := getFieldByType(c.ChType)
		st.Line()
		st.Func().
			Params(jen.Id("t").Op("*").Id(structName)).
			Id("Get" + getStandardName(c.Name)).Params().Add(field).
			Block(
				jen.Return(jen.Id("t." + getStandardName(c.Name))),
			).Line()
	}
}
