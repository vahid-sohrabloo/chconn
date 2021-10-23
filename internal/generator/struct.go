package generator

import (
	"github.com/dave/jennifer/jen"
	"github.com/vahid-sohrabloo/chconn"
)

func GenerateStruct(f *jen.File, structName string, columns []*chconn.Column) *jen.Statement {
	var fields []jen.Code
	for _, c := range columns {
		field := getFieldByType(c.ChType)
		if field != nil {
			fields = append(fields, jen.Id(getStandardName(c.Name)).Add(field))
		}

	}

	st := f.Type().Id(structName)

	st.Struct(fields...)
	st.Line()

	f.Func().
		Id("New" + structName).
		Params().
		Params(jen.Op("*").Id(structName)).
		Block(
			jen.Return().Op("&").Id(structName).Values(jen.Dict{}),
		).Line()

	return st
}
