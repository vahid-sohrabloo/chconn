package generator

import (
	"strings"

	"github.com/dave/jennifer/jen"
	"github.com/vahid-sohrabloo/chconn"
)

func GenerateConsts(f *jen.File, structName string, columns []*chconn.Column) {

	for _, c := range columns {
		if strings.HasPrefix(c.ChType, "Enum8(") || strings.HasPrefix(c.ChType, "Enum16(") {
			startIndex := 7
			if strings.HasPrefix(c.ChType, "Enum8(") {
				startIndex = 6
			}
			enums := strings.Split(c.ChType[startIndex:len(c.ChType)-1], ", ")
			values := make([]jen.Code, len(enums))
			for i, e := range enums {
				e = strings.ReplaceAll(e, "'", "")
				parts := strings.Split(e, " = ")
				values[i] = jen.Id(getStandardName(structName) + getStandardName(c.Name) + getStandardName(parts[0]))
				if strings.HasPrefix(c.ChType, "Enum8(") {
					values[i].(*jen.Statement).Int8()
				} else {
					values[i].(*jen.Statement).Int16()
				}
				values[i].(*jen.Statement).Op("=").Id(parts[1])
			}

			f.Const().Defs(values...)
		}
	}
}
