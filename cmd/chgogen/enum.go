package main

import (
	"log"
	"strings"

	"github.com/dave/jennifer/jen"
)

func generateEnum(packageName, structName string, columns []ChColumns) {
	f := jen.NewFile(packageName)
	var hasEnum bool
	for _, c := range columns {
		if !strings.HasPrefix(c.Type, "Enum8(") && !strings.HasPrefix(c.Type, "Enum16(") {
			continue
		}
		hasEnum = true
		startIndex := len("Enum16(")
		if strings.HasPrefix(c.Type, "Enum8(") {
			startIndex = len("Enum8(")
		}
		enums := strings.Split(c.Type[startIndex:len(c.Type)-1], ", ")
		values := make([]jen.Code, len(enums))
		for i, e := range enums {
			e = strings.ReplaceAll(e, "'", "")
			parts := strings.Split(e, " = ")
			values[i] = jen.Id(getStandardName(structName) + getStandardName(c.Name) + getStandardName(parts[0]))
			if strings.HasPrefix(c.Type, "Enum8(") {
				values[i].(*jen.Statement).Int8()
			} else {
				values[i].(*jen.Statement).Int16()
			}
			values[i].(*jen.Statement).Op("=").Id(parts[1])
		}
		f.Const().Defs(values...)
	}

	if hasEnum {
		err := f.Save(strings.ToLower(structName) + "_enums.go")
		if err != nil {
			log.Fatal(err)
		}
	}
}
