package main

import (
	"log"
	"strconv"
	"strings"

	"github.com/dave/jennifer/jen"
)

func generateModel(packageName, structName string, getter bool, columns []ChColumns) {
	f := jen.NewFile(packageName)

	var fields []jen.Code
	for _, c := range columns {
		field := getFieldByType(c.Type)
		fields = append(fields, jen.Id(getStandardName(c.Name)).Add(field))
	}

	st := f.Type().Id(structName)

	st.Struct(fields...)
	st.Line()

	var defaultValues []jen.Code
	for _, c := range columns {
		if !strings.HasPrefix(c.Type, "FixedString(") && !strings.HasPrefix(c.Type, "LowCardinality(FixedString") {
			continue
		}
		var lenStr string
		if strings.HasPrefix(c.Type, "FixedString(") {
			lenStr = c.Type[len("FixedString(") : len(c.Type)-1]
		} else if strings.HasPrefix(c.Type, "LowCardinality(FixedString") {
			lenStr = c.Type[len("LowCardinality(FixedString(") : len(c.Type)-2]
		}

		fixeSize, err := strconv.Atoi(lenStr)
		if err != nil {
			panic(err)
		}
		defaultValues = append(defaultValues,
			jen.Id(getStandardName(c.Name)).
				Op(":").Index().Byte().Parens(
				jen.Id("\""+strings.Repeat(" ", fixeSize)+"\""),
			),
		)
	}
	f.Func().
		Id("New" + structName).
		Params().
		Params(jen.Op("*").Id(structName)).
		Block(
			jen.Return().Op("&").Id(structName).Values(defaultValues...),
		).Line()

	if getter {
		for _, c := range columns {
			field := getFieldByType(c.Type)
			st.Line()
			st.Func().
				Params(jen.Id("t").Op("*").Id(structName)).
				Id("Get" + getStandardName(c.Name)).Params().Add(field).
				Block(
					jen.Return(jen.Id("t." + getStandardName(c.Name))),
				).Line()
		}
	}
	err := f.Save(strings.ToLower(structName) + "_model.go")
	if err != nil {
		log.Fatal(err)
	}
}

func getFieldByType(chType string) *jen.Statement {
	switch chType {
	case "Int8":
		return jen.Int8()
	case "Int16":
		return jen.Int16()
	case "Int32":
		return jen.Int32()
	case "Int64":
		return jen.Int64()
	case "UInt8":
		return jen.Uint8()
	case "UInt16":
		return jen.Uint16()
	case "UInt32":
		return jen.Uint32()
	case "UInt64":
		return jen.Uint64()
	case "Float64":
		return jen.Float64()
	case "Float32":
		return jen.Float32()
	case "String":
		return jen.String()
	case "DateTime", "DateTime64", "Date32", "Date":
		return jen.Qual("time", "Time")
	case "IPv4":
		return jen.Qual("net", "IP")
	case "IPv6":
		return jen.Qual("net", "IP")
	case "UUID":
		return jen.Index(jen.Lit(16)).Byte()
	default:
		if strings.HasPrefix(chType, "DateTime(") {
			return jen.Qual("time", "Time")
		}
		if strings.HasPrefix(chType, "DateTime64(") {
			return jen.Qual("time", "Time")
		}
		// todo support Decimal Decimal128 and Decimal256
		if strings.HasPrefix(chType, "Decimal(") {
			return jen.Float64()
		}
		if strings.HasPrefix(chType, "LowCardinality(") {
			return getFieldByType(chType[15 : len(chType)-1])
		}

		if strings.HasPrefix(chType, "Enum8(") {
			return jen.Int8()
		}
		if strings.HasPrefix(chType, "Enum16(") {
			return jen.Int16()
		}
		if strings.HasPrefix(chType, "Nullable(") {
			return jen.Op("*").Add(getFieldByType(chType[len("Nullable(") : len(chType)-1]))
		}
		if strings.HasPrefix(chType, "SimpleAggregateFunction(") {
			return getFieldByType(getNestedType(chType[len("SimpleAggregateFunction("):]))
		}
		if strings.HasPrefix(chType, "FixedString(") {
			return jen.Index().Byte()
		}
		if strings.HasPrefix(chType, "Array(") {
			field := getFieldByType(chType[len("Array(") : len(chType)-1])
			if field == nil {
				return nil
			}
			return jen.Index().Add(field)
		}
		if strings.HasPrefix(chType, "Tuple(") {
			var openFunc int
			var fields []jen.Code
			cur := 0
			// for between `Tuple(` and `)`
			tupleTypes := chType[6 : len(chType)-1]
			idx := 1
			for i, char := range tupleTypes {
				if char == ',' {
					if openFunc == 0 {
						fields = append(
							fields,
							jen.Id("Field"+strconv.Itoa(idx)).Add(getFieldByType(tupleTypes[cur:i])),
						)
						idx++
						cur = i + 2
					}
					continue
				}
				if char == '(' {
					openFunc++
					continue
				}
				if char == ')' {
					openFunc--
					continue
				}
			}
			fields = append(
				fields,
				jen.Id("Field"+strconv.Itoa(idx)).Add(getFieldByType(tupleTypes[cur:])),
			)

			return jen.Struct(fields...)
		}
		if strings.HasPrefix(chType, "Array(") {
			field := getFieldByType(chType[6 : len(chType)-1])
			if field == nil {
				return nil
			}
			return jen.Index().Add(field)
		}
	}
	panic("NOT support " + chType)
}
