package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/dave/jennifer/jen"
	"github.com/vahid-sohrabloo/chconn"
)

func main() {
	getter := flag.Bool("getter", false, "generate gatter for properties")
	structName := flag.String("name", "TableNameRow", "struct name")

	flag.Parse()
	query := flag.Arg(0)
	ctx := context.Background()
	conn, err := chconn.Connect(ctx, "password=salam")
	if err != nil {
		panic(err)
	}

	stmt, err := conn.Insert(ctx, query)
	if err != nil {
		panic(err)
	}
	block := stmt.GetBlock()

	f := jen.NewFile("main")
	st := f.Type().Id(*structName)

	var fields []jen.Code
	for _, c := range block.Columns {
		field := getFieldByType(c.ChType)
		if field != nil {
			fields = append(fields, jen.Id(getStandardName(c.Name)).Add(field))
		}
	}

	st.Struct(fields...)
	st.Line()
	st.Comment("write method")
	st.Line()

	var lines []jen.Code

	lines = append(lines, jen.Id("writer").Op(".").Id("AddRow").Call(jen.Lit(1)))
	var offset int
	for _, c := range block.Columns {
		lines = append(lines, jen.Commentf("column %s", c.Name))
		funcinsert, newOffset := getInsertFunc("t.", c.Name, c.ChType, offset)
		offset = newOffset
		if funcinsert != nil {
			lines = append(lines, funcinsert)
		}
	}

	f.Func().
		Id("New" + *structName + "Writer").
		Params().
		Params(jen.Op("*").Qual("github.com/vahid-sorahbloo/chconn", "InsertWriter")).
		Block(
			jen.Return(jen.Qual("github.com/vahid-sorahbloo/chconn", "NewInsertWriter").Call(jen.Lit(offset))),
		).Line()

	st.Func().
		Params(jen.Id("t").Op("*").Id(*structName)).
		Id("Write").
		Params(jen.Id("writer").Op("*").Qual("github.com/vahid-sorahbloo/chconn", "InsertWriter")).
		Block(
			lines...,
		).
		Line()

	if *getter {
		for _, c := range block.Columns {
			field := getFieldByType(c.ChType)
			st.Line()
			st.Func().
				Params(jen.Id("t").Op("*").Id(*structName)).
				Id("Get" + getStandardName(c.Name)).Params().Add(field).
				Block(
					jen.Return(jen.Id("t." + getStandardName(c.Name))),
				).Line()
		}
	}
	fmt.Printf("%#v", f)
}

func snakeCaseToCamelCase(inputUnderScoreStr string) (camelCase string) {
	isToUpper := false

	for k, v := range inputUnderScoreStr {
		if k == 0 {
			camelCase = strings.ToUpper(string(inputUnderScoreStr[0]))
		} else {
			if isToUpper {
				camelCase += strings.ToUpper(string(v))
				isToUpper = false
			} else {
				if v == '_' {
					isToUpper = true
				} else {
					camelCase += string(v)
				}
			}
		}
	}
	return
}

func getStandardName(name string) string {
	if name == "f" {
		return "f"
	}
	return snakeCaseToCamelCase(strings.ReplaceAll(name, ".", "_"))
}

//nolint:gocyclo
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
	case "DateTime":
		return jen.Qual("time", "Time")
	case "Date":
		return jen.Qual("time", "Time")
	default:
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
			return jen.Op("*").Add(getFieldByType(chType[9 : len(chType)-1]))
		}
		if strings.HasPrefix(chType, "SimpleAggregateFunction(") {
			return getFieldByType(getNestedType(chType[24:]))
		}
		if strings.HasPrefix(chType, "FixedString(") {
			return jen.Index().Byte()
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

func getNestedType(chType string) string {
	for i, v := range chType {
		if v == ',' {
			return chType[i+2 : len(chType)-1]
		}
	}
	panic("Cannot found  netsted type of " + chType)
}

//nolint:funlen,gocyclo
func getInsertFunc(prefixName, name, chType string, offset int) (jencode *jen.Statement, newOffset int) {
	switch chType {
	case "Int8":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Int8").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Int16":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Int16").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Int32":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Int32").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Int64":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Int64").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "UInt8":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Uint8").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "UInt16":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Uint16").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "UInt32":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Uint32").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "UInt64":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Uint64").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Float64":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Float64").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Float32":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Float32").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "String":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("String").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "DateTime":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("DateTime").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Date":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Date").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Int8P":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Int8P").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Int16P":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Int16P").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Int32P":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Int32P").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Int64P":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Int64P").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "UInt8P":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Uint8P").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "UInt16P":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Uint16P").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "UInt32P":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Uint32P").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "UInt64P":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Uint64P").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Float64P":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Float64").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "Float32P":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Float32").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "StringP":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("String").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "DateTimeP":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("DateTime").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "DateP":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("Date").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	case "LowCardinality(String)":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("AddStringLowCardinality").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
			s.Line()
		}), offset + 1
	default:
		if strings.HasPrefix(chType, "Array(") {
			return jen.Do(func(s *jen.Statement) {
				s.Id("writer").Op(".").Id("AddLen").
					Call(
						jen.Lit(offset),
						jen.Id("uint64").
							Call(jen.Len(jen.Id(prefixName+getStandardName(name)))),
					)

				s.Line()
				offset++
				s.Line()
				block, newOffset := getInsertFunc("", "f", chType[6:len(chType)-1], offset)
				s.For(
					jen.Id("_").
						Op(",").
						Id("f").
						Op(":=").
						Range().Id(prefixName + getStandardName(name)),
				).Block(
					block,
				)
				s.Line()
				offset = newOffset
			}), offset
		}

		if strings.HasPrefix(chType, "Enum8(") {
			return jen.Do(func(s *jen.Statement) {
				s.Id("writer").Op(".").Id("Int8").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
				s.Line()
			}), offset + 1
		}

		if strings.HasPrefix(chType, "Enum16(") {
			return jen.Do(func(s *jen.Statement) {
				s.Id("writer").Op(".").Id("Int16").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
				s.Line()
			}), offset + 1
		}
		if strings.HasPrefix(chType, "FixedString(") {
			return jen.Do(func(s *jen.Statement) {
				s.Id("writer").Op(".").Id("FixedString").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
				s.Line()
			}), offset + 1
		}

		if strings.HasPrefix(chType, "Nullable(") {
			block, newOffset := getInsertFunc(prefixName, name, chType[9:len(chType)-1]+"P", offset)
			return block, newOffset + 1
		}

		if strings.HasPrefix(chType, "SimpleAggregateFunction(") {
			block, newOffset := getInsertFunc(prefixName, name, getNestedType(chType[24:]), offset)
			return block, newOffset
		}
		if strings.HasPrefix(chType, "LowCardinality(") {
			chLowCardinalityType := chType[15 : len(chType)-1]
			if len(chLowCardinalityType) >= 12 && chLowCardinalityType[:12] == "FixedString(" {
				return jen.Do(func(s *jen.Statement) {
					s.Id("writer").Op(".").Id("AddFixedStringLowCardinality").Call(jen.Lit(offset), jen.Id(prefixName+getStandardName(name)))
					s.Line()
				}), offset + 1
			}
		}
	}
	panic("not support " + chType)
}
