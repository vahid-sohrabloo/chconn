package generator

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dave/jennifer/jen"
	"github.com/vahid-sohrabloo/chconn"
)

func GenerateWriter(f *jen.File, st *jen.Statement, structName string, columns []*chconn.Column) {
	st.Comment("write method")
	st.Line()

	var linesInsert []jen.Code

	linesInsert = append(linesInsert,
		jen.Var().Err().Error(),
		jen.Id("writer").Op(".").Id("AddRow").Call(jen.Lit(1)),
	)

	var offset int
	for _, c := range columns {
		linesInsert = append(linesInsert, jen.Commentf("column %s", c.Name))
		funcinsert, newOffset := getInsertFunc("t.", c.Name, c.ChType, offset)
		offset = newOffset
		if funcinsert != nil {
			linesInsert = append(linesInsert, funcinsert)
		}
	}

	f.Func().
		Id("New" + structName + "Writer").
		Params().
		Params(jen.Op("*").Qual("github.com/vahid-sohrabloo/chconn", "InsertWriter")).
		Block(
			jen.Return(jen.Qual("github.com/vahid-sohrabloo/chconn", "NewInsertWriter").Call(jen.Lit(offset))),
		).Line()

	linesInsert = append(linesInsert, jen.Return().Nil())
	st.Func().
		Params(jen.Id("t").Op("*").Id(structName)).
		Id("Write").
		Params(jen.Id("writer").Op("*").Qual("github.com/vahid-sohrabloo/chconn", "InsertWriter")).
		Params(jen.Error()).
		Block(

			linesInsert...,
		).
		Line()
}

//nolint:funlen,gocyclo
func getInsertFunc(prefixName, name, chType string, offset int) (jencode *jen.Statement, newOffset int) {
	defaultName := prefixName + getStandardName(name)
	switch chType {
	case "Int8",
		"Int16",
		"Int32",
		"Int64",
		"UInt8",
		"UInt16",
		"UInt32",
		"UInt64",
		"Float64",
		"Float32",
		"String",
		"DateTime",
		"Date",
		"UUID",
		"Int8P",
		"Int16P",
		"Int32P",
		"Int64P",
		"UInt8P",
		"UInt16P",
		"UInt32P",
		"UInt64P",
		"Float64P",
		"Float32P",
		"StringP",
		"DateTimeP",
		"DateP":
		chType = strings.Replace(chType, "UInt", "Uint", 1)
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id(chType).Call(jen.Lit(offset), jen.Id(defaultName))
			s.Line()
		}), offset + 1
	case
		"IPv4",
		"IPv6":
		return jen.Do(func(s *jen.Statement) {
			s.Err().Op("=").Id("writer").Op(".").Id(chType).Call(jen.Lit(offset), jen.Id(defaultName)).
				Line()
			s.If().Err().Op("!=").Nil().Block(
				jen.Return().Err(),
			)
			s.Line()
		}), offset + 1
	case "LowCardinality(String)":
		return jen.Do(func(s *jen.Statement) {
			s.Id("writer").Op(".").Id("AddStringLowCardinality").Call(jen.Lit(offset), jen.Id(defaultName))
			s.Line()
		}), offset + 1
	default:
		if strings.HasPrefix(chType, "Array(") {
			return jen.Do(func(s *jen.Statement) {
				s.Id("writer").Op(".").Id("AddLen").
					Call(
						jen.Lit(offset),
						jen.Id("uint64").
							Call(jen.Len(jen.Id(defaultName))),
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
		if strings.HasPrefix(chType, "Tuple(") {
			return jen.Do(func(s *jen.Statement) {
				var openFunc int
				cur := 6
				// for between `Tuple(` and `)`
				idx := 1
				for i, char := range chType[6 : len(chType)-1] {
					if char == ',' {
						if openFunc == 0 {
							block, newOffset := getInsertFunc(defaultName+".", "Field"+strconv.Itoa(idx), chType[cur:i+6], offset)
							offset = newOffset
							s.Add(block)
							idx++
							cur = i + 6
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

				block, newOffset := getInsertFunc(defaultName+".", "Field"+strconv.Itoa(idx), chType[cur+2:len(chType)-1], offset)
				offset = newOffset
				s.Add(block)
			}), offset
		}

		if strings.HasPrefix(chType, "DateTime(") {
			return jen.Do(func(s *jen.Statement) {
				s.Id("writer").Op(".").Id("DateTime").Call(jen.Lit(offset), jen.Id(defaultName))
				s.Line()
			}), offset + 1
		}
		if strings.HasPrefix(chType, "DateTime64(") {
			return jen.Do(func(s *jen.Statement) {
				params := strings.Split(chType[11:len(chType)-1], ",")
				s.Id("writer").Op(".").Id("DateTime64").Call(
					jen.Lit(offset),
					jen.Id(params[0]),
					jen.Id(defaultName),
				)
				s.Line()
			}), offset + 1
		}
		if strings.HasPrefix(chType, "Decimal(9") {
			return jen.Do(func(s *jen.Statement) {
				s.Id("writer").Op(".").Id("Decimal32").Call(
					jen.Lit(offset),
					jen.Id(defaultName),
					jen.Id(chType[11:len(chType)-1]),
				)
				s.Line()
			}), offset + 1
		}
		if strings.HasPrefix(chType, "Decimal(18") {
			return jen.Do(func(s *jen.Statement) {
				s.Id("writer").Op(".").Id("Decimal64").Call(
					jen.Lit(offset),
					jen.Id(defaultName),
					jen.Id(chType[12:len(chType)-1]),
				)
				s.Line()
			}), offset + 1
		}
		if strings.HasPrefix(chType, "Enum8(") {
			return jen.Do(func(s *jen.Statement) {
				s.Id("writer").Op(".").Id("Int8").Call(jen.Lit(offset), jen.Id(defaultName))
				s.Line()
			}), offset + 1
		}

		if strings.HasPrefix(chType, "Enum16(") {
			return jen.Do(func(s *jen.Statement) {
				s.Id("writer").Op(".").Id("Int16").Call(jen.Lit(offset), jen.Id(defaultName))
				s.Line()
			}), offset + 1
		}

		if strings.HasPrefix(chType, "FixedString(") {
			return jen.Do(func(s *jen.Statement) {
				s.If().Len(jen.Id(defaultName)).Op("!=").Id(chType[12 : len(chType)-1]).
					Block(
						jen.Return().Qual("fmt", "Errorf").Call(
							jen.Lit(fmt.Sprintf("len of %s should be %s not %%d", defaultName, chType[12:len(chType)-1])),
							jen.Len(jen.Id(defaultName)),
						),
					).Line()
				s.Id("writer").Op(".").Id("FixedString").Call(jen.Lit(offset), jen.Id(defaultName))
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
					s.Id("writer").Op(".").Id("AddFixedStringLowCardinality").Call(jen.Lit(offset), jen.Id(defaultName))
					s.Line()
				}), offset + 1
			}
		}
	}
	panic("not support " + chType)
}
