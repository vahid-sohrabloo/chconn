package generator

import (
	"strconv"
	"strings"

	"github.com/dave/jennifer/jen"
	"github.com/vahid-sohrabloo/chconn"
)

func GenerateSelect(f *jen.File, st *jen.Statement, structName string, columns []*chconn.Column) {
	var linesSelect []jen.Code

	linesSelect = append(linesSelect,
		jen.Id("rows").Op(":=").
			Make(jen.Index().Op("*").Id(structName), jen.Lit(0)))

	var forSelectBlock []jen.Code
	forSelectBlock = append(forSelectBlock,
		jen.Id("rowsInBlock").Op(":=").Id("int(stmt.RowsInBlock())"),
		jen.For(
			jen.Id("i0").Op(":=").Lit(0),
			jen.Id("i0").Op("<").Id("rowsInBlock"),
			jen.Id("i0").Op("++"),
		).Block(
			jen.Id("rows").Op("=").Append(jen.Id("rows"), jen.Id("New"+(structName)).Call()),
		).Line(),
	)
	for _, c := range columns {
		forSelectBlock = append(forSelectBlock,
			jen.Commentf("column %s (%s)", c.Name, c.ChType),
			jen.Id("_").Op(",").Err().Op("=").Id("stmt").Op(".").Id("NextColumn").Call(),
			jen.If().Err().Op("!=").Nil().Block(
				jen.Return().Nil().Op(",").Err(),
			),
		)
		funcinsert, _ := getSelectFunc("rows[rowOffset+i0].", c.Name, c.ChType, "rowsInBlock", "", 0)

		if funcinsert != nil {
			forSelectBlock = append(forSelectBlock,
				funcinsert,
			)
		}
	}

	forSelectBlock = append(forSelectBlock,
		jen.Id("rowOffset += rowsInBlock"),
	)

	linesSelect = append(linesSelect,
		jen.Var().Err().Error(),
		jen.Var().Id("rowOffset").Int(),
		jen.For(jen.Id("stmt").Op(".").Id("Next").Call()).
			Block(
				forSelectBlock...,
			),
	)

	linesSelect = append(linesSelect, jen.Return(jen.Id("rows"), jen.Nil()))

	st.Line().Comment("read method").Line().Func().
		Id("Read"+(structName)).
		Params(jen.Id("stmt").Qual("github.com/vahid-sohrabloo/chconn", "SelectStmt")).
		Params(jen.Index().Op("*").Id(structName), jen.Error()).
		Block(
			linesSelect...,
		).
		Line()
}

//nolint:funlen
func getSelectFunc(prefixName, name, chType, numRowVar, parentType string, level int) (*jen.Statement, int) {
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
		"IPv4",
		"IPv6":
		chType = strings.Replace(chType, "UInt", "Uint", 1)
		return jen.Do(func(s *jen.Statement) {
			getSimpleFor(s, numRowVar, level, jen.Id(prefixName+getStandardName(name)).Op(",").Err().Op("=").
				Id("stmt").Op(".").Id(chType).Call(),
				jen.If().Err().Op("!=").Nil().Block(
					jen.Return().Nil().Op(",").Err(),
				))
		}), level
	default:
		if strings.HasPrefix(chType, "Array(") {
			return jen.Do(func(s *jen.Statement) {
				if parentType != "Array" {
					getArrayHeaderRead(s, name, numRowVar, chType, false, level)
					var st *jen.Statement
					st, chType = getArrayFor(name, numRowVar, chType, prefixName, prefixName+getStandardName(name), false, level)
					s.Add(st)
				}
				// iStr := "i" + strconv.Itoa(level)
				// iStrNew := "i" + strconv.Itoa(level+1)
				// inner, newLevel := getSelectFunc(
				// 	prefixName+getStandardName(name)+"["+iStrNew+"]",
				// 	"",
				// 	chType[6:len(chType)-1],
				// 	"l", "Array", level+1)
				// s.For(
				// 	jen.Id(iStr).
				// 		Op(",").
				// 		Id("l").
				// 		Op(":=").
				// 		Range().Id("len"+getStandardName(name)),
				// ).Block(
				// 	jen.Id(prefixName+getStandardName(name)).Op("=").
				// 		Make(
				// 			jen.Index().Add(getFieldByType(chType[6:len(chType)-1])),
				// 			jen.Id("l"),
				// 		),
				// 	inner,
				// )
				// level = newLevel
			}), level + 1
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
							block, newLevel := getSelectFunc(
								prefixName+getStandardName(name)+".",
								"Field"+strconv.Itoa(idx),
								chType[cur:i+6],
								numRowVar,
								"Tuple",
								level,
							)
							level = newLevel
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

				block, newLevel := getSelectFunc(
					prefixName+getStandardName(name)+".",
					"Field"+strconv.Itoa(idx),
					chType[cur+2:len(chType)-1],
					numRowVar,
					"Tuple",
					level,
				)
				level = newLevel
				s.Add(block)
			}), level
		}

		if strings.HasPrefix(chType, "DateTime(") {
			return jen.Do(func(s *jen.Statement) {
				getSimpleFor(s, numRowVar, level, jen.Id(prefixName+getStandardName(name)).Op(",").Err().Op("=").
					Id("stmt").Op(".").Id("DateTime").Call(),
					jen.If().Err().Op("!=").Nil().Block(
						jen.Return().Nil().Op(",").Err(),
					))
			}), level
		}

		if strings.HasPrefix(chType, "DateTime64(") {
			return jen.Do(func(s *jen.Statement) {
				params := strings.Split(chType[11:len(chType)-1], ",")
				getSimpleFor(s, numRowVar, level, jen.Id(prefixName+getStandardName(name)).Op(",").Err().Op("=").
					Id("stmt").Op(".").Id("DateTime64").Call(jen.Id(params[0])),
					jen.If().Err().Op("!=").Nil().Block(
						jen.Return().Nil().Op(",").Err(),
					))
			}), level
		}

		if strings.HasPrefix(chType, "Decimal(9") {
			return jen.Do(func(s *jen.Statement) {
				getSimpleFor(s, numRowVar, level, jen.Id(prefixName+getStandardName(name)).Op(",").Err().Op("=").
					Id("stmt").Op(".").Id("Decimal32").Call(jen.Id(chType[11:len(chType)-1])),
					jen.If().Err().Op("!=").Nil().Block(
						jen.Return().Nil().Op(",").Err(),
					))
			}), level
		}

		if strings.HasPrefix(chType, "Decimal(18") {
			return jen.Do(func(s *jen.Statement) {
				getSimpleFor(s, numRowVar, level, jen.Id(prefixName+getStandardName(name)).Op(",").Err().Op("=").
					Id("stmt").Op(".").Id("Decimal64").Call(jen.Id(chType[12:len(chType)-1])),
					jen.If().Err().Op("!=").Nil().Block(
						jen.Return().Nil().Op(",").Err(),
					))
			}), level
		}

		if strings.HasPrefix(chType, "Enum8(") {
			return jen.Do(func(s *jen.Statement) {
				getSimpleFor(s, numRowVar, level, jen.Id(prefixName+getStandardName(name)).Op(",").Err().Op("=").
					Id("stmt").Op(".").Id("Int8").Call(),
					jen.If().Err().Op("!=").Nil().Block(
						jen.Return().Nil().Op(",").Err(),
					))
			}), level
		}

		if strings.HasPrefix(chType, "Enum16(") {
			return jen.Do(func(s *jen.Statement) {
				getSimpleFor(s, numRowVar, level, jen.Id(prefixName+getStandardName(name)).Op(",").Err().Op("=").
					Id("stmt").Op(".").Id("Int16").Call(),
					jen.If().Err().Op("!=").Nil().Block(
						jen.Return().Nil().Op(",").Err(),
					))
			}), level
		}
		if strings.HasPrefix(chType, "FixedString(") {
			return jen.Do(func(s *jen.Statement) {
				getSimpleFor(s, numRowVar, level, jen.Id(prefixName+getStandardName(name)).Op(",").Err().Op("=").
					Id("stmt").Op(".").Id("FixedString").Call(jen.Id(chType[12:len(chType)-1])),
					jen.If().Err().Op("!=").Nil().Block(
						jen.Return().Nil().Op(",").Err(),
					))
			}), level
		}

		if strings.HasPrefix(chType, "SimpleAggregateFunction(") {
			return getSelectFunc(
				prefixName,
				getStandardName(name),
				getNestedType(chType[24:]),
				numRowVar,
				"SimpleAggregateFunction",
				level,
			)
		}

		if strings.HasPrefix(chType, "LowCardinality(") {
			chLowCardinalityType := chType[15 : len(chType)-1]
			return jen.Do(func(s *jen.Statement) {
				chLowType := getFieldByType(chLowCardinalityType)
				s.Id("low"+getStandardName(name)).Op(":=").
					Make(
						jen.Index().Add(chLowType),
						jen.Lit(0),
						jen.Id(numRowVar),
					).Line()
				getSimpleFor(s, numRowVar, level, jen.Id(prefixName+getStandardName(name)).Op(",").Err().Op("=").
					Id("stmt").Op(".").Id("FixedString").Call(jen.Id(chLowCardinalityType)),
					jen.If().Err().Op("!=").Nil().Block(
						jen.Return().Nil().Op(",").Err(),
					))
			}), level
		}
	}
	panic("not support " + chType)
}

func getSimpleFor(s *jen.Statement, numRowVar string, level int, inBlock ...jen.Code) {
	iStr := "i" + strconv.Itoa(level)
	s.For(
		jen.Id(iStr).Op(":=").Lit(0),
		jen.Id(iStr).Op("<").Id(numRowVar),
		jen.Id(iStr).Op("++"),
	).Block(
		inBlock...,
	).Line()
}

func getArrayHeaderRead(s *jen.Statement, name, numRowVar, chType string, hasParent bool, level int) {
	lenName := "len" + getStandardName(name) + strconv.Itoa(level)
	s.Comment("get array lens").Line().
		Id(lenName).Op(":=").
		Make(jen.Id("[]int"), jen.Lit(0), jen.Id(numRowVar)).
		Line()
	s.Id("lastOffset,err")
	if hasParent {
		s.Op("=")
	} else {
		s.Op(":=")
	}

	s.Id("stmt.LenS").Call(jen.Uint64().Call(jen.Id(numRowVar)), jen.Id("&"+lenName)).
		Line().
		If().Err().Op("!=").Nil().Block(
		jen.Return().Nil().Op(",").Err(),
	).Line()
	s.Var().Id("index" + getStandardName(name) + strconv.Itoa(level)).Int().Line()
	// ignore error if not need lastOffset
	s.Id("_=lastOffset").Line()
	if strings.HasPrefix(chType[6:len(chType)-1], "Array(") {
		getArrayHeaderRead(s, name, "lastOffset", chType[6:len(chType)-1], true, level+1)
	}
}

func getArrayFor(name, numRowVar, chType, prefixName, prefixNameRead string, hasParent bool, level int) (*jen.Statement, string) {
	iStr := "i" + strconv.Itoa(level)
	lenName := "len" + getStandardName(name) + strconv.Itoa(level)
	iStrNew := "i" + strconv.Itoa(level+1)
	// inner, newLevel := getSelectFunc(
	// 	prefixName+getStandardName(name)+"["+iStrNew+"]",
	// 	"",
	// 	chType[6:len(chType)-1],
	// 	"l", "Array", level+1)
	var inner *jen.Statement
	var retChtype = chType
	if strings.HasPrefix(chType[6:len(chType)-1], "Array(") {
		inner, retChtype = getArrayFor(name, "lastOffset", chType[6:len(chType)-1], prefixName, prefixNameRead+"["+iStrNew+"]", true, level+1)
	} else {
		inner, _ = getSelectFunc(
			prefixNameRead+"["+iStrNew+"]",
			"",
			chType[6:len(chType)-1],
			"l", "Array", level+1)
	}
	var st *jen.Statement
	if hasParent {
		st = jen.For(
			jen.Id(iStr).
				Op(":=").
				Range().Id(prefixName+getStandardName(name)),
		).Block(
			jen.Id("l").Op("=").Id(lenName+"[l]"),
			jen.Id(prefixName+getStandardName(name)).Op("=").
				Make(
					jen.Index().Add(getFieldByType(chType[6:len(chType)-1])),
					jen.Id("l"),
				),
			inner,
		)
	} else {
		st = jen.For(
			jen.Id(iStr).
				Op(",").
				Id("l").
				Op(":=").
				Range().Id(lenName),
		).Block(
			jen.Id(prefixName+getStandardName(name)).Op("=").
				Make(
					jen.Index().Add(getFieldByType(chType[6:len(chType)-1])),
					jen.Id("l"),
				),
			inner,
		)
	}

	return st, retChtype
}
