package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/dave/jennifer/jen"
	"github.com/vahid-sohrabloo/chconn"
)

//nolint:funlen,gocyclo
func main() {
	getter := flag.Bool("getter", false, "generate gatter for properties")
	selectFuncQuery := flag.Bool("selectFuncQuery", false, "generate function to return select query")
	insertFuncQuery := flag.Bool("insertFuncQuery", false, "generate function to return insert query")
	structName := flag.String("name", "TableNameRow", "struct name")

	flag.Parse()
	var query string

	file := os.Stdin
	fi, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	size := fi.Size()
	if size == 0 {
		query = flag.Arg(0)
	} else {
		queryData, _ := ioutil.ReadAll(file)
		query = string(queryData)
	}

	ctx := context.Background()
	conn, err := chconn.Connect(ctx, "password=salam")
	if err != nil {
		log.Fatal(err)
	}
	// todo check insert or select query
	stmt, err := conn.Insert(ctx, query)
	if err != nil {
		log.Fatal(err)
	}
	block := stmt.GetBlock()

	f := jen.NewFile("main")

	var fields []jen.Code
	var dbFields []string
	for _, c := range block.Columns {
		field := getFieldByType(c.ChType)
		if field != nil {
			fields = append(fields, jen.Id(getStandardName(c.Name)).Add(field))
		}

		dbFields = append(dbFields, c.Name)

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
				values[i] = jen.Id(getStandardName(*structName) + getStandardName(c.Name) + getStandardName(parts[0]))
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

	if *selectFuncQuery {
		f.Func().
			Id("GetSelect" + *structName + "Query").
			Params(jen.Id("tableName").String()).
			Params(jen.String()).Block(
			jen.Return().Qual("fmt", "Sprintf").Call(
				jen.Id("`Select "+strings.Join(dbFields, ",\n")+" FROM %s`"),
				jen.Id("tableName"),
			),
		).Line()
	}
	if *insertFuncQuery {
		f.Func().
			Id("GetInsert" + *structName + "Query").
			Params(jen.Id("tableName").String()).
			Params(jen.String()).Block(
			jen.Return().Qual("fmt", "Sprintf").Call(
				jen.Id("`INSERT INTO %s ("+strings.Join(dbFields, ",\n")+") VALUES `"),
				jen.Id("tableName"),
			),
		).Line()
	}

	st := f.Type().Id(*structName)

	st.Struct(fields...)
	st.Line()
	st.Comment("write method")
	st.Line()

	var linesInsert []jen.Code
	var linesSelect []jen.Code

	linesSelect = append(linesSelect,
		jen.Id("rows").Op(":=").
			Make(jen.Index().Op("*").Id(*structName), jen.Lit(0)))

	linesInsert = append(linesInsert,
		jen.Var().Err().Error(),
		jen.Id("writer").Op(".").Id("AddRow").Call(jen.Lit(1)),
	)
	var offset int
	for _, c := range block.Columns {
		linesInsert = append(linesInsert, jen.Commentf("column %s", c.Name))
		funcinsert, newOffset := getInsertFunc("t.", c.Name, c.ChType, offset)
		offset = newOffset
		if funcinsert != nil {
			linesInsert = append(linesInsert, funcinsert)
		}
	}

	var forSelectBlock []jen.Code
	forSelectBlock = append(forSelectBlock,
		jen.Id("rowsInBlock").Op(":=").Id("int(stmt.RowsInBlock())"),
		jen.For(
			jen.Id("i0").Op(":=").Lit(0),
			jen.Id("i0").Op("<").Id("rowsInBlock"),
			jen.Id("i0").Op("++"),
		).Block(
			jen.Id("rows").Op("=").Append(jen.Id("rows"), jen.Id("New"+(*structName)).Call()),
		).Line(),
	)
	for _, c := range block.Columns {
		forSelectBlock = append(forSelectBlock,
			jen.Commentf("column %s (%s)", c.Name, c.ChType),
			jen.Id("_").Op(",").Err().Op("=").Id("stmt").Op(".").Id("NextColumn").Call(),
			jen.If().Err().Op("!=").Nil().Block(
				jen.Return().Nil().Op(",").Err(),
			),
		)
		funcinsert, _ := getSelectFunc("rows[rowOffset+i0].", c.Name, c.ChType, "rowsInBlock", 0)

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
	f.Func().
		Id("New" + *structName).
		Params().
		Params(jen.Op("*").Id(*structName)).
		Block(
			jen.Return().Op("&").Id(*structName).Values(jen.Dict{}),
		).Line()
	f.Func().
		Id("New" + *structName + "Writer").
		Params().
		Params(jen.Op("*").Qual("github.com/vahid-sorahbloo/chconn", "InsertWriter")).
		Block(
			jen.Return(jen.Qual("github.com/vahid-sorahbloo/chconn", "NewInsertWriter").Call(jen.Lit(offset))),
		).Line()

	linesInsert = append(linesInsert, jen.Return().Nil())
	st.Func().
		Params(jen.Id("t").Op("*").Id(*structName)).
		Id("Write").
		Params(jen.Id("writer").Op("*").Qual("github.com/vahid-sorahbloo/chconn", "InsertWriter")).
		Params(jen.Error()).
		Block(

			linesInsert...,
		).
		Line()

	st.Line().Comment("read method").Line().Func().
		Id("Read"+(*structName)).
		Params(jen.Id("stmt").Qual("github.com/vahid-sorahbloo/chconn", "SelectStmt")).
		Params(jen.Index().Op("*").Id(*structName), jen.Error()).
		Block(
			linesSelect...,
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

//nolint:gocyclo,funlen
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
		if strings.HasPrefix(chType, "Tuple(") {
			var openFunc int
			var fields []jen.Code
			cur := 6
			// for between `Tuple(` and `)`
			idx := 1
			for i, char := range chType[6 : len(chType)-1] {
				if char == ',' {
					if openFunc == 0 {
						fields = append(
							fields,
							jen.Id("Field"+strconv.Itoa(idx)).Add(getFieldByType(chType[cur:i+6])),
						)
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
			fields = append(
				fields,
				jen.Id("Field"+strconv.Itoa(idx)).Add(getFieldByType(chType[cur+2:len(chType)-1])),
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

func getNestedType(chType string) string {
	for i, v := range chType {
		if v == ',' {
			return chType[i+2 : len(chType)-1]
		}
	}
	panic("Cannot found  netsted type of " + chType)
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

//nolint:funlen
func getSelectFunc(prefixName, name, chType, numRowVar string, level int) (*jen.Statement, int) {
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
				s.Comment("get array lens").Line().
					Id("len"+getStandardName(name)).Op(":=").
					Make(jen.Id("[]int"), jen.Lit(0), jen.Id(numRowVar)).
					Line()
				s.Id("lastOffset,err").Op(":=").
					Id("stmt.LenS").Call(jen.Uint64().Call(jen.Id(numRowVar)), jen.Id("&len"+getStandardName(name))).
					Line().
					If().Err().Op("!=").Nil().Block(
					jen.Return().Nil().Op(",").Err(),
				).Line()
				s.Id("_=lastOffset").Line()
				iStr := "i" + strconv.Itoa(level)
				iStrNew := "i" + strconv.Itoa(level+1)
				inner, newLevel := getSelectFunc(
					prefixName+getStandardName(name)+"["+iStrNew+"]",
					"",
					chType[6:len(chType)-1],
					"l", level+1)
				s.For(
					jen.Id(iStr).
						Op(",").
						Id("l").
						Op(":=").
						Range().Id("len"+getStandardName(name)),
				).Block(
					jen.Id(prefixName+getStandardName(name)).Op("=").
						Make(
							jen.Index().Add(getFieldByType(chType[6:len(chType)-1])),
							jen.Id("l"),
						),
					inner,
				)
				level = newLevel
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
			getSelectFunc(
				prefixName,
				getStandardName(name),
				getNestedType(chType[24:]),
				numRowVar,
				level,
			)
		}
	}
	return nil, level
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
			// todo add validate fixed string len
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
