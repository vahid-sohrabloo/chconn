package main

import (
	"log"
	"strings"

	"github.com/dave/jennifer/jen"
)

//nolint:gocritic
func getNewFunc(name, chType string, nullable bool) (jen.Code, string) {
	var columnType string
	nullableStr := "false"
	if nullable {
		nullableStr = "true"
	}
	switch chType {
	case "Int8":
		columnType = "NewInt8(" + nullableStr + ")"
	case "Int16":
		columnType = "NewInt16(" + nullableStr + ")"
	case "Int32":
		columnType = "NewInt32(" + nullableStr + ")"
	case "Int64":
		columnType = "NewInt64(" + nullableStr + ")"
	case "UInt8":
		columnType = "NewUint8(" + nullableStr + ")"
	case "UInt16":
		columnType = "NewUint16(" + nullableStr + ")"
	case "UInt32":
		columnType = "NewUint32(" + nullableStr + ")"
	case "UInt64":
		columnType = "NewUint64(" + nullableStr + ")"
	case "Float64":
		columnType = "NewFloat64(" + nullableStr + ")"
	case "Float32":
		columnType = "NewFloat32(" + nullableStr + ")"
	case "String":
		columnType = "NewString(" + nullableStr + ")"
	case "Date":
		columnType = "NewDate(" + nullableStr + ")"
	case "Date32":
		columnType = "NewDate32(" + nullableStr + ")"
	case "DateTime":
		columnType = "NewDateTime(" + nullableStr + ")"
	case "DateTime64":
		params := strings.Split(chType[len("NewDateTime64("):len(chType)-1], ",")
		columnType = "NewDateTime64(" + params[0] + ", " + nullableStr + ")"
	case "IPv4":
		columnType = "NewIPv4(" + nullableStr + ")"
	case "IPv6":
		columnType = "NewIPv6(" + nullableStr + ")"
	case "UUID":
		columnType = "NewUUID(" + nullableStr + ")"
	default:
		if strings.HasPrefix(chType, "DateTime(") {
			columnType = "NewDateTime(" + nullableStr + ")"
			break
		}
		if strings.HasPrefix(chType, "DateTime64(") {
			params := strings.Split(chType[len("NewDateTime64("):len(chType)-1], ",")
			columnType = "NewDateTime64(" + params[0] + ", " + nullableStr + ")"
			break
		}

		if strings.HasPrefix(chType, "Decimal(9 ,") {
			columnType = "NewDecimal32(" + chType[len("Decimal(9 ,"):len(chType)-1] + ", " + nullableStr + ")"
			break
		}
		if strings.HasPrefix(chType, "Decimal(18 ,") {
			columnType = "NewDecimal64(" + chType[len("Decimal(18 ,"):len(chType)-1] + ", " + nullableStr + ")"
			break
		}
		if strings.HasPrefix(chType, "SimpleAggregateFunction(") {
			return getNewFunc(name, getNestedType(chType[len("SimpleAggregateFunction("):]), nullable)
		}

		if strings.HasPrefix(chType, "Enum8(") {
			columnType = "NewInt8(" + nullableStr + ")"
			break
		}
		if strings.HasPrefix(chType, "Enum16(") {
			columnType = "NewInt16(" + nullableStr + ")"
			break
		}
		if strings.HasPrefix(chType, "Nullable(") {
			return getNewFunc(name, chType[len("Nullable("):len(chType)-1], true)
		}
		if strings.HasPrefix(chType, "FixedString(") {
			columnType = "NewRaw(" + chType[len("FixedString("):len(chType)-1] + ", " + nullableStr + ")"
			break
		}

		if strings.HasPrefix(chType, "Array(") {
			fieldName := "t." + getStandardName(name+"Array")
			return jen.Do(func(s *jen.Statement) {
				subCol, subFieldName := getNewFunc(name, chType[len("Array("):len(chType)-1], nullable)
				s.Add(subCol)
				s.Line()
				s.Id(fieldName).Op("=").Id("column.NewArray(" + subFieldName + ")")
			}), fieldName
		}
		if strings.HasPrefix(chType, "LowCardinality(") {
			fieldName := "t." + getStandardName(name+"LC")
			return jen.Do(func(s *jen.Statement) {
				subCol, subFieldName := getNewFunc(name, chType[len("LowCardinality("):len(chType)-1], nullable)
				s.Add(subCol)
				s.Line()
				s.Id(fieldName).Op("=").Id("column.NewLC(" + subFieldName + ")")
			}), fieldName
		}
		// todo add map tuple uint128 uint256 decimal128 decimal256 map
		panic("unknown type: " + chType)
	}
	fieldName := "t." + getStandardName(name)
	return jen.Id(fieldName).Op("=").Id("column." + columnType), fieldName
}

func generateColumns(packageName, structName string, columns []chColumns) {
	f := jen.NewFile(packageName)
	st := f.Type().Id(structName + "Columns")

	var fields []jen.Code
	var fieldsName []string
	for _, c := range columns {
		getColumnByType(c.Name, c.Type, &fields, &fieldsName)
	}
	st.Struct(fields...).Line()

	var initColumns []jen.Code
	initColumns = append(initColumns, jen.Id("t:=&"+structName+"Columns{}"))
	var mainColumnsField []jen.Code
	readAllColumn := jen.Var().Id("err").Error()
	for i, c := range columns {
		fn, fieldName := getNewFunc(c.Name, c.Type, false)
		initColumns = append(initColumns, fn)
		var field jen.Code
		if i == 0 {
			field = jen.Id(fieldName)
		} else {
			field = jen.Line().Id(fieldName)
		}
		mainColumnsField = append(mainColumnsField, field)

		readAllColumn.Line().Id("err").Op("=").Id("stmt").Op(".").Id("NextColumn").Call(jen.Id(fieldName)).
			Line().If(jen.Err().Op("!=").Nil()).Block(jen.Return(jen.Err()))
	}

	readAllColumn.Line().Return(jen.Nil())

	initColumns = append(initColumns, jen.Return(jen.Id("t")))
	f.Func().
		Id("New" + structName + "Columns").
		Params().
		Params(jen.Op("*").Id(structName + "Columns")).
		Block(
			initColumns...,
		).Line()

	var resetFuncs []jen.Code
	for _, f := range fieldsName {
		resetFuncs = append(resetFuncs, jen.Id("t."+f).Id(".Reset()"))
	}
	st.Func().
		Params(jen.Id("t").Op("*").Id(structName + "Columns")).
		Id("Reset").Params().
		Block(
			resetFuncs...,
		).Line()

	var writeFuncs []jen.Code
	for _, c := range columns {
		fieldName := "m." + getStandardName(c.Name)
		writerName := "t." + getStandardName(c.Name)
		fn := getWriteFunc(fieldName, writerName, c.Name, c.Type)
		writeFuncs = append(writeFuncs, fn)
	}

	st.Line()
	st.Func().
		Params(jen.Id("t").Op("*").Id(structName + "Columns")).
		Id("Write").Params(jen.Id("m *" + structName)).
		Block(
			writeFuncs...,
		).Line()

	st.Line().Func().
		Params(jen.Id("t").Op("*").Id(structName + "Columns")).
		Id("ColumnsForInsert").Params().
		Params(jen.Index().Qual("github.com/vahid-sohrabloo/chconn/column", "Column")).
		Block(
			jen.Return(jen.Index().Id("column.Column").Values(mainColumnsField...)),
		).Line()

	st.Line().Func().
		Params(jen.Id("t").Op("*").Id(structName + "Columns")).
		Id("ReadColumns").Params(
		jen.Id("stmt").Qual("github.com/vahid-sohrabloo/chconn", "SelectStmt"),
	).
		Params(jen.Error()).
		Block(
			jen.Return(jen.Id("stmt").Op(".").Id("ReadColumns").Call(mainColumnsField[1:]...)),
		).Line()

	err := f.Save(strings.ToLower(structName) + "_column.go")
	if err != nil {
		log.Fatal(err)
	}
}

func getWriteFunc(fieldName, writerName, name, chType string) jen.Code {
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
		"Date",
		"Date32",
		"DateTime",
		"DateTime64",
		"UUID",
		"IPv4",
		"IPv6":
		return jen.Id(writerName).Op(".").Id("Append").Call(jen.Id(fieldName))
	case "String":
		return jen.Id(writerName).Op(".").Id("AppendString").Call(jen.Id(fieldName))
	case "Int8P",
		"Int16P",
		"Int32P",
		"Int64P",
		"UInt8P",
		"UInt16P",
		"UInt32P",
		"UInt64P",
		"Float64P",
		"Float32P",
		"DateP",
		"Date32P",
		"DateTimeP",
		"DateTime64P",
		"UUIDP",
		"IPv4P",
		"IPv6P":
		return jen.Id(writerName).Op(".").Id("AppendP").Call(jen.Id(fieldName))
	case "StringP":
		return jen.Id(writerName).Op(".").Id("AppendStringP").Call(jen.Id(fieldName))
	case "Int8LC",
		"Int16LC",
		"Int32LC",
		"Int64LC",
		"UInt8LC",
		"UInt16LC",
		"UInt32LC",
		"UInt64LC",
		"Float64LC",
		"Float32LC",
		"DateLC",
		"Date32LC",
		"DateTimeLC",
		"DateTime64LC",
		"UUIDLC",
		"IPv4LC",
		"IPv6LC":
		return jen.Id(writerName).Op(".").Id("AppendDict").Call(jen.Id(fieldName))
	case "StringLC":
		return jen.Id(writerName).Op(".").Id("AppendDict").Call(jen.Id("[]byte(" + fieldName + ")"))
	case "Int8LCP",
		"Int16LCP",
		"Int32LCP",
		"Int64LCP",
		"UInt8LCP",
		"UInt16LCP",
		"UInt32LCP",
		"UInt64LCP",
		"Float64LCP",
		"Float32LCP",
		"DateLCP",
		"Date32LCP",
		"DateTimeLCP",
		"DateTime64LCP",
		"UUIDLCP",
		"IPv4LCP",
		"IPv6LCP":
		return jen.Id(writerName).Op(".").Id("AppendDictP").Call(jen.Id(fieldName))
	case "StringLCP":
		return jen.Id(writerName).Op(".").Id("AppendStringDictP").Call(jen.Id(fieldName))

	default:
		if strings.HasPrefix(chType, "Array(") {
			return jen.Do(func(s *jen.Statement) {
				s.Id(writerName + "Array").Op(".").Id("AppendLen").
					Call(
						jen.Len(jen.Id(fieldName)),
					).Line()

				block := getWriteFunc("f", writerName, name, chType[len("Array("):len(chType)-1])
				s.For(
					jen.Id("_").
						Op(",").
						Id("f").
						Op(":=").
						Range().Id(fieldName),
				).Block(
					block,
				)
				s.Line()
			})
		}
		if strings.HasPrefix(chType, "LowCardinality(") {
			return getWriteFunc(fieldName, writerName, name, chType[len("LowCardinality("):len(chType)-1]+"LC")
		}

		if strings.HasPrefix(chType, "DateTime(") {
			return getWriteFunc(fieldName, writerName, name, chType[len("DateTime("):len(chType)-1])
		}
		if strings.HasPrefix(chType, "DateTime64(") {
			return getWriteFunc(fieldName, writerName, name, chType[len("DateTime64("):len(chType)-1])
		}
		if strings.HasPrefix(chType, "Decimal(9 ,") ||
			strings.HasPrefix(chType, "Decimal(18 ,") ||
			strings.HasPrefix(chType, "Enum8(") ||
			strings.HasPrefix(chType, "Enum16(") {
			return jen.Id(writerName).Op(".").Id("Append").Call(jen.Id(fieldName))
		}
		if strings.HasPrefix(chType, "Nullable(") {
			if strings.HasSuffix(chType, ")LC") {
				return getWriteFunc(fieldName, writerName, name, chType[len("Nullable("):len(chType)-3]+"LCP")
			}
			return getWriteFunc(fieldName, writerName, name, chType[len("Nullable("):len(chType)-1]+"P")
		}
		if strings.HasPrefix(chType, "SimpleAggregateFunction(") {
			return getWriteFunc(fieldName, writerName, name, getNestedType(chType[len("SimpleAggregateFunction("):]))
		}
		if strings.HasPrefix(chType, "FixedString(") {
			if strings.HasSuffix(chType, ")LC") {
				return jen.Id(writerName).Op(".").Id("AppendDict").Call(jen.Id(fieldName))
			}
			if strings.HasSuffix(chType, ")LCP") {
				return jen.Id(writerName).Op(".").Id("AppendDictP").Call(jen.Id(fieldName))
			}
			return jen.Id(writerName).Op(".").Id("Append").Call(jen.Id(fieldName))
		}
	}

	// todo add tuble uint128 uint256 decimal128 decimal256 map

	panic("not support " + chType)
}

func getColumnByType(name, chType string, fields *[]jen.Code, fieldsName *[]string) {
	var columnType string
	switch chType {
	case "Int8":
		columnType = "Int8"
	case "Int16":
		columnType = "Int16"
	case "Int32":
		columnType = "Int32"
	case "Int64":
		columnType = "Int64"
	case "UInt8":
		columnType = "Uint8"
	case "UInt16":
		columnType = "Uint16"
	case "UInt32":
		columnType = "Uint32"
	case "UInt64":
		columnType = "Uint64"
	case "Float64":
		columnType = "Float64"
	case "Float32":
		columnType = "Float32"
	case "String":
		columnType = "String"
	case "Date":
		columnType = "Date"
	case "Date32":
		columnType = "Date32"
	case "DateTime":
		columnType = "DateTime"
	case "DateTime64":
		columnType = "DateTime64"
	case "IPv4":
		columnType = "IPv4"
	case "IPv6":
		columnType = "IPv6"
	case "UUID":
		columnType = "UUID"
	default:
		if strings.HasPrefix(chType, "DateTime(") {
			columnType = "DateTime"
			break
		}
		if strings.HasPrefix(chType, "DateTime64(") {
			columnType = "DateTime64"
			break
		}

		if strings.HasPrefix(chType, "Decimal(9 ,") {
			columnType = "Decimal32"
			break
		}
		if strings.HasPrefix(chType, "Decimal(18 ,") {
			columnType = "Decimal64"
			break
		}
		if strings.HasPrefix(chType, "SimpleAggregateFunction(") {
			getColumnByType(name, getNestedType(chType[len("SimpleAggregateFunction("):]), fields, fieldsName)
			return
		}

		if strings.HasPrefix(chType, "Enum8(") {
			columnType = "Int8"
			break
		}
		if strings.HasPrefix(chType, "Enum16(") {
			columnType = "Int16"
			break
		}
		if strings.HasPrefix(chType, "Nullable(") {
			getColumnByType(name, chType[len("Nullable("):len(chType)-1], fields, fieldsName)
			return
		}
		if strings.HasPrefix(chType, "FixedString(") {
			columnType = "Raw"
			break
		}

		if strings.HasPrefix(chType, "Array(") {
			fieldName := getStandardName(name + "Array")
			*fieldsName = append(*fieldsName, fieldName)
			*fields = append(*fields, jen.Id(fieldName).
				Op("*").
				Qual("github.com/vahid-sohrabloo/chconn/column", "Array").Comment(name+" column - Array"),
			)
			getColumnByType(name, chType[len("Array("):len(chType)-1], fields, fieldsName)
			return
		}
		if strings.HasPrefix(chType, "LowCardinality(") {
			fieldName := getStandardName(name + "LC")
			*fieldsName = append(*fieldsName, fieldName)
			*fields = append(*fields, jen.Id(fieldName).
				Op("*").
				Qual("github.com/vahid-sohrabloo/chconn/column", "LC").Comment(name+" column - LC"),
			)
			getColumnByType(name, chType[len("LowCardinality("):len(chType)-1], fields, fieldsName)
			return
		}
		// todo add tuple uint128 uint256 decimal128 decimal256 map
		panic("unknown type: " + chType)
	}
	fieldName := getStandardName(name)
	*fieldsName = append(*fieldsName, fieldName)
	*fields = append(*fields,
		jen.Id(fieldName).
			Op("*").
			Qual("github.com/vahid-sohrabloo/chconn/column", columnType).Comment(name+" column"),
	)
}
