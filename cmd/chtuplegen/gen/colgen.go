package gen

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

var primitiveTypes = map[string]string{
	"string":    "column.StringBase[%s]",
	"bool":      "column.Base[%s]",
	"int8":      "column.Base[%s]",
	"int16":     "column.Base[%s]",
	"int32":     "column.Base[%s]",
	"int64":     "column.Base[%s]",
	"int":       "column.Base[int64]",
	"uint":      "column.Base[%s]",
	"uint8":     "column.Base[%s]",
	"uint16":    "column.Base[%s]",
	"uint32":    "column.Base[%s]",
	"uint64":    "column.Base[%s]",
	"float32":   "column.Base[%s]",
	"float64":   "column.Base[%s]",
	"time.Time": "column.Date[types.DateTime64]",
}

var primitiveTypesNew = map[string]string{
	"string":    "column.NewStringBase[%s]()",
	"bool":      "column.New[%s]()",
	"int8":      "column.New[%s]()",
	"int16":     "column.New[%s]()",
	"int32":     "column.New[%s]()",
	"int64":     "column.New[%s]()",
	"int":       "column.New[int64]()",
	"uint":      "column.New[%s]()",
	"uint8":     "column.New[%s]()",
	"uint16":    "column.New[%s]()",
	"uint32":    "column.New[%s]()",
	"uint64":    "column.New[%s]()",
	"float32":   "column.New[%s]()",
	"float64":   "column.New[%s]()",
	"time.Time": "column.NewDate[types.DateTime64]().SetPrecision(3)",
}

func (g *Generator) isPtr(t reflect.Type) bool {
	if t.Kind() == reflect.Ptr {
		return true
	}
	if t.Kind() == reflect.Slice {
		return g.isPtr(t.Elem())
	}
	return false
}

func (g *Generator) genStructCH(t reflect.Type) error {
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("cannot generate encoder/decoder for %v, not a struct type", t)
	}

	typ := g.functionName("chtuplegen", t)

	fmt.Fprintf(g.out, "type %s struct {\n", typ)
	fmt.Fprintln(g.out, "    *column.Tuple")

	fs, err := getStructFields(t)
	if err != nil {
		return fmt.Errorf("cannot generate encoder for %v: %v", t, err)
	}

	for _, f := range fs {
		if err := g.genStructField(t, f); err != nil {
			return err
		}
	}

	fmt.Fprintln(g.out, "}")

	// we only generate the constructor if the struct is defined in the same package
	if t.PkgPath() == g.pkgPath {
		fmt.Fprintf(g.out, `func (t %[2]s) ChColumns() column.TupleStruct[%[2]s] {
			return new%[1]s();
		}
	
		`, typ, g.getType(t))
	}
	fmt.Fprintf(g.out, "func new%[1]s() *%[1]s {\n", typ)
	fmt.Fprintf(g.out, "  t := &%[1]s{}\n", typ)
	var columnsName []string
	var columnsSetRow []string
	for _, f := range fs {
		fieldName, err := g.genNewField(t, f)
		if fieldName == "" {
			continue
		}
		if err != nil {
			return err
		}
		columnsName = append(columnsName, "t."+fieldName+"Column")
		if g.isPtr(f.Type) {
			columnsSetRow = append(columnsSetRow, fieldName+": t."+fieldName+"Column.RowP(row)")
		} else if f.Type.Kind() == reflect.Int {
			columnsSetRow = append(columnsSetRow, fieldName+": "+f.Type.Name()+"(t."+fieldName+"Column.Row(row))")
		} else {
			columnsSetRow = append(columnsSetRow, fieldName+": t."+fieldName+"Column.Row(row)")
		}
	}
	fmt.Fprintf(g.out, "  t.Tuple = column.NewTuple(\n%s,\n)\n", strings.Join(columnsName, ", \n"))
	fmt.Fprintln(g.out, "  return t")
	fmt.Fprintln(g.out, "}")

	fmt.Fprintf(g.out, `func (t *%[1]s) Append(data %[2]s) {`, typ, g.getType(t))
	for _, f := range fs {
		if err := g.getAppend(t, f); err != nil {
			return err
		}
	}
	fmt.Fprintln(g.out, "}")

	fmt.Fprintf(g.out, `func (t *%[1]s) AppendMulti(data ...%[2]s) {
		for _, m := range data {
			t.Append(m)
		}
	}
	`, typ, g.getType(t))

	fmt.Fprintf(g.out, `func (t *%[1]s) Array() *column.Array[%[3]s] {
		return column.NewArray[%[3]s](t)
	}


	func (t *%[1]s) Data() []%[3]s {
		val := make([]%[3]s, t.NumRow())
		for i := 0; i < t.NumRow(); i++ {
			val[i] = t.Row(i)
		}
		return val
	}


	func (t *%[1]s) Read(value []%[3]s) []%[3]s {
		if cap(value)-len(value) >= t.NumRow() {
			value = value[:len(value)+t.NumRow()]
		} else {
			value = append(value, make([]%[3]s, t.NumRow())...)
		}
	
		val := value[len(value)-t.NumRow():]
		for i := 0; i < t.NumRow(); i++ {
				val[i]= t.Row(i)
		}
		return value
	}

	func (t *%[1]s) Row(row int) %[3]s {
		return %[3]s{
			%[2]s,
		}
	}

	`, typ, strings.Join(columnsSetRow, ", \n"), g.getType(t))

	return nil
}

//nolint:gocritic
func (g *Generator) genStructField(t reflect.Type, f reflect.StructField) error {
	tags := parseFieldTags(f)

	if tags.omit {
		return nil
	}

	fmt.Fprintf(g.out, "  %s ", f.Name+"Column")
	if err := g.genStructType(f.Type, f, tags, 0, false); err != nil {
		return err
	}
	return nil
}

//nolint:gocritic
func (g *Generator) genStructType(t reflect.Type, f reflect.StructField, tags fieldTags, arrayLevel int, nullable bool) error {
	kind := t.Kind()

	enc := primitiveTypes[kind.String()]
	if enc == "" {
		enc = primitiveTypes[t.String()]
	}

	if kind == reflect.Array && t.Elem().Name() == "uint8" {
		enc = "column.Base[[" + strconv.Itoa(t.Len()) + "]byte]"
	}

	if strings.Count(enc, "%s") == 1 {
		enc = fmt.Sprintf(enc, t.Name())
	}

	if enc == "" && hasTextMarshaler(t) {
		enc = "column.StringMarshaler[" + t.String() + "]"
	}

	if enc != "" {
		var prefixType string
		if arrayLevel > 0 {
			switch arrayLevel {
			case 1:
				prefixType = "Array"
			case 2:
				prefixType = "Array2"
			case 3:
				prefixType = "Array3"
			default:
				return fmt.Errorf("array level %d not supported", arrayLevel)
			}
			if nullable {
				prefixType += "Nullable"
			}
			typ := g.getType(t)

			if kind == reflect.Array && t.Elem().Name() == "uint8" {
				typ = "[" + strconv.Itoa(t.Len()) + "]byte"
			}

			fmt.Fprintf(g.out, "*column."+prefixType+"["+typ+"]"+"\n")
			return nil
		}

		if nullable {
			fmt.Fprintf(g.out, "*column.BaseNullable["+kind.String()+"]"+"\n")
			return nil
		}

		fmt.Fprintf(g.out, "*"+enc+"\n")
		return nil
	}

	switch t.Kind() {
	case reflect.Slice:

		elem := t.Elem()
		return g.genStructType(elem, f, tags, arrayLevel+1, nullable)

	case reflect.Array:
		return fmt.Errorf("array of %s not supported", t.Name())
	case reflect.Struct:
		g.addType(t)
		var prefixType string
		if arrayLevel > 0 {
			switch arrayLevel {
			case 1:
				prefixType = "Array"
			case 2:
				prefixType = "Array2"
			case 3:
				prefixType = "Array3"
			default:
				return fmt.Errorf("array level %d not supported", arrayLevel)
			}
			if nullable {
				prefixType += "Nullable"
			}
			fmt.Fprintf(g.out, "*column."+prefixType+"["+g.getType(t)+"]"+"\n")
			return nil
		}

		if nullable {
			return fmt.Errorf("%s *%s: struct cannot be nullable", f.Name, t.Name())
		}
		fmt.Fprintf(g.out, "column.Column["+g.getType(t)+"]\n")
		return nil
	case reflect.Ptr:
		elem := t.Elem()
		return g.genStructType(elem, f, tags, arrayLevel, true)

	case reflect.Map:
		var prefixType string
		if arrayLevel > 0 {
			switch arrayLevel {
			case 1:
				prefixType = "Array"
			case 2:
				prefixType = "Array2"
			case 3:
				prefixType = "Array3"
			default:
				return fmt.Errorf("array level %d not supported", arrayLevel)
			}
			if nullable {
				prefixType += "Nullable"
			}
			fmt.Fprintf(g.out, "*column."+prefixType+"[map["+g.getType(t.Key())+"]"+g.getType(t.Elem())+"]"+"\n")
			return nil
		}
		fmt.Fprintf(g.out, "*column.Map["+g.getType(t.Key())+", "+g.getType(t.Elem())+"]\n")
		return nil

	case reflect.Interface:
		return fmt.Errorf("interface of %s not supported", t.Name())

	default:
		return fmt.Errorf("type %s not supported", t.Name())
	}
}

//nolint:gocritic
func (g *Generator) genNewField(t reflect.Type, f reflect.StructField) (string, error) {
	jsonName := g.fieldNamer.GetJSONFieldName(t, f)
	tags := parseFieldTags(f)
	if tags.omit {
		return "", nil
	}
	fieldName := f.Name
	if err := g.genTypeNew(f.Type, "t."+fieldName+"Column", false, tags, jsonName, 0, false); err != nil {
		return "", err
	}

	return fieldName, nil
}

func (g *Generator) genTypeNew(t reflect.Type, in string, withVar bool, tags fieldTags, jsonName string, arrayLevel int, nullable bool) error {
	kind := t.Kind()

	enc := primitiveTypesNew[kind.String()]
	if enc == "" {
		enc = primitiveTypesNew[t.String()]
	}
	if kind == reflect.Array && t.Elem().Name() == "uint8" {
		enc = "column.New[[" + strconv.Itoa(t.Len()) + "]byte]()"
	}

	varDef := ""
	if withVar {
		varDef = "var "
	}

	if strings.Count(enc, "%s") == 1 {
		enc = fmt.Sprintf(enc, t.Name())
	}

	if enc == "" && hasTextMarshaler(t) {
		enc = "column.NewStringMarshaler[" + g.getType(t) + "]()"
	}

	if enc != "" {
		var suffixMethod string
		if nullable {
			suffixMethod = ".Nullable()"
		}
		if arrayLevel > 0 {
			switch arrayLevel {
			case 1:
				suffixMethod += ".Array()"
			case 2:
				suffixMethod += ".Array().Array()"
			case 3:
				suffixMethod += ".Array().Array().Array()"
			default:
				return fmt.Errorf("array level %d not supported", arrayLevel)
			}
		}
		fmt.Fprintf(g.out, varDef+in+"="+enc+suffixMethod+"\n")
		if jsonName != "" {
			fmt.Fprintf(g.out, in+".SetName([]byte(%q))\n", jsonName)
		}
		return nil
	}

	switch t.Kind() {
	case reflect.Slice:
		elem := t.Elem()
		return g.genTypeNew(elem, in, withVar, tags, jsonName, arrayLevel+1, nullable)

	case reflect.Array:
		return fmt.Errorf("array not supported")

	case reflect.Struct:
		var suffixMethod string
		if nullable {
			suffixMethod = ".Nullable()"
		}
		if arrayLevel > 0 {
			switch arrayLevel {
			case 1:
				suffixMethod += ".Array()"
			case 2:
				suffixMethod += ".Array().Array()"
			case 3:
				suffixMethod += ".Array().Array().Array()"
			default:
				return fmt.Errorf("array level %d not supported", arrayLevel)
			}
		}
		tmpVar := g.uniqueVarName()
		// we only generate the constructor if the struct is defined in the same package
		// TODO check if implemented ChColumns
		if t.PkgPath() == g.pkgPath {
			fmt.Fprintf(g.out, "var "+tmpVar+" "+g.getType(t)+"\n")
			fmt.Fprintf(g.out, varDef+in+" = "+tmpVar+".ChColumns()"+suffixMethod+"\n")
		} else {
			fmt.Fprintf(g.out, varDef+in+" = new"+g.functionName("chtuplegen", t)+"()"+suffixMethod+"\n")
		}
		if jsonName != "" {
			fmt.Fprintf(g.out, in+".SetName([]byte(%q))\n", jsonName)
		}
		return nil

	case reflect.Ptr:
		elem := t.Elem()
		return g.genTypeNew(elem, in, withVar, tags, jsonName, arrayLevel, true)

	case reflect.Map:
		keyVar := g.uniqueVarName()
		if err := g.genTypeNew(t.Key(), keyVar, true, tags, "", 0, false); err != nil {
			return err
		}

		valVar := g.uniqueVarName()
		if err := g.genTypeNew(t.Elem(), valVar, true, tags, "", 0, false); err != nil {
			return err
		}

		var suffixMethod string
		if nullable {
			suffixMethod = ".Nullable()"
		}
		if arrayLevel > 0 {
			switch arrayLevel {
			case 1:
				suffixMethod += ".Array()"
			case 2:
				suffixMethod += ".Array().Array()"
			case 3:
				suffixMethod += ".Array().Array().Array()"
			default:
				return fmt.Errorf("array level %d not supported", arrayLevel)
			}
		}
		fmt.Fprintf(g.out, varDef+in+
			" = column.NewMap["+g.getType(t.Key())+", "+g.getType(t.Elem())+"]("+keyVar+", "+valVar+")"+suffixMethod+"\n")
		if jsonName != "" {
			fmt.Fprintf(g.out, in+".SetName([]byte(%q))\n", jsonName)
		}
		return nil

	case reflect.Interface:
		panic(t.String())

	default:
		return fmt.Errorf("don't know how to encode %v", t)
	}
}

//nolint:gocritic
func (g *Generator) getAppend(t reflect.Type, f reflect.StructField) error {
	tags := parseFieldTags(f)

	if tags.omit {
		return nil
	}
	fieldName := f.Name
	if err := g.genTypeAppend(f.Type, fieldName, tags, 0); err != nil {
		return err
	}

	return nil
}

// returns true if the type t implements text marshaler interfaces
func hasTextMarshaler(t reflect.Type) bool {
	t = reflect.PointerTo(t)
	return t.Implements(reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem())
}

func (g *Generator) genTypeAppend(t reflect.Type, in string, tags fieldTags, arrayLevel int) error {
	kind := t.Kind()

	enc := primitiveTypesNew[kind.String()]
	if enc == "" {
		enc = primitiveTypesNew[t.String()]
	}

	if kind == reflect.Array && t.Elem().Name() == "uint8" {
		enc = "column.New[[" + strconv.Itoa(t.Len()) + "]byte]()"
	}

	if strings.Count(enc, "%s") == 1 {
		enc = fmt.Sprintf(enc, t.Name())
	}

	if enc != "" {
		if kind.String() == "int" {
			fmt.Fprintf(g.out, "t."+in+"Column.Append(int64(data."+in+"))\n")
			return nil
		}
		fmt.Fprintf(g.out, "t."+in+"Column.Append(data."+in+")\n")
		return nil
	}

	if hasTextMarshaler(t) {
		fmt.Fprintf(g.out, "t."+in+"Column.Append(data."+in+")\n")
		return nil
	}

	switch t.Kind() {
	case reflect.Slice:
		if g.isPtr(t.Elem()) {
			fmt.Fprintf(g.out, "t."+in+"Column.AppendP(data."+in+")\n")
			return nil
		}
		fmt.Fprintf(g.out, "t."+in+"Column.Append(data."+in+")\n")
		return nil

	case reflect.Array:
		return fmt.Errorf("array not supported")

	case reflect.Struct:
		fmt.Fprintf(g.out, "t."+in+"Column.Append(data."+in+")\n")
		return nil

	case reflect.Ptr:
		fmt.Fprintf(g.out, "t."+in+"Column.AppendP(data."+in+")\n")
		return nil

	case reflect.Map:
		fmt.Fprintf(g.out, "t."+in+"Column.Append(data."+in+")\n")
		return nil

	case reflect.Interface:
		return fmt.Errorf("interface not supported")

	default:
		return fmt.Errorf("don't know how to encode %v", t)
	}
}
