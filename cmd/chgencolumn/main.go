package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"golang.org/x/tools/imports"
)

var (
	file = flag.String("file", "", "input file")
	out  = flag.String("o", "", "output file")
)

// A FileSet is the in-memory representation of a
// parsed file.
type FileSet struct {
	Package    string                     // package name
	Specs      map[string]*ast.StructType // type specs in file
	Directives []string                   // raw preprocessor directives
	Imports    []*ast.ImportSpec          // imports
}

// File parses a file at the relative path
// provided and produces a new *FileSet.
// If you pass in a path to a directory, the entire
// directory will be parsed.
// If unexport is false, only exported identifiers are included in the FileSet.
// If the resulting FileSet would be empty, an error is returned.
func Generate(name, out string, unexported bool) error {
	fs := &FileSet{
		Specs: make(map[string]*ast.StructType),
	}

	fset := token.NewFileSet()

	finfo, err := os.Stat(name)
	if err != nil {
		return err
	}
	if finfo.IsDir() {
		pkgs, err := parser.ParseDir(fset, name, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		if len(pkgs) != 1 {
			return fmt.Errorf("multiple packages in directory: %s", name)
		}
		var one *ast.Package //nolint:staticcheck // ast.Package is needed for file iteration
		for _, pkg := range pkgs {
			one = pkg
			break
		}
		fs.Package = one.Name
		for _, fl := range one.Files {
			if !unexported {
				ast.FileExports(fl)
			}
			fs.getTypeSpecs(fl)
		}
	} else {
		f, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		fs.Package = f.Name.Name
		if !unexported {
			ast.FileExports(f)
		}
		fs.getTypeSpecs(f)
	}

	if len(fs.Specs) == 0 {
		return fmt.Errorf("no definitions in %s", name)
	}

	err = fs.process(newFilename(name, fs.Package))
	if err != nil {
		return err
	}

	return nil
}

// getTypeSpecs extracts all of the *ast.TypeSpecs in the file
// into fs.Identities, but does not set the actual element
func (f *FileSet) getTypeSpecs(fi *ast.File) {
	// collect all imports...
	f.Imports = append(f.Imports, fi.Imports...)

	// check all declarations...
	for i := range fi.Decls {
		// for GenDecls...
		if g, ok := fi.Decls[i].(*ast.GenDecl); ok {
			// and check the specs...
			for _, s := range g.Specs {
				// for ast.TypeSpecs....
				if ts, ok := s.(*ast.TypeSpec); ok {
					switch ts.Type.(type) {
					// this is the list of parse-able
					// type specs
					case *ast.StructType:
						f.Specs[ts.Name.Name] = ts.Type.(*ast.StructType)
					}
				}
			}
		}
	}
}

func (f *FileSet) process(out string) error {
	code := fmt.Sprintf("package %s\n", f.Package)

	code += "import (\n"
	code += fmt.Sprintf("%q\n", "github.com/vahid-sohrabloo/chconn/v3/column")
	for _, imp := range f.Imports {
		code += fmt.Sprintf("%s\n", imp.Path.Value)
	}
	code += ")\n"

	for name, spec := range f.Specs {
		structData := fmt.Sprintf("type %sColumns struct {\n", name)

		structNew := fmt.Sprintf("func New%[1]sColumns() *%[1]sColumns {\n", name)
		structNew += fmt.Sprintf("t := &%sColumns{}\n", name)

		structWriteFunc := fmt.Sprintf("func (t *%sColumns) Write(m *%[1]s) {\n", name)
		structColumnsFunc := fmt.Sprintf("func (t *%sColumns) Columns() []column.ColumnBasic {\n return []column.ColumnBasic{\n", name)

		structSetWriteBufferSize := fmt.Sprintf("func (t *%sColumns) SetWriteBufferSize(row int) {\n", name)

		structResetFunc := fmt.Sprintf("func (t *%sColumns) Reset() {\n", name)

		for _, field := range spec.Fields.List {
			var tag reflect.StructTag
			if field.Tag != nil {
				tag = reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
			}
			typeData, err := f.getFieldChType(field, tag)
			if err != nil {
				return err
			}
			structData += fmt.Sprintf("%s *%s\n", field.Names[0].Name, typeData)
			newField, err := f.getNewFieldChType(field, tag)
			if err != nil {
				return err
			}
			columnName := tag.Get("chname")
			structNew += fmt.Sprintf("t.%s =  %s\n", field.Names[0].Name, newField)
			// todo check all column has chname or ignore it and show warning
			if columnName != "" {
				structNew += fmt.Sprintf("t.%s.SetName([]byte(%q))\n", field.Names[0].Name, columnName)
			}

			structColumnsFunc += fmt.Sprintf("t.%s,\n", field.Names[0].Name)

			structSetWriteBufferSize += fmt.Sprintf("t.%s.SetWriteBufferSize(row)\n", field.Names[0].Name)
			structResetFunc += fmt.Sprintf("t.%s.Reset()\n", field.Names[0].Name)

			appendFn, err := f.getAppendFunc(field, tag)
			if err != nil {
				return err
			}

			structWriteFunc += fmt.Sprintf("%s\n", appendFn)
		}
		structData += "}\n"
		code += structData

		structNew += "return t \n}\n\n"
		code += structNew

		structWriteFunc += "\n}\n\n"
		code += structWriteFunc

		structColumnsFunc += "}\n}\n\n"
		code += structColumnsFunc

		structSetWriteBufferSize += "}\n"
		code += structSetWriteBufferSize

		structResetFunc += "}\n"
		code += structResetFunc
	}

	return format(out, []byte(code))
}

func format(file string, data []byte) error {
	out, err := imports.Process(file, data, nil)
	if err != nil {
		return err
	}
	return os.WriteFile(file, out, 0600)
}

func (f *FileSet) goType(field *ast.Field, chType string) (string, error) {
	return f.goTypeFromAst(field.Type), nil
}

//nolint:funlen,gocyclo
func (f *FileSet) getColumnByChType(field *ast.Field, chType string, arrayLevel int, lowCardinality, nullable bool) (string, error) {
	if strings.HasPrefix(chType, "SimpleAggregateFunction(") {
		chType = string(helper.FilterSimpleAggregate([]byte(chType)))
	}

	chTypeBytes := []byte(chType)
	switch {
	case helper.IsEnum8(chTypeBytes) || helper.IsEnum16(chTypeBytes):
	case chType == "Int8":
	case chType == "Int16":
	case chType == "Int32":
	case chType == "Int64":
	case chType == "Int128":
	case chType == "Int256":
	case chType == "UInt8":
	case chType == "UInt16":
	case chType == "UInt32":
	case chType == "UInt64":
	case chType == "UInt128":
	case chType == "UInt256":
	case chType == "Float32":
	case chType == "Float64":
	case chType == "String":
	case helper.IsFixedString(chTypeBytes):
	case chType == "Date":
	case chType == "Date32":
	case chType == "DateTime" || helper.IsDateTimeWithParam(chTypeBytes):
	case helper.IsDateTime64(chTypeBytes):
	case helper.IsDecimal(chTypeBytes):
	case chType == "UUID":
	case chType == "IPv4":
	case chType == "IPv6":
	case helper.IsNullable(chTypeBytes):
		return f.getColumnByChType(field, chType[helper.LenNullableStr:len(chType)-1], arrayLevel, lowCardinality, true)
	case helper.IsArray(chTypeBytes):
		return f.getColumnByChType(field, chType[helper.LenArrayStr:len(chType)-1], arrayLevel+1, lowCardinality, nullable)
	case helper.IsLowCardinality(chTypeBytes):
		return f.getColumnByChType(field, chType[helper.LenLowCardinalityStr:len(chType)-1], arrayLevel, true, nullable)
	case helper.IsTuple(chTypeBytes):
		panic("todo implement tuple")
	case helper.IsMap(chTypeBytes):
		panic("todo implement tuple")
	default:
		return "", fmt.Errorf("unknown type %s", chType)
	}

	goType, err := f.goType(field, chType)
	if err != nil {
		return "", err
	}
	if goType != "" {
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
				return "", fmt.Errorf("array level %d not supported", arrayLevel)
			}
			if nullable {
				prefixType += "Nullable"
			}
			return "column." + prefixType + "[" + goType + "]", nil
		}
		if lowCardinality {
			if nullable {
				return "column.LowCardinalityNullable[" + goType + "]", nil
			}
			return "column.LowCardinality[" + goType + "]", nil
		}

		if nullable {
			return "column.Nullable[" + goType + "]", nil
		}

		if goType == "string" {
			return "column.String", nil
		}

		return "column.Base[" + goType + "]", nil
	}

	return "", fmt.Errorf("unknown type: %s", chType)
}

//nolint:funlen,gocyclo
func (f *FileSet) getNewColumnByChType(field *ast.Field, chType string, arrayLevel int, lowCardinality, nullable bool) (string, error) {
	if strings.HasPrefix(chType, "SimpleAggregateFunction(") {
		chType = string(helper.FilterSimpleAggregate([]byte(chType)))
	}

	chTypeBytes := []byte(chType)
	switch {
	case helper.IsEnum8(chTypeBytes) || helper.IsEnum16(chTypeBytes):
	case chType == "Int8":
	case chType == "Int16":
	case chType == "Int32":
	case chType == "Int64":
	case chType == "Int128":
	case chType == "Int256":
	case chType == "UInt8":
	case chType == "UInt16":
	case chType == "UInt32":
	case chType == "UInt64":
	case chType == "UInt128":
	case chType == "UInt256":
	case chType == "Float32":
	case chType == "Float64":
	case chType == "String":
	case helper.IsFixedString(chTypeBytes):
	case chType == "Date":
	case chType == "Date32":
	case chType == "DateTime" || helper.IsDateTimeWithParam(chTypeBytes):
	case helper.IsDateTime64(chTypeBytes):
	case helper.IsDecimal(chTypeBytes):
	case chType == "UUID":
	case chType == "IPv4":
	case chType == "IPv6":
	case helper.IsNullable(chTypeBytes):
		return f.getNewColumnByChType(field, chType[helper.LenNullableStr:len(chType)-1], arrayLevel, lowCardinality, true)
	case helper.IsArray(chTypeBytes):
		return f.getNewColumnByChType(field, chType[helper.LenArrayStr:len(chType)-1], arrayLevel+1, lowCardinality, nullable)
	case helper.IsLowCardinality(chTypeBytes):
		return f.getNewColumnByChType(field, chType[helper.LenLowCardinalityStr:len(chType)-1], arrayLevel, true, nullable)
	case helper.IsTuple(chTypeBytes):
		panic("todo implement tuple")
	case helper.IsMap(chTypeBytes):
		panic("todo implement tuple")
	default:
		return "", fmt.Errorf("unknown type %s", chType)
	}

	goType, err := f.goType(field, chType)
	if err != nil {
		return "", err
	}
	suffixMethod := ""

	if nullable {
		suffixMethod += ".Nullable()"
	}

	if lowCardinality {
		suffixMethod += ".LowCardinality()"
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
			return "", fmt.Errorf("array level %d not supported", arrayLevel)
		}
	}
	if goType == "string" {
		return "column.NewString()" + suffixMethod, nil
	}

	return "column.New[" + goType + "]()" + suffixMethod, nil
}

func (f *FileSet) getFieldChType(field *ast.Field, tag reflect.StructTag) (string, error) {
	var chType string
	if tag.Get("chtype") != "" {
		chType = tag.Get("chtype")
	} else {
		var err error
		chType, err = f.chTypeFromAst(field.Type)
		if err != nil {
			return "", err
		}
	}
	// fmt.Printf("field %#v\n", field.Type)
	// return "", fmt.Errorf("field %s has no tag", field.Names[0].Name)

	return f.getColumnByChType(field, chType, 0, false, false)
}

func (f *FileSet) chTypeFromAst(field ast.Expr) (string, error) {
	switch i := field.(type) {
	case *ast.Ident:
		return f.chTypeFromGoType(i.Name)
	case *ast.StarExpr:
		return f.chTypeFromAst(i.X)
	case *ast.ArrayType:
		// todo
		// if i.Len == nil {
		// 	return f.chTypeFromAst(i.Elt)
		// }
		// if lenData, ok := i.Len.(*ast.BasicLit); ok && lenData.Kind == token.INT {
		// 	return "[" + lenData.Value + "]" + f.chTypeFromAst(i.Elt), nil
		// }
		chTyple, err := f.chTypeFromAst(i.Elt)
		if err != nil {
			return "", err
		}
		return "Array(" + chTyple + ")", nil
	case *ast.SelectorExpr:
		fmt.Println(i.X, i.Sel.Name)
		panic(222)
		// todo
		// return f.chTypeFromAst(i.X) + "." + i.Sel.Name, nil
	default:
		panic(fmt.Sprintf("unexpected type: %#v", i))
	}
}

func (f *FileSet) chTypeFromGoType(name string) (string, error) {
	switch name {
	case "int8":
		return "Int8", nil
	case "int16":
		return "Int16", nil
	case "int32":
		return "Int32", nil
	case "int64":
		return "Int64", nil
	case "int128":
		return "Int128", nil
	case "int256":
		return "Int256", nil
	case "uint8":
		return "UInt8", nil
	case "uint16":
		return "UInt16", nil
	case "uint32":
		return "UInt32", nil
	case "uint64":
		return "UInt64", nil
	case "uint128":
		return "UInt128", nil
	case "uint256":
		return "UInt256", nil
	case "float32":
		return "Float32", nil
	case "float64":
		return "Float64", nil
	case "string":
		return "String", nil
	case "time.Time":
		return "DateTime", nil
	default:
		return "", fmt.Errorf("unknown type %s", name)
	}
}

func (f *FileSet) goTypeFromAst(field ast.Expr) string {
	switch i := field.(type) {
	case *ast.Ident:
		return i.Name
	case *ast.StarExpr:
		return f.goTypeFromAst(i.X)
	case *ast.ArrayType:
		if i.Len == nil {
			return f.goTypeFromAst(i.Elt)
		}
		if lenData, ok := i.Len.(*ast.BasicLit); ok && lenData.Kind == token.INT {
			return "[" + lenData.Value + "]" + f.goTypeFromAst(i.Elt)
		}
		panic("invalid array type")
	case *ast.SelectorExpr:
		return f.goTypeFromAst(i.X) + "." + i.Sel.Name
	default:
		panic(fmt.Sprintf("unexpected type: %T", i))
	}
}

func (f *FileSet) isPointer(field ast.Expr) bool {
	switch i := field.(type) {
	case *ast.Ident:
		return false
	case *ast.StarExpr:
		return true
	case *ast.ArrayType:
		return f.isPointer(i.Elt)
	case *ast.SelectorExpr:
		return false
	default:
		panic(fmt.Sprintf("unexpected type: %T", i))
	}
}

func (f *FileSet) getNewFieldChType(field *ast.Field, tag reflect.StructTag) (string, error) {
	var chType string
	if tag.Get("chtype") != "" {
		chType = tag.Get("chtype")
	} else {
		var err error
		chType, err = f.chTypeFromAst(field.Type)
		if err != nil {
			return "", err
		}
	}

	return f.getNewColumnByChType(field, chType, 0, false, false)
	// return "", fmt.Errorf("field %s has no tag", field.Names[0].Name)
}

func (f *FileSet) getAppendFunc(field *ast.Field, tag reflect.StructTag) (string, error) {
	if f.isPointer(field.Type) {
		return fmt.Sprintf("t.%[1]s.AppendP(m.%[1]s)", field.Names[0].Name), nil
	}
	return fmt.Sprintf("t.%[1]s.Append(m.%[1]s)", field.Names[0].Name), nil
}

// picks a new file name based on input flags and input filename(s).
func newFilename(old string, pkg string) string {
	if *out != "" {
		if pre := strings.TrimPrefix(*out, old); len(pre) > 0 &&
			!strings.HasSuffix(*out, ".go") {
			return filepath.Join(old, *out)
		}
		return *out
	}

	if fi, err := os.Stat(old); err == nil && fi.IsDir() {
		old = filepath.Join(old, pkg)
	}
	// new file name is old file name + _gen.go
	return strings.TrimSuffix(old, ".go") + "_column_gen.go"
}

func main() {
	flag.Parse()

	err := Generate(*file, *out, false)
	if err != nil {
		log.Fatal(err)
	}
}
