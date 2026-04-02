package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"unicode"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

type columnsConfig struct {
	input    string
	withIter bool
}

func runColumns(args []string) error {
	var cfg columnsConfig
	fs := flag.NewFlagSet("columns", flag.ExitOnError)
	fs.StringVar(&cfg.input, "input", "", "Input Go file (default: $GOFILE)")
	fs.BoolVar(&cfg.withIter, "with-iter", false, "Generate Iter() method")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cfg.input == "" {
		cfg.input = os.Getenv("GOFILE")
	}
	if cfg.input == "" {
		return errors.New("--input is required (or run via go:generate)")
	}

	base := strings.TrimSuffix(cfg.input, filepath.Ext(cfg.input))
	outFile := base + "_columns_gen.go"

	return generateColumns(cfg.input, outFile, cfg.withIter)
}

// fieldInfo describes a single struct field with db/chtype tags.
type fieldInfo struct {
	Name   string
	GoType string
	DBName string
	ChType string
	Col    colInfo
}

// structInfo describes a struct with tagged fields.
type structInfo struct {
	Name   string
	Fields []fieldInfo
}

// typeString converts an AST type expression to a Go type string.
func typeString(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return typeString(t.X) + "." + t.Sel.Name
	case *ast.StarExpr:
		return "*" + typeString(t.X)
	case *ast.ArrayType:
		if t.Len == nil {
			return "[]" + typeString(t.Elt)
		}
		// Fixed-size array like [16]byte
		if lit, ok := t.Len.(*ast.BasicLit); ok && lit.Kind == token.INT {
			return "[" + lit.Value + "]" + typeString(t.Elt)
		}
		return "[]" + typeString(t.Elt)
	case *ast.MapType:
		return "map[" + typeString(t.Key) + "]" + typeString(t.Value)
	default:
		return fmt.Sprintf("%T", expr)
	}
}

// lowerFirst returns s with the first letter lowercased.
func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	r[0] = unicode.ToLower(r[0])
	return string(r)
}

// findStructFields looks up a struct type by name in the package syntax files
// and returns its fields that have both db and chtype tags.
func findStructFields(pkg *packages.Package, fset *token.FileSet, structName string) ([]fieldInfo, error) {
	for _, f := range pkg.Syntax {
		var found []fieldInfo
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || ts.Name.Name != structName {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range st.Fields.List {
				if field.Tag == nil || len(field.Names) == 0 {
					continue
				}
				rawTag := strings.Trim(field.Tag.Value, "`")
				tag := reflect.StructTag(rawTag)
				dbName := tag.Get("db")
				if dbName == "" || dbName == "-" {
					continue
				}
				chType := tag.Get("chtype")
				if chType == "" {
					continue
				}
				goType := typeString(field.Type)
				ci, err := colMapping(goType, chType)
				if err != nil {
					continue
				}
				found = append(found, fieldInfo{
					Name:   field.Names[0].Name,
					GoType: goType,
					DBName: dbName,
					ChType: chType,
					Col:    ci,
				})
			}
			return false // found it, stop
		})
		if found != nil {
			return found, nil
		}
	}
	return nil, fmt.Errorf("struct %q not found in package", structName)
}

// parseTupleOrNestedArgs parses the inner part of "Tuple(name Type, name2 Type2)"
// or "Nested(name Type, name2 Type2)" and returns a map from db name to CH type.
func parseTupleOrNestedArgs(chType string) map[string]string {
	// Strip outer wrapper
	var inner string
	if strings.HasPrefix(chType, "Tuple(") {
		inner = chType[len("Tuple(") : len(chType)-1]
	} else if strings.HasPrefix(chType, "Nested(") {
		inner = chType[len("Nested(") : len(chType)-1]
	} else {
		return nil
	}

	result := make(map[string]string)
	// Split by top-level commas
	for inner != "" {
		comma := findTopLevelComma(inner)
		var part string
		if comma < 0 {
			part = strings.TrimSpace(inner)
			inner = ""
		} else {
			part = strings.TrimSpace(inner[:comma])
			inner = inner[comma+1:]
		}
		// Each part is "name Type" — split on first space
		name, typ, ok := strings.Cut(part, " ")
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		typ = strings.TrimSpace(typ)
		result[name] = typ
	}
	return result
}

// resolveTupleSubColumns resolves the sub-columns for a Tuple or Nested field.
// parentFieldName is the Go field name (e.g., "Address"), used as prefix for sub-column vars.
// structName is the Go struct type name to look up.
// chType is the full Tuple(...) or Nested(...) chtype string.
func resolveTupleSubColumns(pkg *packages.Package, fset *token.FileSet, parentFieldName, structName, chType string) ([]tupleSubCol, error) {
	subFields, err := findStructFields(pkg, fset, structName)
	if err != nil {
		return nil, fmt.Errorf("resolving sub-columns for %s: %w", parentFieldName, err)
	}

	// Parse the chtype args to validate sub-fields exist
	chArgs := parseTupleOrNestedArgs(chType)
	if chArgs == nil {
		return nil, fmt.Errorf("cannot parse chtype args from %q", chType)
	}

	prefix := lowerFirst(parentFieldName)
	var subs []tupleSubCol
	for _, sf := range subFields {
		if _, ok := chArgs[sf.DBName]; !ok {
			continue // sub-field not in the chtype definition
		}
		subs = append(subs, tupleSubCol{
			fieldName:  sf.Name,
			colVarName: prefix + sf.Name + "Col",
			dbName:     sf.DBName,
			col:        sf.Col,
		})
	}

	if len(subs) == 0 {
		return nil, fmt.Errorf("no matching sub-columns found for %s", parentFieldName)
	}
	return subs, nil
}

// generateColumns parses inputFile, finds tagged structs, and writes generated code to outFile.
func generateColumns(inputFile, outFile string, withIter bool) error {
	absInput, err := filepath.Abs(inputFile)
	if err != nil {
		return fmt.Errorf("resolving input path: %w", err)
	}

	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes,
		Fset: token.NewFileSet(),
		Dir:  filepath.Dir(absInput),
	}

	pkgs, err := packages.Load(cfg, ".")
	if err != nil {
		return fmt.Errorf("loading package: %w", err)
	}
	if len(pkgs) == 0 {
		return errors.New("no packages found")
	}

	pkg := pkgs[0]
	if len(pkg.Errors) > 0 {
		return fmt.Errorf("package errors: %v", pkg.Errors)
	}

	// Find the syntax file matching absInput
	var targetFile *ast.File
	for _, f := range pkg.Syntax {
		pos := cfg.Fset.Position(f.Pos())
		if filepath.Clean(pos.Filename) == filepath.Clean(absInput) {
			targetFile = f
			break
		}
	}
	if targetFile == nil {
		return fmt.Errorf("could not find syntax file for %s", absInput)
	}

	pkgName := pkg.Name

	// Walk AST to find structs with tagged fields
	var structs []structInfo
	ast.Inspect(targetFile, func(n ast.Node) bool {
		typeSpec, ok := n.(*ast.TypeSpec)
		if !ok {
			return true
		}
		structType, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			return true
		}

		si := structInfo{Name: typeSpec.Name.Name}
		for _, field := range structType.Fields.List {
			if field.Tag == nil {
				continue
			}
			rawTag := strings.Trim(field.Tag.Value, "`")
			tag := reflect.StructTag(rawTag)

			dbName := tag.Get("db")
			if dbName == "" || dbName == "-" {
				continue
			}
			chType := tag.Get("chtype")
			if chType == "" {
				continue
			}

			if len(field.Names) == 0 {
				continue
			}
			fieldName := field.Names[0].Name
			goType := typeString(field.Type)

			ci, err := colMapping(goType, chType)
			if err != nil {
				// Skip fields we can't map (log to stderr)
				fmt.Fprintf(os.Stderr, "warning: skipping field %s.%s: %v\n", typeSpec.Name.Name, fieldName, err)
				continue
			}

			// Resolve sub-columns for Tuple/Nested fields
			if ci.isTuple || ci.isNested {
				structName := ci.goType
				subs, err := resolveTupleSubColumns(pkg, cfg.Fset, fieldName, structName, chType)
				if err != nil {
					fmt.Fprintf(os.Stderr, "warning: skipping field %s.%s: %v\n", typeSpec.Name.Name, fieldName, err)
					continue
				}
				ci.subColumns = subs
			}

			si.Fields = append(si.Fields, fieldInfo{
				Name:   fieldName,
				GoType: goType,
				DBName: dbName,
				ChType: chType,
				Col:    ci,
			})
		}

		if len(si.Fields) > 0 {
			structs = append(structs, si)
		}
		return true
	})

	if len(structs) == 0 {
		return errors.New("no structs with db+chtype tags found")
	}

	// Generate output code
	var buf bytes.Buffer
	buf.WriteString("// Code generated by chgen columns; DO NOT EDIT.\n\n")
	buf.WriteString("package " + pkgName + "\n\n")

	for _, s := range structs {
		writeColumnsStruct(&buf, s, withIter)
	}

	// Run goimports on the output
	formatted, err := imports.Process(outFile, buf.Bytes(), nil)
	if err != nil {
		// Fall back to unformatted if goimports fails
		formatted = buf.Bytes()
	}

	if err := os.WriteFile(outFile, formatted, 0o644); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}
	return nil
}

func writeColumnsStruct(buf *bytes.Buffer, s structInfo, withIter bool) {
	name := s.Name
	colsName := name + "Columns"

	// Struct definition
	fmt.Fprintf(buf, "// %s holds the columns for reading/writing %s rows.\n", colsName, name)
	fmt.Fprintf(buf, "type %s struct {\n", colsName)
	for _, f := range s.Fields {
		if f.Col.isTuple || f.Col.isNested {
			// Sub-column fields (unexported)
			for _, sub := range f.Col.subColumns {
				fmt.Fprintf(buf, "\t%s %s\n", sub.colVarName, sub.col.fieldType)
			}
			// Exported Tuple/Nested field
			fmt.Fprintf(buf, "\t%s %s\n", f.Name, f.Col.fieldType)
		} else {
			fmt.Fprintf(buf, "\t%s %s\n", f.Name, f.Col.fieldType)
		}
	}
	fmt.Fprintf(buf, "}\n\n")

	// Check if any field is Tuple/Nested
	hasTupleOrNested := false
	for _, f := range s.Fields {
		if f.Col.isTuple || f.Col.isNested {
			hasTupleOrNested = true
			break
		}
	}

	// Constructor
	fmt.Fprintf(buf, "// New%s creates a new %s with all columns initialized.\n", colsName, colsName)
	fmt.Fprintf(buf, "func New%s() *%s {\n", colsName, colsName)

	if hasTupleOrNested {
		// Use assignment style when Tuple/Nested fields exist
		fmt.Fprintf(buf, "\tt := &%s{}\n", colsName)
		for _, f := range s.Fields {
			if f.Col.isTuple {
				for _, sub := range f.Col.subColumns {
					fmt.Fprintf(buf, "\tt.%s = %s\n", sub.colVarName, sub.col.constructor)
				}
				fmt.Fprintf(buf, "\tt.%s = column.NewTuple(", f.Name)
				for i, sub := range f.Col.subColumns {
					if i > 0 {
						buf.WriteString(", ")
					}
					fmt.Fprintf(buf, "t.%s", sub.colVarName)
				}
				buf.WriteString(")\n")
			} else if f.Col.isNested {
				for _, sub := range f.Col.subColumns {
					fmt.Fprintf(buf, "\tt.%s = %s\n", sub.colVarName, sub.col.constructor)
				}
				fmt.Fprintf(buf, "\tt.%s = column.NewNested(", f.Name)
				for i, sub := range f.Col.subColumns {
					if i > 0 {
						buf.WriteString(", ")
					}
					fmt.Fprintf(buf, "t.%s", sub.colVarName)
				}
				buf.WriteString(")\n")
			} else {
				fmt.Fprintf(buf, "\tt.%s = %s\n", f.Name, f.Col.constructor)
			}
		}
		// SetName calls
		for _, f := range s.Fields {
			fmt.Fprintf(buf, "\tt.%s.SetName([]byte(%q))\n", f.Name, f.DBName)
		}
	} else {
		// Use struct literal style (backward compatible)
		fmt.Fprintf(buf, "\tt := &%s{\n", colsName)
		for _, f := range s.Fields {
			fmt.Fprintf(buf, "\t\t%s: %s,\n", f.Name, f.Col.constructor)
		}
		fmt.Fprintf(buf, "\t}\n")
		// SetName calls
		for _, f := range s.Fields {
			fmt.Fprintf(buf, "\tt.%s.SetName([]byte(%q))\n", f.Name, f.DBName)
		}
	}
	// SetStrict(false) calls
	for _, f := range s.Fields {
		if f.Col.needsStrictFalse {
			fmt.Fprintf(buf, "\tt.%s.SetStrict(false)\n", f.Name)
		}
	}
	fmt.Fprintf(buf, "\treturn t\n")
	fmt.Fprintf(buf, "}\n\n")

	// Columns() method — returns Tuple/ArrayBase, not sub-columns
	fmt.Fprintf(buf, "// Columns returns the list of ColumnCore for use with SelectStmt.\n")
	fmt.Fprintf(buf, "func (t *%s) Columns() []column.ColumnCore {\n", colsName)
	fmt.Fprintf(buf, "\treturn []column.ColumnCore{\n")
	for _, f := range s.Fields {
		fmt.Fprintf(buf, "\t\tt.%s,\n", f.Name)
	}
	fmt.Fprintf(buf, "\t}\n")
	fmt.Fprintf(buf, "}\n\n")

	// Write() method
	fmt.Fprintf(buf, "// Write appends a single %s row to all columns.\n", name)
	fmt.Fprintf(buf, "func (t *%s) Write(m *%s) {\n", colsName, name)
	for _, f := range s.Fields {
		if f.Col.isTuple {
			// Write sub-columns from struct fields
			for _, sub := range f.Col.subColumns {
				fmt.Fprintf(buf, "\tt.%s.%s(m.%s.%s)\n", sub.colVarName, sub.col.appendMethod, f.Name, sub.fieldName)
			}
		} else if f.Col.isNested {
			// Write nested: AppendLen + loop
			fmt.Fprintf(buf, "\tt.%s.AppendLen(len(m.%s))\n", f.Name, f.Name)
			fmt.Fprintf(buf, "\tfor _, v := range m.%s {\n", f.Name)
			for _, sub := range f.Col.subColumns {
				fmt.Fprintf(buf, "\t\tt.%s.%s(v.%s)\n", sub.colVarName, sub.col.appendMethod, sub.fieldName)
			}
			fmt.Fprintf(buf, "\t}\n")
		} else {
			fmt.Fprintf(buf, "\tt.%s.%s(m.%s)\n", f.Name, f.Col.appendMethod, f.Name)
		}
	}
	fmt.Fprintf(buf, "}\n\n")

	// Read() method
	fmt.Fprintf(buf, "// Read reads a single row at index row from all columns.\n")
	fmt.Fprintf(buf, "func (t *%s) Read(row int) %s {\n", colsName, name)
	fmt.Fprintf(buf, "\treturn %s{\n", name)
	for _, f := range s.Fields {
		if f.Col.isTuple {
			// Reconstruct struct from sub-columns
			fmt.Fprintf(buf, "\t\t%s: %s{\n", f.Name, f.Col.goType)
			for _, sub := range f.Col.subColumns {
				fmt.Fprintf(buf, "\t\t\t%s: t.%s.%s(row),\n", sub.fieldName, sub.colVarName, sub.col.rowMethod)
			}
			fmt.Fprintf(buf, "\t\t},\n")
		} else if f.Col.isNested {
			// Nested Read not supported — return nil
			fmt.Fprintf(buf, "\t\t// TODO: Nested Read not yet supported — access sub-columns directly\n")
		} else {
			fmt.Fprintf(buf, "\t\t%s: t.%s.%s(row),\n", f.Name, f.Name, f.Col.rowMethod)
		}
	}
	fmt.Fprintf(buf, "\t}\n")
	fmt.Fprintf(buf, "}\n\n")

	// SetWriteBufferSize() method — only call on Tuple/ArrayBase (delegates to sub-columns)
	fmt.Fprintf(buf, "// SetWriteBufferSize sets the write buffer size on all columns.\n")
	fmt.Fprintf(buf, "func (t *%s) SetWriteBufferSize(n int) {\n", colsName)
	for _, f := range s.Fields {
		fmt.Fprintf(buf, "\tt.%s.SetWriteBufferSize(n)\n", f.Name)
	}
	fmt.Fprintf(buf, "}\n\n")

	// Reset() method — only call on Tuple/ArrayBase (delegates to sub-columns)
	fmt.Fprintf(buf, "// Reset resets all columns to empty.\n")
	fmt.Fprintf(buf, "func (t *%s) Reset() {\n", colsName)
	for _, f := range s.Fields {
		fmt.Fprintf(buf, "\tt.%s.Reset()\n", f.Name)
	}
	fmt.Fprintf(buf, "}\n\n")

	// Iter() method (optional)
	if withIter {
		fmt.Fprintf(buf, "// Iter returns an iterator over %s rows from a SelectStmt.\n", name)
		fmt.Fprintf(buf, "func (t *%s) Iter(stmt chconn.SelectStmt) iter.Seq2[%s, error] {\n", colsName, name)
		fmt.Fprintf(buf, "\treturn func(yield func(%s, error) bool) {\n", name)
		fmt.Fprintf(buf, "\t\tdefer stmt.Close()\n")
		fmt.Fprintf(buf, "\t\tfor n, err := range stmt.Iter() {\n")
		fmt.Fprintf(buf, "\t\t\tif err != nil {\n")
		fmt.Fprintf(buf, "\t\t\t\tyield(%s{}, err)\n", name)
		fmt.Fprintf(buf, "\t\t\t\treturn\n")
		fmt.Fprintf(buf, "\t\t\t}\n")
		fmt.Fprintf(buf, "\t\t\tfor i := range n {\n")
		fmt.Fprintf(buf, "\t\t\t\tif !yield(t.Read(i), nil) {\n")
		fmt.Fprintf(buf, "\t\t\t\t\treturn\n")
		fmt.Fprintf(buf, "\t\t\t\t}\n")
		fmt.Fprintf(buf, "\t\t\t}\n")
		fmt.Fprintf(buf, "\t\t}\n")
		fmt.Fprintf(buf, "\t}\n")
		fmt.Fprintf(buf, "}\n\n")
	}
}
