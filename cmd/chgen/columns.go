package main

import (
	"bytes"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/scanner"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"text/template"
	"text/template/parse"
	"unicode"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

//go:embed columns.tmpl
var columnsTemplate string

type columnsConfig struct {
	input     string
	withIter  bool
	templates stringList
}

// stringList is a repeatable string flag.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func runColumns(args []string) error {
	var cfg columnsConfig
	fs := flag.NewFlagSet("columns", flag.ExitOnError)
	fs.StringVar(&cfg.input, "input", "", "Input Go file (default: $GOFILE)")
	fs.BoolVar(&cfg.withIter, "with-iter", false, "Generate Iter() method")
	fs.Var(&cfg.templates, "template", "Extra template file (repeatable); may add an \"extra\" block or override built-in blocks")

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

	return generateColumns(cfg.input, outFile, cfg.withIter, cfg.templates...)
}

// File is the data passed to the "file" template block.
type File struct {
	Package  string
	WithIter bool
	Structs  []*Struct
}

// Struct is the data passed to every per-struct template block.
type Struct struct {
	Name     string // Go row struct name
	ColsName string // generated columns struct name
	Fields   []*Field
}

// HasTupleOrNested reports whether any field is a Tuple or Nested column.
func (s *Struct) HasTupleOrNested() bool {
	for _, f := range s.Fields {
		if f.IsTuple || f.IsNested {
			return true
		}
	}
	return false
}

// ColumnNames returns the comma-separated column names in field order.
func (s *Struct) ColumnNames() string {
	names := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		names[i] = f.DBName
	}
	return strings.Join(names, ", ")
}

// Field is one top-level column.
type Field struct {
	Name             string // Go field name, also the column field name
	GoType           string // Go type of the row field
	DBName           string
	ChType           string
	FieldType        string // column type, e.g. *column.Base[uint64]
	Constructor      string // empty for Tuple/Nested
	AppendMethod     string // empty for Tuple/Nested
	WriteExpr        string // value passed to AppendMethod, e.g. m.ID or int8(m.Status)
	ReadExpr         string // value read back for row; empty when it cannot be inverted
	NeedsStrictFalse bool
	IsTuple          bool
	IsNested         bool
	TupleType        string // Go struct type of Tuple/Nested elements
	SubColumns       []*SubColumn
}

// SubColumn is one element column of a Tuple or Nested field.
type SubColumn struct {
	FieldName    string // Go field name in the element struct
	ColVar       string // unexported column field name, e.g. addressCityCol
	DBName       string
	ChType       string
	FieldType    string
	Constructor  string
	AppendMethod string
	WriteExpr    string // m.Address.City for Tuple, v.Number for Nested
	ReadExpr     string
}

// fieldTags holds the chgen tags of one struct field.
type fieldTags struct {
	db, chType, conv string
}

// columnTags returns the tags of a struct field, or ok=false when the field
// is not a column (no db tag, db:"-", or no chtype).
func columnTags(field *ast.Field) (fieldTags, bool) {
	if field.Tag == nil || len(field.Names) == 0 {
		return fieldTags{}, false
	}
	tag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
	ft := fieldTags{db: tag.Get("db"), chType: tag.Get("chtype"), conv: tag.Get("chconv")}
	if ft.db == "" || ft.db == "-" || ft.chType == "" {
		return fieldTags{}, false
	}
	return ft, true
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

// resolver maps struct fields to columns using the loaded package.
type resolver struct {
	pkg  *packages.Package
	qual types.Qualifier
}

func newResolver(pkg *packages.Package) *resolver {
	return &resolver{
		pkg: pkg,
		qual: func(p *types.Package) string {
			if p == pkg.Types {
				return ""
			}
			return p.Name()
		},
	}
}

// mapped is a resolved column for one field. writeFmt and readFmt wrap the
// field value and the column row value; readFmt is empty when the
// conversion cannot be inverted.
type mapped struct {
	col      colInfo
	writeFmt string
	readFmt  string
}

// mapField resolves the column for a field: through the chconv tag if set,
// otherwise through mapType.
func (r *resolver) mapField(field *ast.Field, tags fieldTags) (mapped, error) {
	goType := typeString(field.Type)
	t := r.pkg.TypesInfo.TypeOf(field.Type)
	if tags.conv != "" {
		return r.mapConv(t, goType, tags)
	}
	return r.mapType(t, goType, tags.chType)
}

// mapType maps a Go type to a column. Order: direct mapping, the canonical
// name of a basic type (byte, aliases), then a cast through a named type's
// underlying type.
func (r *resolver) mapType(t types.Type, goType, chType string) (mapped, error) {
	ci, err := colMapping(goType, chType)
	if err == nil {
		return mapped{col: ci, writeFmt: "%s", readFmt: "%s"}, nil
	}

	switch u := types.Unalias(t).(type) {
	case *types.Basic:
		if ci, bErr := colMapping(types.Typ[u.Kind()].Name(), chType); bErr == nil {
			return mapped{col: ci, writeFmt: "%s", readFmt: "%s"}, nil
		}
	case *types.Named:
		switch u.Underlying().(type) {
		case *types.Struct, *types.Pointer, *types.Interface:
			return mapped{}, err
		}
		under := types.TypeString(u.Underlying(), r.qual)
		ci, uErr := colMapping(under, chType)
		if uErr != nil {
			return mapped{}, fmt.Errorf("%w (underlying type %s: %w)", err, under, uErr)
		}
		if ci.isTuple || ci.isNested {
			return mapped{}, err
		}
		return mapped{col: ci, writeFmt: under + "(%s)", readFmt: types.TypeString(t, r.qual) + "(%s)"}, nil
	}
	return mapped{}, err
}

// mapConv resolves a chconv tag: "Func" calls a package-level func with the
// field value; ".Method" calls a method on the field value.
func (r *resolver) mapConv(t types.Type, goType string, tags fieldTags) (mapped, error) {
	var (
		sig     *types.Signature
		convFmt string
	)
	if method, ok := strings.CutPrefix(tags.conv, "."); ok {
		obj, _, _ := types.LookupFieldOrMethod(t, true, r.pkg.Types, method)
		fn, ok := obj.(*types.Func)
		if !ok {
			return mapped{}, fmt.Errorf("chconv: %s has no method %s", goType, method)
		}
		sig = fn.Signature()
		if sig.Params().Len() != 0 || sig.Results().Len() != 1 {
			return mapped{}, fmt.Errorf("chconv: %s must take no arguments and return exactly one value", tags.conv)
		}
		convFmt = "%s." + method + "()"
	} else {
		fn, ok := r.pkg.Types.Scope().Lookup(tags.conv).(*types.Func)
		if !ok {
			return mapped{}, fmt.Errorf("chconv: no package-level func %s in package %s", tags.conv, r.pkg.Name)
		}
		sig = fn.Signature()
		if sig.TypeParams().Len() > 0 || sig.Variadic() {
			return mapped{}, fmt.Errorf("chconv: %s must not be generic or variadic", tags.conv)
		}
		if sig.Params().Len() != 1 || sig.Results().Len() != 1 {
			return mapped{}, fmt.Errorf("chconv: %s must take 1 argument and return exactly one value", tags.conv)
		}
		if param := sig.Params().At(0).Type(); !types.AssignableTo(t, param) {
			return mapped{}, fmt.Errorf("chconv: %s takes %s, field is %s", tags.conv,
				types.TypeString(param, r.qual), types.TypeString(t, r.qual))
		}
		convFmt = tags.conv + "(%s)"
	}

	rt := sig.Results().At(0).Type()
	result := types.TypeString(rt, r.qual)
	m, err := r.mapType(rt, result, tags.chType)
	if err != nil {
		return mapped{}, fmt.Errorf("chconv: %s returns %s: %w", tags.conv, result, err)
	}
	if m.col.isTuple || m.col.isNested {
		return mapped{}, errors.New("chconv: not supported on Tuple or Nested columns")
	}
	return mapped{col: m.col, writeFmt: strings.Replace(m.writeFmt, "%s", convFmt, 1)}, nil
}

// isAnyPlaceholder reports whether t is any or a slice of any. chgen model
// emits these for Tuple/Nested columns that need a hand-written struct.
func isAnyPlaceholder(t types.Type) bool {
	for {
		s, ok := types.Unalias(t).(*types.Slice)
		if !ok {
			break
		}
		t = s.Elem()
	}
	iface, ok := types.Unalias(t).(*types.Interface)
	return ok && iface.Empty()
}

// findStruct returns the struct type with the given name in the package.
func findStruct(pkg *packages.Package, name string) (*ast.StructType, bool) {
	for _, f := range pkg.Syntax {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gd.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok || ts.Name.Name != name {
					continue
				}
				if st, ok := ts.Type.(*ast.StructType); ok {
					return st, true
				}
			}
		}
	}
	return nil, false
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

// resolveSubColumns resolves the element columns of a Tuple or Nested field.
// Every tagged element field must map, and its db name must appear in the
// chtype, and every chtype element must have a field.
func (r *resolver) resolveSubColumns(f *Field) error {
	st, ok := findStruct(r.pkg, f.TupleType)
	if !ok {
		return fmt.Errorf("struct %q not found in package", f.TupleType)
	}
	chArgs := parseTupleOrNestedArgs(f.ChType)
	if chArgs == nil {
		return fmt.Errorf("cannot parse chtype args from %q", f.ChType)
	}

	prefix := lowerFirst(f.Name)
	seen := make(map[string]bool, len(chArgs))
	for _, field := range st.Fields.List {
		tags, ok := columnTags(field)
		if !ok {
			continue
		}
		name := field.Names[0].Name
		if _, ok := chArgs[tags.db]; !ok {
			return fmt.Errorf("%s.%s: db name %q is not in chtype %q", f.TupleType, name, tags.db, f.ChType)
		}
		m, err := r.mapField(field, tags)
		if err != nil {
			return fmt.Errorf("%s.%s: %w", f.TupleType, name, err)
		}
		if m.col.isTuple || m.col.isNested {
			return fmt.Errorf("%s.%s: Tuple/Nested inside Tuple/Nested is not supported", f.TupleType, name)
		}
		seen[tags.db] = true

		src := "v." + name
		if f.IsTuple {
			src = "m." + f.Name + "." + name
		}
		colVar := prefix + name + "Col"
		sc := &SubColumn{
			FieldName:    name,
			ColVar:       colVar,
			DBName:       tags.db,
			ChType:       tags.chType,
			FieldType:    m.col.fieldType,
			Constructor:  m.col.constructor,
			AppendMethod: m.col.appendMethod,
			WriteExpr:    fmt.Sprintf(m.writeFmt, src),
		}
		if m.readFmt != "" {
			sc.ReadExpr = fmt.Sprintf(m.readFmt, "t."+colVar+"."+m.col.rowMethod+"(row)")
		}
		f.SubColumns = append(f.SubColumns, sc)
	}
	for name := range chArgs {
		if !seen[name] {
			return fmt.Errorf("chtype %q element %q has no field in %s", f.ChType, name, f.TupleType)
		}
	}
	return nil
}

// buildStruct returns the columns model for a struct, or nil if it has no
// column fields.
func (r *resolver) buildStruct(name string, st *ast.StructType) (*Struct, error) {
	s := &Struct{Name: name, ColsName: name + "Columns"}
	for _, field := range st.Fields.List {
		tags, ok := columnTags(field)
		if !ok {
			continue
		}
		fieldName := field.Names[0].Name
		if isAnyPlaceholder(r.pkg.TypesInfo.TypeOf(field.Type)) {
			fmt.Fprintf(os.Stderr, "warning: skipping field %s.%s: type any needs a hand-written struct\n", name, fieldName)
			continue
		}
		m, err := r.mapField(field, tags)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", name, fieldName, err)
		}
		f := &Field{
			Name:             fieldName,
			GoType:           typeString(field.Type),
			DBName:           tags.db,
			ChType:           tags.chType,
			FieldType:        m.col.fieldType,
			Constructor:      m.col.constructor,
			AppendMethod:     m.col.appendMethod,
			NeedsStrictFalse: m.col.needsStrictFalse,
			IsTuple:          m.col.isTuple,
			IsNested:         m.col.isNested,
		}
		if f.IsTuple || f.IsNested {
			f.TupleType = m.col.goType
			if err := r.resolveSubColumns(f); err != nil {
				return nil, fmt.Errorf("%s.%s: %w", name, fieldName, err)
			}
		} else {
			f.WriteExpr = fmt.Sprintf(m.writeFmt, "m."+fieldName)
			if m.readFmt != "" {
				f.ReadExpr = fmt.Sprintf(m.readFmt, "t."+fieldName+"."+m.col.rowMethod+"(row)")
			}
		}
		s.Fields = append(s.Fields, f)
	}
	if len(s.Fields) == 0 {
		return nil, nil
	}
	return s, nil
}

// generateColumns parses inputFile, finds tagged structs, and writes generated code to outFile.
func generateColumns(inputFile, outFile string, withIter bool, templateFiles ...string) error {
	absInput, err := filepath.Abs(inputFile)
	if err != nil {
		return fmt.Errorf("resolving input path: %w", err)
	}

	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
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

	r := newResolver(pkg)
	file := &File{Package: pkg.Name, WithIter: withIter}
	for _, decl := range targetFile.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}
			s, err := r.buildStruct(ts.Name.Name, st)
			if err != nil {
				return err
			}
			if s != nil {
				file.Structs = append(file.Structs, s)
			}
		}
	}

	if len(file.Structs) == 0 {
		return errors.New("no structs with db+chtype tags found")
	}

	src, err := renderColumns(file, templateFiles)
	if err != nil {
		return err
	}

	formatted, err := imports.Process(outFile, src, nil)
	if err != nil {
		return formatError(err, src, templateFiles)
	}

	if err := os.WriteFile(outFile, formatted, 0o644); err != nil { //nolint:gosec
		return fmt.Errorf("writing output: %w", err)
	}
	return nil
}

// renderColumns executes the built-in template plus the user templates.
// A block a user template defines replaces the built-in one, even when
// empty; the same block defined in two user templates is an error.
func renderColumns(file *File, templateFiles []string) ([]byte, error) {
	builtin, err := template.New("columns.tmpl").Parse(columnsTemplate)
	if err != nil {
		return nil, fmt.Errorf("parsing built-in template: %w", err)
	}

	blocks := make(map[string]*parse.Tree)
	for _, t := range builtin.Templates() {
		if t.Tree != nil && t.Name() != builtin.Name() {
			blocks[t.Name()] = t.Tree
		}
	}

	definedIn := make(map[string]string)
	for _, path := range templateFiles {
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading template: %w", err)
		}
		root := "user:" + path
		user, err := template.New(root).Parse(string(content))
		if err != nil {
			return nil, fmt.Errorf("parsing template %s: %w", path, err)
		}
		for _, t := range user.Templates() {
			if t.Name() == root || t.Tree == nil {
				continue
			}
			if prev, ok := definedIn[t.Name()]; ok {
				return nil, fmt.Errorf("block %q is defined in both %s and %s", t.Name(), prev, path)
			}
			definedIn[t.Name()] = path
			blocks[t.Name()] = t.Tree
		}
	}

	tmpl := template.New("chgen")
	for name, tree := range blocks {
		if _, err := tmpl.AddParseTree(name, tree); err != nil {
			return nil, fmt.Errorf("adding block %q: %w", name, err)
		}
	}

	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "file", file); err != nil {
		return nil, fmt.Errorf("executing template: %w", err)
	}
	return buf.Bytes(), nil
}

// formatError reports a goimports failure with the offending generated line.
func formatError(err error, src []byte, templateFiles []string) error {
	where := "built-in template"
	if len(templateFiles) > 0 {
		where = "templates " + strings.Join(templateFiles, ", ")
	}
	var list scanner.ErrorList
	if errors.As(err, &list) && len(list) > 0 {
		lines := strings.Split(string(src), "\n")
		if n := list[0].Pos.Line; n > 0 && n <= len(lines) {
			return fmt.Errorf("generated code is not valid Go (%s): %w\n%d: %s", where, err, n, lines[n-1])
		}
	}
	return fmt.Errorf("generated code is not valid Go (%s): %w", where, err)
}
