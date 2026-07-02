package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"

	chconn "github.com/vahid-sohrabloo/chconn/v3"
	"golang.org/x/tools/imports"
)

type modelConfig struct {
	dsn        string
	table      string
	database   string
	sqlFile    string
	out        string
	pkg        string
	timeAsUint bool
}

// columnSchema describes one row from system.columns.
type columnSchema struct {
	Name string `db:"name"`
	Type string `db:"type"`
}

func runModel(args []string) error {
	var cfg modelConfig
	fs := flag.NewFlagSet("model", flag.ExitOnError)
	fs.StringVar(&cfg.dsn, "dsn", "", "ClickHouse connection string")
	fs.StringVar(&cfg.table, "table", "", "Table name")
	fs.StringVar(&cfg.database, "database", "default", "Database name")
	fs.StringVar(&cfg.sqlFile, "sql", "", "Path to SQL file with CREATE TABLE")
	fs.StringVar(&cfg.out, "out", "", "Output file path")
	fs.StringVar(&cfg.pkg, "package", "", "Go package name (default: inferred from output dir)")
	fs.BoolVar(&cfg.timeAsUint, "time-as-uint", false, "Use uint/int types for date/time columns")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cfg.dsn == "" && cfg.sqlFile == "" {
		return errors.New("either --dsn or --sql is required")
	}
	if cfg.dsn != "" && cfg.sqlFile != "" {
		return errors.New("--dsn and --sql are mutually exclusive")
	}
	if cfg.dsn != "" && cfg.table == "" {
		return errors.New("--table is required with --dsn")
	}
	if cfg.out == "" {
		return errors.New("--out is required")
	}

	var columns []columnSchema
	var err error

	if cfg.dsn != "" {
		columns, err = fetchColumnsFromDSN(cfg.dsn, cfg.database, cfg.table)
	} else {
		columns, err = fetchColumnsFromSQL(cfg.sqlFile)
	}
	if err != nil {
		return fmt.Errorf("fetching columns: %w", err)
	}

	return generateModel(cfg, columns)
}

// fetchColumnsFromDSN connects to ClickHouse and queries system.columns.
func fetchColumnsFromDSN(dsn, database, table string) ([]columnSchema, error) {
	conn, err := chconn.Connect(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	defer conn.Close()

	const query = `SELECT name, type FROM system.columns WHERE database = {db:String} AND table = {tbl:String} ORDER BY position`
	cols, err := chconn.QueryAll[columnSchema](
		context.Background(), conn, query,
		chconn.StringParameter("db", database),
		chconn.StringParameter("tbl", table),
	)
	if err != nil {
		return nil, fmt.Errorf("querying system.columns: %w", err)
	}
	return cols, nil
}

// createTableRe matches CREATE TABLE [IF NOT EXISTS] [db.]name
var createTableRe = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?(\w+)`)

// extractTableName extracts the table name from a CREATE TABLE statement.
func extractTableName(sql string) (string, error) {
	m := createTableRe.FindStringSubmatch(sql)
	if m == nil {
		return "", errors.New("could not find CREATE TABLE statement")
	}
	return m[1], nil
}

// fetchColumnsFromSQL runs clickhouse-local with the SQL file content and reads system.columns.
func fetchColumnsFromSQL(sqlFile string) ([]columnSchema, error) {
	data, err := os.ReadFile(sqlFile)
	if err != nil {
		return nil, fmt.Errorf("reading sql file: %w", err)
	}

	tableName, err := extractTableName(string(data))
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(
		"%s\nSELECT name, type FROM system.columns WHERE table = '%s' ORDER BY position FORMAT TabSeparated",
		strings.TrimRight(string(data), "\n\r ;"), tableName,
	)

	cmd := exec.Command("clickhouse-local", "--multiquery")
	cmd.Stdin = strings.NewReader(query)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("running clickhouse-local: %w", err)
	}

	return parseTabSeparated(string(out))
}

// parseTabSeparated parses tab-separated output of (name, type) pairs.
func parseTabSeparated(output string) ([]columnSchema, error) {
	var cols []columnSchema
	for line := range strings.SplitSeq(strings.TrimRight(output, "\n"), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("unexpected line format: %q", line)
		}
		cols = append(cols, columnSchema{Name: parts[0], Type: parts[1]})
	}
	return cols, nil
}

// toGoName converts a snake_case, camelCase, or mixed name to an exported Go name.
func toGoName(s string) string {
	// Split on _, -, .
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '_' || r == '-' || r == '.'
	})

	var result strings.Builder
	for _, part := range parts {
		if part == "" {
			continue
		}
		// Within each part, split on camelCase boundaries and capitalize each word.
		words := splitCamel(part)
		for _, w := range words {
			if w == "" {
				continue
			}
			runes := []rune(w)
			runes[0] = unicode.ToUpper(runes[0])
			result.WriteString(string(runes))
		}
	}
	return result.String()
}

// splitCamel splits a camelCase word into segments at uppercase boundaries.
// e.g. "entityId" -> ["entity", "Id"], "AdSlotId" -> ["Ad", "Slot", "Id"]
func splitCamel(s string) []string {
	if s == "" {
		return nil
	}
	var parts []string
	start := 0
	runes := []rune(s)
	for i := 1; i < len(runes); i++ {
		if unicode.IsUpper(runes[i]) && !unicode.IsUpper(runes[i-1]) {
			parts = append(parts, string(runes[start:i]))
			start = i
		}
	}
	parts = append(parts, string(runes[start:]))
	return parts
}

// generateModel generates the Go struct file from column definitions.
func generateModel(cfg modelConfig, columns []columnSchema) error { //nolint:gocritic
	// Determine package name
	pkg := cfg.pkg
	if pkg == "" {
		pkg = filepath.Base(filepath.Dir(cfg.out))
	}

	// Determine table name for struct naming
	tableName := cfg.table
	if tableName == "" {
		// When using sql file mode, we may not have table set; use output file base name
		base := filepath.Base(cfg.out)
		tableName = strings.TrimSuffix(base, filepath.Ext(base))
	}
	structName := toGoName(tableName)

	var buf bytes.Buffer
	buf.WriteString("// Code generated by chgen model; DO NOT EDIT.\n\n")
	buf.WriteString("//go:generate go tool chgen columns\n\n")
	buf.WriteString("package " + pkg + "\n\n")

	// Collect enum types to emit before the struct
	type enumDef struct {
		typeName string
		baseType string
		values   map[string]int
	}

	type fieldDef struct {
		goName  string
		goType  string
		dbName  string
		chType  string
		enumDef *enumDef
	}

	var fields []fieldDef

	for _, col := range columns {
		info, err := chTypeToGo(col.Type, cfg.timeAsUint)
		if err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}

		fd := fieldDef{
			goName: toGoName(col.Name),
			dbName: col.Name,
			chType: col.Type,
		}

		if info.isEnum {
			enumTypeName := structName + toGoName(col.Name)
			fd.goType = enumTypeName
			fd.enumDef = &enumDef{
				typeName: enumTypeName,
				baseType: info.goType,
				values:   info.enumValues,
			}
		} else {
			fd.goType = info.goType
		}

		fields = append(fields, fd)
	}

	// Emit enum type declarations
	for _, f := range fields {
		if f.enumDef == nil {
			continue
		}
		ed := f.enumDef
		fmt.Fprintf(&buf, "type %s %s\n\n", ed.typeName, ed.baseType)

		// Sort values by int value for deterministic output
		type kv struct {
			name string
			val  int
		}
		sorted := make([]kv, 0, len(ed.values))
		for k, v := range ed.values {
			sorted = append(sorted, kv{k, v})
		}
		sort.Slice(sorted, func(i, j int) bool {
			if sorted[i].val != sorted[j].val {
				return sorted[i].val < sorted[j].val
			}
			return sorted[i].name < sorted[j].name
		})

		buf.WriteString("const (\n")
		for _, kv := range sorted {
			constName := ed.typeName + toGoName(kv.name)
			fmt.Fprintf(&buf, "\t%s %s = %d\n", constName, ed.typeName, kv.val)
		}
		buf.WriteString(")\n\n")
	}

	// Emit struct
	fmt.Fprintf(&buf, "type %s struct {\n", structName)
	for _, f := range fields {
		fmt.Fprintf(&buf, "\t%s %s `db:%q chtype:%q`\n", f.goName, f.goType, f.dbName, f.chType)
	}
	buf.WriteString("}\n")

	// Run goimports
	formatted, err := imports.Process(cfg.out, buf.Bytes(), nil)
	if err != nil {
		formatted = buf.Bytes()
	}

	if err := os.MkdirAll(filepath.Dir(cfg.out), 0o755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}
	if err := os.WriteFile(cfg.out, formatted, 0o644); err != nil { //nolint:gosec
		return fmt.Errorf("writing output: %w", err)
	}
	return nil
}
