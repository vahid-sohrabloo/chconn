package main

import (
	"errors"
	"flag"
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

	return errors.New("model command not yet implemented")
}
