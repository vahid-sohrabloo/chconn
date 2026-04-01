package main

import (
	"errors"
	"flag"
	"os"
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

	return errors.New("columns command not yet implemented")
}
