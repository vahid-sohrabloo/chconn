package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: chgen <model|columns> [flags]\n")
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "model":
		err = runModel(os.Args[2:])
	case "columns":
		err = runColumns(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\nUsage: chgen <model|columns> [flags]\n", os.Args[1])
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
