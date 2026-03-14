package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vahid-sohrabloo/chconn/v3/cmd/chtuplegen/bootstrap"
	"github.com/vahid-sohrabloo/chconn/v3/cmd/chtuplegen/parser"
)

var buildTags = flag.String("build_tags", "", "build tags to add to generated file")
var genBuildFlags = flag.String("gen_build_flags", "", "build flags when running the generator while bootstrapping")
var snakeCase = flag.Bool("snake_case", false, "use snake_case names instead of CamelCase by default")
var lowerCamelCase = flag.Bool("lower_camel_case", false, "use lowerCamelCase names instead of CamelCase by default")
var allStructs = flag.Bool("all", false, "generate tuple struct for all structs in a file")
var leaveTemps = flag.Bool("leave_temps", false, "do not delete temporary files")
var stubs = flag.Bool("stubs", false, "only generate stubs")
var noformat = flag.Bool("noformat", false, "do not run 'gofmt -w' on output file")
var specifiedName = flag.String("output_filename", "", "specify the filename of the output")
var processPkg = flag.Bool("pkg", false, "process the whole package instead of just the given file")

func generate(fname string) (err error) {
	fInfo, err := os.Stat(fname) //nolint:gosec // fname is from command-line flag, not user input
	if err != nil {
		return err
	}

	p := parser.Parser{AllStructs: *allStructs}
	if err := p.Parse(fname, fInfo.IsDir()); err != nil {
		return fmt.Errorf("error parsing %v: %v", fname, err)
	}

	var outName string
	if fInfo.IsDir() {
		outName = filepath.Join(fname, p.PkgName+"_chconn_gen.go")
	} else {
		if s := strings.TrimSuffix(fname, ".go"); s == fname {
			return errors.New("filename must end in '.go'")
		} else {
			outName = s + "_chconn_gen.go"
		}
	}

	if *specifiedName != "" {
		outName = *specifiedName
	}

	var trimmedBuildTags string
	if *buildTags != "" {
		trimmedBuildTags = strings.TrimSpace(*buildTags)
	}

	var trimmedGenBuildFlags string
	if *genBuildFlags != "" {
		trimmedGenBuildFlags = strings.TrimSpace(*genBuildFlags)
	}

	g := bootstrap.Generator{
		BuildTags:      trimmedBuildTags,
		GenBuildFlags:  trimmedGenBuildFlags,
		PkgPath:        p.PkgPath,
		PkgName:        p.PkgName,
		Types:          p.StructNames,
		SnakeCase:      *snakeCase,
		LowerCamelCase: *lowerCamelCase,
		LeaveTemps:     *leaveTemps,
		OutName:        outName,
		StubsOnly:      *stubs,
		NoFormat:       *noformat,
	}

	if err := g.Run(); err != nil {
		return fmt.Errorf("bootstrap failed: %v", err)
	}
	return nil
}

func main() {
	flag.Parse()

	files := flag.Args()

	gofile := os.Getenv("GOFILE")
	if *processPkg {
		gofile = filepath.Dir(gofile)
	}

	if len(files) == 0 && gofile != "" {
		files = []string{gofile}
	} else if len(files) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	for _, fname := range files {
		if err := generate(fname); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
