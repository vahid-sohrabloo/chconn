package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	flag "github.com/spf13/pflag"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
)

type ChColumns struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
}
type ExplainData []struct {
	Plan struct {
		NodeType   string `json:"Node Type"`
		Expression struct {
			Inputs []struct {
				Name string `json:"Name"`
				Type string `json:"Type"`
			} `json:"Inputs"`
			Actions []struct {
				NodeType         string `json:"Node Type"`
				ResultType       string `json:"Result Type"`
				Arguments        []int  `json:"Arguments"`
				RemovedArguments []int  `json:"Removed Arguments"`
				Result           int    `json:"Result"`
			} `json:"Actions"`
			Outputs      []ChColumns `json:"Outputs"`
			Positions    []int       `json:"Positions"`
			ProjectInput bool        `json:"Project Input"`
		} `json:"Expression"`
		Plans []struct {
			NodeType string `json:"Node Type"`
			Plans    []struct {
				NodeType string `json:"Node Type"`
				ReadType string `json:"Read Type"`
				Parts    int    `json:"Parts"`
				Granules int    `json:"Granules"`
			} `json:"Plans"`
		} `json:"Plans"`
	} `json:"Plan"`
}

func main() {
	getter := flag.Bool("getter", false, "generate gatter for properties")
	structName := flag.String("name", "Table", "struct name")
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	packageName := flag.String("package", filepath.Base(dir), "package name")

	flag.Parse()
	var query string

	file := os.Stdin
	fi, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	size := fi.Size()
	if size == 0 {
		query = flag.Arg(0)
	} else {
		queryData, _ := ioutil.ReadAll(file)
		query = string(queryData)
	}

	ctx := context.Background()
	conn, err := chconn.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := conn.Select(context.Background(), "EXPLAIN json = 1, actions = 1, description = 0, header = 0 "+query)
	if err != nil {
		log.Fatal(err)
	}
	col := column.NewString(false)
	var explainData ExplainData
	for stmt.Next() {
		err = stmt.NextColumn(col)
		if err != nil {
			log.Fatal(err)
		}
		col.Next()
		err = json.Unmarshal(col.Value(), &explainData)
		if err != nil {
			log.Fatal(err)
		}
	}
	if stmt.Err() != nil {
		log.Fatal(stmt.Err())
	}

	generateEnum(*packageName, *structName, explainData[0].Plan.Expression.Outputs)
	generateModel(*packageName, *structName, *getter, explainData[0].Plan.Expression.Outputs)
	generateColumns(*packageName, *structName, explainData[0].Plan.Expression.Outputs)
}
