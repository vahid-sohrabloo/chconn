[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn?ref=badge_shield)

# chconn - ClickHouse low level Driver

chconn is a pure Go driver for ClickHouse that use Native protocol
chconn aims to be low-level, fast, and performant.

## Example Usage
```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/vahid-sohrabloo/chconn/chpool"
	"github.com/vahid-sohrabloo/chconn/column"
)

func main() {
	conn, err := chpool.Connect(context.Background(), os.Getenv("DATABESE_URL"))
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	_, err = conn.Exec(context.Background(), `DROP TABLE IF EXISTS example_table`)
	if err != nil {
		panic(err)
	}

	_, err = conn.Exec(context.Background(), `CREATE TABLE  example_table (
		uint64 UInt64,
		uint64_nullable Nullable(UInt64)
	) Engine=Memory`)
	if err != nil {
		panic(err)
	}

	col1 := column.NewUint64(false)
	col2 := column.NewUint64(true)
	rows := 10000000 // One hundred million rows- insert in 10 times
	numInsert := 10
	startInsert := time.Now()
	for i := 0; i < numInsert; i++ {
		for y := 0; y < rows; y++ {
			col1.Append(uint64(i))
			if i%2 == 0 {
				col2.AppendIsNil(false)
				col2.Append(uint64(i))
			} else {
				col2.AppendIsNil(true)
				col2.AppendEmpty()
			}
		}

		ctxInsert, cancelInsert := context.WithTimeout(context.Background(), time.Second*30)
		// insert data
		insertStmt, err := conn.Insert(ctxInsert, "INSERT INTO example_table (uint64,uint64_nullable) VALUES")
		if err != nil {
			cancelInsert()
			panic(err)
		}
		err = insertStmt.Commit(ctxInsert, col1, col2)
		if err != nil {
			cancelInsert()
			panic(err)
		}
		cancelInsert()
	}
	fmt.Println("inserted 100M rows in ", time.Since(startInsert))

	// select data
	col1Read := column.NewUint64(false)
	col2Read := column.NewUint64(true)

	ctxSelect, cancelSelect := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelSelect()

	startSelect := time.Now()
	// insert data
	selectStmt, err := conn.Select(ctxSelect, "SELECT uint64,uint64_nullable FROM  example_table")
	if err != nil {
		panic(err)
	}

	// make sure close the statement after you are done with it to back it to the pool
	defer selectStmt.Close()

	// next block of data
	// for more information about block, see: https://clickhouse.com/docs/en/development/architecture/#block
	var col1Data []uint64
	var col2DataNil []uint8
	var col2Data []uint64
	for selectStmt.Next() {
		err = selectStmt.NextColumn(col1Read)
		if err != nil {
			panic(err)
		}
		col1Data = col1Data[:0]
		col1Read.ReadAll(&col1Data)

		err = selectStmt.NextColumn(col2Read)
		if err != nil {
			panic(err)
		}
		col2DataNil = col2DataNil[:0]
		col2Read.ReadAllNil(&col2DataNil)

		col2Data = col2Data[:0]
		col2Read.ReadAll(&col2Data)

	}

	// check errors
	if selectStmt.Err() != nil {
		panic(selectStmt.Err())
	}
	fmt.Println("selected 100M rows in ", time.Since(startSelect))

}

```
```
inserted 100M rows in  1.206666188s
selected 100M rows in  880.505004ms
```

# Features
* Support all ClickHouse data types
* Batch select and insert
* Full TLS connection control
* Connection pool with after-connect hook for arbitrary connection setup similar to pgx (thanks @jackc)
* Read raw binary data
* Supports profile and progress 
* database connection very like pgx (thanks @jackc)

# TODO
* Support ExternalTable
* Support Clickhouse Log
* Add a help page in wiki
* Add code generator

## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn?ref=badge_large)
