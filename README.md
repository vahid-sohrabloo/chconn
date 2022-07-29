[![Go Reference](https://pkg.go.dev/badge/github.com/vahid-sohrabloo/chconn.svg)](https://pkg.go.dev/github.com/vahid-sohrabloo/chconn)
[![codecov](https://codecov.io/gh/vahid-sohrabloo/chconn/branch/master/graph/badge.svg?token=K3JN6XWFVV)](https://codecov.io/gh/vahid-sohrabloo/chconn)
[![Go Report Card](https://goreportcard.com/badge/github.com/vahid-sohrabloo/chconn)](https://goreportcard.com/report/github.com/vahid-sohrabloo/chconn)
[![Actions Status](https://github.com/vahid-sohrabloo/chconn/workflows/CI/badge.svg)](https://github.com/vahid-sohrabloo/chconn/actions)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn?ref=badge_shield)

# chconn - ClickHouse low level Driver

chconn is a pure generic Go (1.18+) driver for [ClickHouse](https://clickhouse.com/) that use Native protocol
chconn aims to be low-level, fast, and performant.

For comparison with other libraries, please see https://github.com/jwilm0028/go-driver-benchmark/ and https://github.com/go-faster/ch-bench#benchmarks

If you have any suggestion or comment, please feel free to open an issue

## Example Usage
```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/vahid-sohrabloo/chconn/v2/chpool"
	"github.com/vahid-sohrabloo/chconn/v2/column"
)

func main() {
	conn, err := chpool.New(os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	// to check if the connection is alive
	err = conn.Ping(context.Background())
	if err != nil {
		panic(err)
	}

	err = conn.Exec(context.Background(), `DROP TABLE IF EXISTS example_table`)
	if err != nil {
		panic(err)
	}

	err = conn.Exec(context.Background(), `CREATE TABLE  example_table (
		uint64 UInt64,
		uint64_nullable Nullable(UInt64)
	) Engine=Memory`)
	if err != nil {
		panic(err)
	}

	col1 := column.New[uint64]()
	col2 := column.New[uint64]().Nullable()
	rows := 1_000_0000 // One hundred million rows- insert in 10 times
	numInsert := 10
	col1.SetWriteBufferSize(rows)
	col2.SetWriteBufferSize(rows)
	startInsert := time.Now()
	for i := 0; i < numInsert; i++ {
		col1.Reset()
		col2.Reset()
		for y := 0; y < rows; y++ {
			col1.Append(uint64(i))
			if i%2 == 0 {
				col2.Append(uint64(i))
			} else {
				col2.AppendNil()
			}
		}

		ctxInsert, cancelInsert := context.WithTimeout(context.Background(), time.Second*30)
		// insert data
		err = conn.Insert(ctxInsert, "INSERT INTO example_table (uint64,uint64_nullable) VALUES", col1, col2)
		if err != nil {
			cancelInsert()
			panic(err)
		}
		cancelInsert()
	}
	fmt.Println("inserted 10M rows in ", time.Since(startInsert))

	// select data
	col1Read := column.New[uint64]()
	col2Read := column.New[uint64]().Nullable()

	ctxSelect, cancelSelect := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelSelect()

	startSelect := time.Now()
	selectStmt, err := conn.Select(ctxSelect, "SELECT uint64,uint64_nullable FROM  example_table", col1Read, col2Read)
	if err != nil {
		panic(err)
	}

	// make sure the stmt close after select. but it's not necessary
	defer selectStmt.Close()

	var col1Data []uint64
	var col2DataNil []bool
	var col2Data []uint64
	// read data block by block
	// for more information about block, see: https://clickhouse.com/docs/en/development/architecture/#block
	for selectStmt.Next() {
		col1Data = col1Data[:0]
		col1Data = col1Read.Read(col1Data)

		col2DataNil = col2DataNil[:0]
		col2DataNil = col2Read.ReadNil(col2DataNil)

		col2Data = col2Data[:0]
		col2Data = col2Read.Read(col2Data)
	}

	// check errors
	if selectStmt.Err() != nil {
		panic(selectStmt.Err())
	}
	fmt.Println("selected 10M rows in ", time.Since(startSelect))
}
```
```
inserted 10M rows in  1.206666188s
selected 10M rows in  880.505004ms
```


**For moe information**, please see the [documentation](https://github.com/vahid-sohrabloo/chconn/wiki)
## Features
*   Generics (go1.18) for column types
*   Connection pool with after-connect hook for arbitrary connection setup similar to pgx (thanks @jackc)
*   Support DSN and Query connection string  (thanks @jackc)
*   Support All ClickHouse data types
*   Read and write data in column-oriented (like ClickHouse)
*   Do not use `interface{}` , `reflect`
*   Batch select and insert
*   Full TLS connection control
*   Read raw binary data
*   Supports profile and progress 
*   database url connection very like pgx (thanks @jackc)
*   Code generator for Insert
*   Support LZ4 compression protocol
*   Support execution telemetry streaming profiles and progress

## Supported types
*   UInt8, UInt16, UInt32, UInt64, UInt128, UInt256
*   Int8, Int16, Int32, Int64, Int128, Int256
*   Date, Date32, DateTime, DateTime64
*   Decimal32, Decimal64, Decimal128, Decimal256
*   IPv4, IPv6
*   String, FixedString(N)
*   UUID
*   Array(T)
*   Enums
*   LowCardinality(T)
*   Map(K, V)
*   Tuple(T1, T2, ..., Tn)
*   Nullable(T)
*   Point, Ring, Polygon, MultiPolygon



** 

## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn?ref=badge_large)
