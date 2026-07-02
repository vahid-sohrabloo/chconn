[![Go Reference](https://pkg.go.dev/badge/github.com/vahid-sohrabloo/chconn/v3.svg)](https://pkg.go.dev/github.com/vahid-sohrabloo/chconn/v3)
[![codecov](https://codecov.io/gh/vahid-sohrabloo/chconn/branch/master/graph/badge.svg?token=K3JN6XWFVV)](https://codecov.io/gh/vahid-sohrabloo/chconn)
[![Go Report Card](https://goreportcard.com/badge/github.com/vahid-sohrabloo/chconn/v3)](https://goreportcard.com/report/github.com/vahid-sohrabloo/chconn/v3)
[![Actions Status](https://github.com/vahid-sohrabloo/chconn/workflows/CI/badge.svg)](https://github.com/vahid-sohrabloo/chconn/actions)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn?ref=badge_shield)

# chconn - ClickHouse Native Protocol Driver for Go

chconn is a pure Go driver for [ClickHouse](https://clickhouse.com/) using the native TCP protocol. It is designed for high-performance, column-oriented data access with full generics support.

If you have any suggestion or comment, please feel free to open an issue.

## Installation

```bash
go get github.com/vahid-sohrabloo/chconn/v3
```

Requires the latest two stable Go releases.

## Quick Start

```go
package main

import (
	"context"
	"fmt"

	"github.com/vahid-sohrabloo/chconn/v3/chpool"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func main() {
	conn, err := chpool.New("clickhouse://default:@localhost:9000/default")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Create table
	err = conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS example (
		id    UInt64,
		name  String
	) Engine=Memory`)
	if err != nil {
		panic(err)
	}

	// Insert
	colID := column.New[uint64]()
	colName := column.NewString()
	colID.Append(1)
	colName.Append("Alice")
	colID.Append(2)
	colName.Append("Bob")

	err = conn.Insert(context.Background(), "INSERT INTO example (id, name) VALUES", colID, colName)
	if err != nil {
		panic(err)
	}

	// Select
	colIDRead := column.New[uint64]()
	colNameRead := column.NewString()
	selectStmt, err := conn.Select(context.Background(), "SELECT id, name FROM example", colIDRead, colNameRead)
	if err != nil {
		panic(err)
	}
	defer selectStmt.Close()

	for selectStmt.Next() {
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			fmt.Printf("id=%d name=%s\n", colIDRead.Row(i), colNameRead.Row(i))
		}
	}
	if err := selectStmt.Err(); err != nil {
		panic(err)
	}
}
```

## Core Patterns

### Column-Oriented Insert

The fastest way to insert data. Append values to typed columns and insert in bulk:

```go
col1 := column.New[uint64]()
col2 := column.NewString()
col3 := column.New[uint64]().Nullable()

// Pre-allocate write buffers for large inserts
col1.SetWriteBufferSize(1_000_000)
col2.SetWriteBufferSize(1_000_000)
col3.SetWriteBufferSize(1_000_000)

for i := range 1_000_000 {
    col1.Append(uint64(i))
    col2.Append(fmt.Sprintf("row_%d", i))
    if i%2 == 0 {
        col3.Append(uint64(i))
    } else {
        col3.AppendNil()
    }
}

err := conn.Insert(ctx, "INSERT INTO table (id, name, value) VALUES", col1, col2, col3)
```

### Column-Oriented Select

Read data block by block for maximum throughput:

```go
colID := column.New[uint64]()
colName := column.NewString()

selectStmt, err := conn.Select(ctx, "SELECT id, name FROM table", colID, colName)
if err != nil {
    return err
}
defer selectStmt.Close()

var ids []uint64
var names []string
for selectStmt.Next() {
    ids = colID.Read(ids)
    names = colName.Read(names)
}
if err := selectStmt.Err(); err != nil {
    return err
}
```

### Row-Oriented Query

Familiar SQL-style row iteration with `Query` and `Scan`:

```go
rows, err := conn.Query(ctx, "SELECT id, name FROM users WHERE age > {age: UInt32}",
    chconn.IntParameter("age", 18),
)
if err != nil {
    return err
}
defer rows.Close()

for rows.Next() {
    var id uint64
    var name string
    if err := rows.Scan(&id, &name); err != nil {
        return err
    }
    fmt.Printf("%d: %s\n", id, name)
}
return rows.Err()
```

### Struct Scanning

Map results directly to Go structs by field name:

```go
type User struct {
    ID    uint64
    Name  string
    Email string
}

// Collect all rows into a slice
users, err := chconn.QueryAll[User](ctx, conn, "SELECT id, name, email FROM users")

// Get exactly one row
user, err := chconn.QueryExactlyOne[User](ctx, conn,
    "SELECT id, name, email FROM users WHERE id = {id: UInt64}",
    chconn.UintParameter("id", 42),
)

// Iterator pattern
for user, err := range chconn.QueryIter[User](ctx, conn, "SELECT id, name, email FROM users") {
    if err != nil {
        return err
    }
    fmt.Println(user.Name)
}
```

Scanning options:
- `RowToStructByName[T]` — match columns to struct fields by name (case-insensitive)
- `RowToStructByNameLax[T]` — same, but tolerates extra/missing fields
- `RowToStructByPos[T]` — match by column position
- `RowTo[T]` — auto-detect: struct by name, `map[string]any`, or scalar

### Streaming Insert

For multi-batch inserts or row-by-row appending:

```go
stmt, err := conn.InsertStream(ctx, "INSERT INTO table (id, name) VALUES")
if err != nil {
    return err
}

// Row-by-row append
for i := range 1000 {
    if err := stmt.Append(uint64(i), fmt.Sprintf("name_%d", i)); err != nil {
        return err
    }
}

// Or write column batches
col1 := column.New[uint64]()
col2 := column.NewString()
col1.Append(1001)
col2.Append("batch_row")
if err := stmt.Write(ctx, col1, col2); err != nil {
    return err
}

return stmt.Flush(ctx)
```

### Parameterized Queries

Type-safe query parameters using ClickHouse native parameter syntax:

```go
rows, err := conn.Query(ctx,
    "SELECT * FROM events WHERE ts > {start: DateTime} AND level = {level: String}",
    chconn.StringParameter("start", "2024-01-01 00:00:00"),
    chconn.StringParameter("level", "error"),
)
```

Available parameter functions: `IntParameter`, `UintParameter`, `Float32Parameter`, `Float64Parameter`, `StringParameter`, and their slice variants (`IntSliceParameter`, etc.).

## Features

### Connection Pool

```go
pool, err := chpool.New("clickhouse://user:password@localhost:9000/mydb")
// or with full config:
config, _ := chpool.ParseConfig("clickhouse://user:password@localhost:9000/mydb")
config.MaxConns = 10
config.MinConns = 2
config.MaxConnLifetime = time.Hour
config.MaxConnIdleTime = 30 * time.Minute
config.HealthCheckPeriod = time.Minute

pool, err := chpool.NewWithConfig(ctx, config)
```

The pool implements the same `Select`, `Insert`, `Query`, `Exec` methods as a single connection.

### Supported Types

| ClickHouse Type | Go Column |
|---|---|
| Bool | `column.New[bool]()` |
| UInt8/16/32/64/128/256 | `column.New[uint8]()`, `column.New[types.Uint128]()`, etc. |
| Int8/16/32/64/128/256 | `column.New[int8]()`, `column.New[types.Int128]()`, etc. |
| Float32/64 | `column.New[float32]()`, `column.New[float64]()` |
| BFloat16 | `column.New[types.BFloat16]()` |
| Decimal32/64/128/256 | `column.New[types.Decimal32]()`, etc. |
| String | `column.NewString()` |
| FixedString(N) | `column.New[[N]byte]()` |
| Date, Date32 | `column.NewDate[types.Date]()`, `column.NewDate[types.Date32]()` |
| DateTime, DateTime64 | `column.NewDate[types.DateTime]()`, `column.NewDate[types.DateTime64]()` |
| Time, Time64 | `column.New[types.ChTime]()`, `column.New[types.ChTime64]()` |
| UUID | `column.New[types.UUID]()` |
| IPv4, IPv6 | `column.New[types.IPv4]()`, `column.New[types.IPv6]()` |
| Enum8, Enum16 | `column.New[int8]()`, `column.New[int16]()` |
| Array(T) | `col.Array()` |
| Nullable(T) | `col.Nullable()` |
| LowCardinality(T) | `col.LowCardinality()` |
| Map(K, V) | `column.NewMap[K, V](keyCol, valCol)` |
| Tuple(T1, ..., Tn) | `column.NewTuple(cols...)` or typed `column.NewTuple2[T](col1, col2)` |
| Nested | `column.NewNested(cols...)` |
| JSON | `column.NewJSON()` |
| Variant(T1, ..., Tn) | `column.NewVariant(cols...)` |
| Dynamic | `column.NewDynamic(cols...)` |
| Point, Ring, Polygon, MultiPolygon | `column.NewPoint()` |
| Nothing | `column.NewNothing()` |

### Compression

```go
// LZ4 (recommended)
config.Compress = chconn.CompressLZ4

// ZSTD
config.Compress = chconn.CompressZSTD
```

Or via connection string: `clickhouse://localhost:9000/mydb?compress=lz4`

### TLS/SSL

```go
clickhouse://user:pass@host:9440/db?sslmode=verify-full&sslrootcert=/path/to/ca.pem
```

SSL modes: `disable`, `allow`, `prefer` (default), `require`, `verify-ca`, `verify-full`

### Code Generator (chgen)

Generate Go model structs and column boilerplate from ClickHouse tables:

```bash
# Install
go install github.com/vahid-sohrabloo/chconn/v3/cmd/chgen@latest

# Generate model from live table
chgen model -dsn "clickhouse://localhost:9000" -table users -out models/user.go

# Generate model from SQL file
chgen model -sql create_users.sql -out models/user.go

# Generate column declarations for insert/select
chgen columns -dsn "clickhouse://localhost:9000" -table users -out columns/user_columns.go
```

### SQL Builder

```go
sb := sqlbuilder.NewSelectBuilder()
query, _ := sb.Select("id", "name", "email").
    From("users").
    Where("age > 18").
    PreWhere("active = 1").
    OrderBy("name").
    Limit(100).
    Build()
```

Supports: `Select`, `From`, `Where`, `PreWhere`, `Having`, `GroupBy`, `OrderBy`, `Limit`, `Offset`, `Distinct`, `Final`, `Join` (all types), `ArrayJoin`.

### JSON Export

Export query results as JSON:

```go
exporter := format.NewJSON(1000, func(data []byte, cols []column.ColumnCore) {
    os.Stdout.Write(data)
})
selectStmt, _ := conn.Select(ctx, "SELECT * FROM table", cols...)
exporter.Read(selectStmt)        // JSON objects
exporter.ReadCompact(selectStmt) // JSON arrays (compact)
exporter.ReadEachRow(selectStmt) // One JSON object per row
```

### Progress and Profile Callbacks

Monitor query execution in real time:

```go
opts := &chconn.QueryOptions{
    QueryID: "my-query-123",
    OnProgress: func(p *chconn.Progress) {
        fmt.Printf("Read %d rows, %d bytes\n", p.ReadRows, p.ReadBytes)
    },
    OnProfile: func(p *chconn.Profile) {
        fmt.Printf("Result: %d rows in %d blocks\n", p.Rows, p.Blocks)
    },
}
selectStmt, err := conn.SelectWithOption(ctx, "SELECT ...", opts, cols...)
```

### Error Handling

```go
var chErr *chconn.ChError
if errors.As(err, &chErr) {
    fmt.Printf("ClickHouse error %d: %s\n", chErr.Code, chErr.Message)
    fmt.Println(chErr.StackTrace)
}

// Sentinel errors
if errors.Is(err, chconn.ErrNoRows) { ... }
if errors.Is(err, chconn.ErrTooManyRows) { ... }
```

### Context Support

All operations accept `context.Context` for timeouts and cancellation:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

err := conn.Insert(ctx, "INSERT INTO ...", cols...)
```

## Supported Versions

- **Go**: latest two stable releases
- **ClickHouse**: 24.3+

Tested in CI against ClickHouse 24.3, 24.8, 25.3, 25.8, and 26.2.

## Documentation

For more information, please see the [documentation](https://github.com/vahid-sohrabloo/chconn/wiki) and [Go package reference](https://pkg.go.dev/github.com/vahid-sohrabloo/chconn/v3).

## Benchmarks

For comparison with other Go ClickHouse drivers, see the [benchmark repository](https://github.com/vahid-sohrabloo/go-ch-benchmark) and [ch-bench results](https://github.com/go-faster/ch-bench#benchmarks).

```
name \ time/op            chconn       chgo       go-clickhouse     uptrace
TestSelect100MUint64-16    150ms      154ms           8019ms         3045ms
TestSelect10MString-16     271ms      447ms            969ms          822ms
TestInsert10M-16           198ms      514ms            561ms          304ms

name \ alloc/op           chconn       chgo       go-clickhouse     uptrace
TestSelect100MUint64-16    111kB      262kB       3202443kB        800941kB
TestSelect10MString-16    1.63MB     1.79MB        1626.51MB       241.03MB
TestInsert10M-16          26.0MB    283.7MB         1680.4MB        240.2MB

name \ allocs/op          chconn       chgo       go-clickhouse     uptrace
TestSelect100MUint64-16      35       6683       200030937       100006069
TestSelect10MString-16       49       1748        30011991        20001120
TestInsert10M-16             26         80             224              50
```

## License

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fvahid-sohrabloo%2Fchconn?ref=badge_large)
