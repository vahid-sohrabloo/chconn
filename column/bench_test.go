package column_test

import (
	"context"
	"testing"

	"github.com/vahid-sohrabloo/chconn/v2"
	"github.com/vahid-sohrabloo/chconn/v2/column"
)

func BenchmarkTestChconnSelect100MUint64(b *testing.B) {
	// return
	ctx := context.Background()
	c, err := chconn.Connect(ctx, "password=salam")
	if err != nil {
		b.Fatal(err)
	}
	// var datStr [][]byte
	colRead := column.New[uint64]()
	for n := 0; n < b.N; n++ {
		s, err := c.Select(ctx, "SELECT number FROM system.numbers_mt LIMIT 100000000", colRead)
		if err != nil {
			b.Fatal(err)
		}

		// colReadStr := column.NewString(false)x
		for s.Next() {
			// if err := s.ReadColumns(colRead); err != nil {
			// 	b.Fatal(err)
			// }
			colRead.Data()
			// if err := s.NextColumn(colReadStr); err != nil {
			// 	b.Fatal(err)
			// }
			// datStr = datStr[:0]
			// colReadStr.ReadAll(&datStr)
		}
		if err := s.Err(); err != nil {
			b.Fatal(err)
		}
		s.Close()
	}
}

func BenchmarkTestChconnSelect1MString(b *testing.B) {
	// return
	ctx := context.Background()
	c, err := chconn.Connect(ctx, "password=salam")
	if err != nil {
		b.Fatal(err)
	}
	// var datStr [][]byte
	colRead := column.NewString()
	var data [][]byte
	for n := 0; n < b.N; n++ {
		s, err := c.Select(ctx, "SELECT randomString(20) FROM system.numbers_mt LIMIT 1000000", colRead)
		if err != nil {
			b.Fatal(err)
		}

		for s.Next() {
			data = data[:0]
			colRead.DataBytes()
		}
		if err := s.Err(); err != nil {
			b.Fatal(err)
		}
		s.Close()
	}
}

func BenchmarkTestChconnInsert10M(b *testing.B) {
	// return
	ctx := context.Background()
	c, err := chconn.Connect(ctx, "password=salam")
	if err != nil {
		b.Fatal(err)
	}
	err = c.Exec(ctx, "DROP TABLE IF EXISTS test_insert_chconn")
	if err != nil {
		b.Fatal(err)
	}
	err = c.Exec(ctx, "CREATE TABLE test_insert_chconn (id UInt64) ENGINE = Null")
	if err != nil {
		b.Fatal(err)
	}

	const (
		rowsInBlock = 10_000_000
	)

	idColumns := column.New[uint64]()
	idColumns.SetWriteBufferSize(rowsInBlock)
	// vColumns := column.NewString(false)
	for n := 0; n < b.N; n++ {
		for y := 0; y < rowsInBlock; y++ {
			idColumns.Append(1)
			// vColumns.Append([]byte("test"))
		}
		err := c.Insert(ctx, "INSERT INTO test_insert_chconn VALUES", idColumns)
		if err != nil {
			b.Fatal(err)
		}
	}
}
