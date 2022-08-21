package column_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v2"
	"github.com/vahid-sohrabloo/chconn/v2/column"
	"github.com/vahid-sohrabloo/chconn/v2/types"
)

func TestInsertColumnLowCardinalityError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := chconn.ParseConfig(connString)
	require.NoError(t, err)

	c, err := chconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_column_error_lc`)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_column_error_lc (
		col  LowCardinality(String)
	) Engine=Memory`)

	require.NoError(t, err)

	startValidReader := 3

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "write header",
			wantErr:     "block: write header block data for column col (timeout)",
			numberValid: startValidReader,
		},
		{
			name:        "write stype",
			wantErr:     "block: write block data for column col (error writing stype: timeout)",
			numberValid: startValidReader + 1,
		},
		{
			name:        "write dictionarySize",
			wantErr:     "block: write block data for column col (error writing dictionarySize: timeout)",
			numberValid: startValidReader + 2,
		},
		{
			name:        "write dictionary",
			wantErr:     "block: write block data for column col (error writing dictionary: timeout)",
			numberValid: startValidReader + 3,
		},
		{
			name:        "write keys len",
			wantErr:     "block: write block data for column col (error writing keys len: timeout)",
			numberValid: startValidReader + 4,
		},
		{
			name:        "write indices",
			wantErr:     "block: write block data for column col (error writing indices: timeout)",
			numberValid: startValidReader + 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.WriterFunc = func(w io.Writer) io.Writer {
				return &writerErrorHelper{
					err:         errors.New("timeout"),
					w:           w,
					numberValid: tt.numberValid,
				}
			}
			c, err = chconn.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			col := column.NewString().LowCardinality()
			col.Append("test")
			err = c.Insert(context.Background(),
				"insert into clickhouse_test_insert_column_error_lc (col) VALUES",
				col,
			)
			require.EqualError(t, errors.Unwrap(err), tt.wantErr)
			assert.True(t, c.IsClosed())
		})
	}
}

func TestSelectReadLCError(t *testing.T) {
	startValidReader := 35

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column name length",
			wantErr:     "read column header: read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column header: read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type length",
			wantErr:     "read column header: read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type error",
			wantErr:     "read column header: read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "error reading keys serialization version",
			wantErr:     "read column header: error reading keys serialization version: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "error reading serialization type",
			wantErr:     "read data toLowCardinality(toString(number)): error reading serialization type: timeout",
			numberValid: startValidReader + 5,
		},
		{
			name:        "error reading dictionary size",
			wantErr:     "read data toLowCardinality(toString(number)): error reading dictionary size: timeout",
			numberValid: startValidReader + 6,
		},
		{
			name:        "error reading dictionary",
			wantErr:     "read data toLowCardinality(toString(number)): error reading dictionary: error read string len: timeout",
			numberValid: startValidReader + 7,
		},
		{
			name:        "error reading string len",
			wantErr:     "read data toLowCardinality(toString(number)): error reading dictionary: error read string len: timeout",
			numberValid: startValidReader + 8,
		},
		{
			name:        "error reading string",
			wantErr:     "read data toLowCardinality(toString(number)): error reading dictionary: error read string: timeout",
			numberValid: startValidReader + 9,
		},
		{
			name:        "error reading indices size",
			wantErr:     "read data toLowCardinality(toString(number)): error reading indices size: timeout",
			numberValid: startValidReader + 10,
		},
		{
			name:        "error reading indices",
			wantErr:     "read data toLowCardinality(toString(number)): error reading indices: read data: timeout",
			numberValid: startValidReader + 11,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.NewString().LC()
			stmt, err := c.Select(context.Background(), "SELECT toLowCardinality(toString(number)) FROM system.numbers LIMIT 1;", col)
			require.NoError(t, err)
			stmt.Next()

			assert.EqualError(t, stmt.Err(), tt.wantErr)
		})
	}
}

func TestInsertColumnArrayError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := chconn.ParseConfig(connString)
	require.NoError(t, err)

	c, err := chconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_column_error_array`)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_column_error_array (
		col  Array(UInt8)
	) Engine=Memory`)

	require.NoError(t, err)

	startValidReader := 3

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "write block data",
			wantErr:     "block: write header block data for column col (timeout)",
			numberValid: startValidReader,
		},
		{
			name:        "write len data",
			wantErr:     "block: write block data for column col (write len data: timeout)",
			numberValid: startValidReader + 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.WriterFunc = func(w io.Writer) io.Writer {
				return &writerErrorHelper{
					err:         errors.New("timeout"),
					w:           w,
					numberValid: tt.numberValid,
				}
			}
			c, err = chconn.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			col := column.New[uint8]().Array()
			col.Append([]uint8{1})
			err = c.Insert(context.Background(),
				"insert into clickhouse_test_insert_column_error_array (col) VALUES",
				col,
			)
			require.EqualError(t, errors.Unwrap(err), tt.wantErr)
			assert.True(t, c.IsClosed())
		})
	}
}

func TestSelectReadArrayError(t *testing.T) {
	startValidReader := 35

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column name length",
			wantErr:     "read column header: read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column header: read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type length",
			wantErr:     "read column header: read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type error",
			wantErr:     "read column header: read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read offset error",
			wantErr:     "read data array(number, number): array: read offset column: read data: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "read data column",
			wantErr:     "read data array(number, number): array: read data column: read data: timeout",
			numberValid: startValidReader + 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.New[uint64]().Array()
			stmt, err := c.Select(context.Background(), "SELECT array(number,number) FROM system.numbers LIMIT 1;", col)
			require.NoError(t, err)
			stmt.Next()

			assert.EqualError(t, stmt.Err(), tt.wantErr)
		})
	}
}

func TestInsertColumnArrayNullable(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := chconn.ParseConfig(connString)
	require.NoError(t, err)

	c, err := chconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_column_error_array_nullable`)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_column_error_array_nullable (
		col  Array(Nullable(UInt8))
	) Engine=Memory`)

	require.NoError(t, err)

	startValidReader := 3

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "write block data",
			wantErr:     "block: write header block data for column col (timeout)",
			numberValid: startValidReader,
		},
		{
			name:        "write len data",
			wantErr:     "block: write block data for column col (write len data: timeout)",
			numberValid: startValidReader + 1,
		},
		{
			name:        "write nullable data",
			wantErr:     "block: write block data for column col (write nullable data: timeout)",
			numberValid: startValidReader + 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.WriterFunc = func(w io.Writer) io.Writer {
				return &writerErrorHelper{
					err:         errors.New("timeout"),
					w:           w,
					numberValid: tt.numberValid,
				}
			}
			c, err = chconn.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			col := column.New[uint8]().Nullable().Array()
			col.Append([]uint8{1})
			err = c.Insert(context.Background(),
				"insert into clickhouse_test_insert_column_error_array_nullable (col) VALUES",
				col,
			)
			require.EqualError(t, errors.Unwrap(err), tt.wantErr)
			assert.True(t, c.IsClosed())
		})
	}
}

func TestSelectReadArrayNullableError(t *testing.T) {
	startValidReader := 38

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column type error",
			wantErr:     "read column header: read column type: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read offset error",
			wantErr:     "read data array(toNullable(number)): array: read offset column: read data: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read data column",
			wantErr:     "read data array(toNullable(number)): array: read data column: read nullable data: read nullable data: timeout",
			numberValid: startValidReader + 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.New[uint64]().Nullable().Array()
			stmt, err := c.Select(context.Background(), "SELECT array(toNullable(number)) FROM system.numbers LIMIT 1;", col)
			require.NoError(t, err)
			stmt.Next()

			assert.EqualError(t, stmt.Err(), tt.wantErr)
		})
	}
}

func TestSelectReadNullableError(t *testing.T) {
	startValidReader := 38

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column type error",
			wantErr:     "read column header: read column type: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read nullable data",
			wantErr:     "read data toNullable(number): read nullable data: read nullable data: timeout",
			numberValid: startValidReader + 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.New[uint64]().Nullable()
			stmt, err := c.Select(context.Background(), "SELECT toNullable(number) FROM system.numbers LIMIT 1;", col)
			require.NoError(t, err)
			stmt.Next()

			assert.EqualError(t, stmt.Err(), tt.wantErr)
		})
	}
}

func TestInsertColumnArray2Error(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := chconn.ParseConfig(connString)
	require.NoError(t, err)

	c, err := chconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_column_error_array2`)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_column_error_array2 (
		col  Array(Array(UInt8))
	) Engine=Memory`)

	require.NoError(t, err)

	startValidReader := 3

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "write block data",
			wantErr:     "block: write header block data for column col (timeout)",
			numberValid: startValidReader,
		},
		{
			name:        "write len data",
			wantErr:     "block: write block data for column col (write len data: timeout)",
			numberValid: startValidReader + 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.WriterFunc = func(w io.Writer) io.Writer {
				return &writerErrorHelper{
					err:         errors.New("timeout"),
					w:           w,
					numberValid: tt.numberValid,
				}
			}
			c, err = chconn.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			col := column.New[uint8]().Array().Array()
			col.Append([][]uint8{{1}})
			err = c.Insert(context.Background(),
				"insert into clickhouse_test_insert_column_error_array2 (col) VALUES",
				col,
			)
			require.EqualError(t, errors.Unwrap(err), tt.wantErr)
			assert.True(t, c.IsClosed())
		})
	}
}

func TestSelectReadArray2Error(t *testing.T) {
	startValidReader := 35

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column name length",
			wantErr:     "read column header: read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column header: read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type length",
			wantErr:     "read column header: read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type error",
			wantErr:     "read column header: read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read offset error",
			wantErr:     "read data array(array(number, number)): array: read offset column: read data: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "read data column",
			wantErr:     "read data array(array(number, number)): array: read data column: array: read offset column: read data: timeout",
			numberValid: startValidReader + 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.New[uint64]().Array().Array()
			stmt, err := c.Select(context.Background(), "SELECT array(array(number,number)) FROM system.numbers LIMIT 1;", col)
			require.NoError(t, err)
			stmt.Next()

			assert.EqualError(t, stmt.Err(), tt.wantErr)
		})
	}
}
func TestInsertColumnArray3Error(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := chconn.ParseConfig(connString)
	require.NoError(t, err)

	c, err := chconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_column_error_array3`)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_column_error_array3 (
		col  Array(Array(Array(UInt8)))
	) Engine=Memory`)

	require.NoError(t, err)

	startValidReader := 3

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "write block data",
			wantErr:     "block: write header block data for column col (timeout)",
			numberValid: startValidReader,
		},
		{
			name:        "write len data",
			wantErr:     "block: write block data for column col (write len data: timeout)",
			numberValid: startValidReader + 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.WriterFunc = func(w io.Writer) io.Writer {
				return &writerErrorHelper{
					err:         errors.New("timeout"),
					w:           w,
					numberValid: tt.numberValid,
				}
			}
			c, err = chconn.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			col := column.New[uint8]().Array().Array().Array()
			col.Append([][][]uint8{{{1}}})
			err = c.Insert(context.Background(),
				"insert into clickhouse_test_insert_column_error_array3 (col) VALUES",
				col,
			)
			require.EqualError(t, errors.Unwrap(err), tt.wantErr)
			assert.True(t, c.IsClosed())
		})
	}
}

func TestSelectReadArray3Error(t *testing.T) {
	startValidReader := 35

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column header: read column name length",
			wantErr:     "read column header: read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column header: read column name",
			wantErr:     "read column header: read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column header: read column type length",
			wantErr:     "read column header: read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column header: read column type error",
			wantErr:     "read column header: read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read offset error",
			wantErr:     "read data array(array(array(number, number))): array: read offset column: read data: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "read data column",
			wantErr:     "read data array(array(array(number, number))): array: read data column: array: read offset column: read data: timeout",
			numberValid: startValidReader + 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.New[uint64]().Array().Array().Array()
			stmt, err := c.Select(context.Background(), "SELECT array(array(array(number,number))) FROM system.numbers LIMIT 1;", col)
			require.NoError(t, err)
			stmt.Next()

			assert.EqualError(t, stmt.Err(), tt.wantErr)
		})
	}
}

func TestInsertColumnTupleError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := chconn.ParseConfig(connString)
	require.NoError(t, err)

	c, err := chconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_column_error_tuple`)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_column_error_tuple (
		col  Tuple(String)
	) Engine=Memory`)

	require.NoError(t, err)

	startValidReader := 3

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "write header",
			wantErr:     "block: write header block data for column col (timeout)",
			numberValid: startValidReader,
		},
		{
			name:        "write columns",
			wantErr:     "block: write block data for column col (tuple: write column index 0: timeout)",
			numberValid: startValidReader + 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.WriterFunc = func(w io.Writer) io.Writer {
				return &writerErrorHelper{
					err:         errors.New("timeout"),
					w:           w,
					numberValid: tt.numberValid,
				}
			}
			c, err = chconn.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			col := column.NewString()
			colTuple := column.NewTuple(col)
			col.Append("test")
			err = c.Insert(context.Background(),
				"insert into clickhouse_test_insert_column_error_tuple (col) VALUES",
				colTuple,
			)
			require.EqualError(t, errors.Unwrap(err), tt.wantErr)
			assert.True(t, c.IsClosed())
		})
	}
}

func TestSelectReadTupleError(t *testing.T) {
	startValidReader := 35

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
		lc          bool
	}{
		{
			name:        "read column name length",
			wantErr:     "read column header: read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column header: read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type length",
			wantErr:     "read column header: read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type error",
			wantErr:     "read column header: read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read sub column header",
			wantErr:     "read column header: tuple: read column header index 0: error reading keys serialization version: timeout",
			numberValid: startValidReader + 4,
			lc:          true,
		},
		{
			name:        "read column index 2",
			wantErr:     "read data tuple(1): tuple: read column index 0: read data: timeout",
			numberValid: startValidReader + 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			// we can't use tupp[le(toLowCardinality('1')) so we use this tricky way
			// https://github.com/ClickHouse/ClickHouse/issues/39109
			var col column.ColumnBasic
			if tt.lc {
				col = column.New[uint64]().LC()
			} else {
				col = column.New[uint8]()
			}
			colTuple := column.NewTuple(col)
			stmt, err := c.Select(context.Background(), "SELECT tuple(1);", colTuple)
			require.NoError(t, err)
			stmt.Next()
			assert.EqualError(t, stmt.Err(), tt.wantErr)
		})
	}
}

func TestInsertColumnMapError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := chconn.ParseConfig(connString)
	require.NoError(t, err)

	c, err := chconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_column_error_map`)
	require.NoError(t, err)

	err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_column_error_map (
		col  Map(UInt8,UInt8)
	) Engine=Memory`)

	require.NoError(t, err)

	startValidReader := 3

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "write block data",
			wantErr:     "block: write header block data for column col (timeout)",
			numberValid: startValidReader,
		},
		{
			name:        "write len data",
			wantErr:     "block: write block data for column col (write len data: timeout)",
			numberValid: startValidReader + 1,
		},
		{
			name:        "write key data",
			wantErr:     "block: write block data for column col (write key data: timeout)",
			numberValid: startValidReader + 2,
		},
		{
			name:        "write value data",
			wantErr:     "block: write block data for column col (write value data: timeout)",
			numberValid: startValidReader + 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.WriterFunc = func(w io.Writer) io.Writer {
				return &writerErrorHelper{
					err:         errors.New("timeout"),
					w:           w,
					numberValid: tt.numberValid,
				}
			}
			c, err = chconn.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			colValue := column.New[uint8]()
			col := column.NewMap[uint8, uint8](column.New[uint8](), colValue)
			col.Append(map[uint8]uint8{1: 1})
			err = c.Insert(context.Background(),
				"insert into clickhouse_test_insert_column_error_map (col) VALUES",
				col,
			)
			require.EqualError(t, errors.Unwrap(err), tt.wantErr)
			assert.True(t, c.IsClosed())
		})
	}
}

func TestSelectReadMapError(t *testing.T) {
	startValidReader := 35

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
		lc          bool
	}{
		{
			name:        "read column name length",
			wantErr:     "read column header: read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column header: read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type length",
			wantErr:     "read column header: read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type error",
			wantErr:     "read column header: read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read value header",
			wantErr:     "read column header: map: read key header: error reading keys serialization version: timeout",
			numberValid: startValidReader + 4,
			lc:          true,
		},
		{
			name:        "read value header",
			wantErr:     "read column header: map: read value header: error reading keys serialization version: timeout",
			numberValid: startValidReader + 5,
			lc:          true,
		},
		{
			name:        "read offset error",
			wantErr:     "read data map(number, number): map: read offset column: read data: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "read key column",
			wantErr:     "read data map(number, number): map: read key column: read data: timeout",
			numberValid: startValidReader + 5,
		},
		{
			name:        "read value column",
			wantErr:     "read data map(number, number): map: read value column: read data: timeout",
			numberValid: startValidReader + 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			var colKey column.Column[uint64]
			var colValue column.Column[uint64]
			if tt.lc {
				colKey = column.New[uint64]().LC()
				colValue = column.New[uint64]().LC()
			} else {
				colKey = column.New[uint64]()
				colValue = column.New[uint64]()
			}
			col := column.NewMap(colKey, colValue)
			stmt, err := c.Select(context.Background(), "SELECT map(number,number) FROM system.numbers LIMIT 1;", col)
			require.NoError(t, err)
			stmt.Next()

			assert.EqualError(t, stmt.Err(), tt.wantErr)
		})
	}
}

func TestInvalidType(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := chconn.ParseConfig(connString)
	require.NoError(t, err)

	c, err := chconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	require.NoError(t, err)

	tests := []struct {
		name           string
		columnSelector string
		wantErr        string
		column         column.ColumnBasic
	}{
		{
			name:           "1 byte invalid",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: Int8|UInt8|Enum8",
			column:         column.New[int8](),
		},
		{
			name:           "2 bytes invalid",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: Int16|UInt16|Enum16|Date",
			column:         column.New[int16](),
		},
		{
			name:           "4 bytes invalid",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: Int32|UInt32|Float32|Decimal32|Date32|DateTime|IPv4",
			column:         column.New[int32](),
		},
		{
			name:           "8 bytes invalid",
			columnSelector: "toInt32(number)",
			wantErr:        "mismatch column type: ClickHouse Type: Int32, column types: Int64|UInt64|Float64|Decimal64|DateTime64",
			column:         column.New[int64](),
		},
		{
			name:           "16 bytes invalid",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: Int128|UInt128|Decimal128|IPv6|UUID",
			column:         column.New[types.Int128](),
		},
		{
			name:           "32 bytes invalid",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: Int256|UInt256|Decimal256",
			column:         column.New[types.Int256](),
		},
		{
			name:           "string invalid",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: String",
			column:         column.NewString(),
		},
		{
			name:           "fixed string invalid",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: T(20 bytes size)",
			column:         column.New[[20]byte](),
		},
		{
			name:           "fixed string invalid size",
			columnSelector: "toFixedString(toString(number),2)",
			wantErr:        "mismatch column type: ClickHouse Type: FixedString(2), column types: T(20 bytes size)",
			column:         column.New[[20]byte](),
		},
		{
			name:           "invalid nullable",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: Nullable(Int64|UInt64|Float64|Decimal64|DateTime64)",
			column:         column.New[int64]().Nullable(),
		},
		{
			name:           "invalid nullable inside",
			columnSelector: "toNullable(number)",
			wantErr:        "mismatch column type: ClickHouse Type: Nullable(UInt64), column types: Nullable(Int8|UInt8|Enum8)",
			column:         column.New[int8]().Nullable(),
		},
		{
			name:           "invalid LowCardinality",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: LowCardinality(Int64|UInt64|Float64|Decimal64|DateTime64)",
			column:         column.New[int64]().LC(),
		},
		{
			name:           "invalid LowCardinality inside",
			columnSelector: "toLowCardinality(number)",
			wantErr:        "mismatch column type: ClickHouse Type: LowCardinality(UInt64), column types: LowCardinality(Int8|UInt8|Enum8)",
			column:         column.New[int8]().LC(),
		},
		{
			name:           "invalid nullable LowCardinality",
			columnSelector: "number",
			wantErr: strings.Join([]string{
				"mismatch column type: ClickHouse Type: UInt64, column types: ",
				"LowCardinality(Nullable(Int64|UInt64|Float64|Decimal64|DateTime64))",
			}, ""),
			column: column.New[int64]().Nullable().LC(),
		},
		{
			name:           "invalid nullable LowCardinality inside",
			columnSelector: "toLowCardinality(toNullable(number))",
			wantErr: strings.Join([]string{
				"mismatch column type: ClickHouse Type: LowCardinality(Nullable(UInt64)), column types: ",
				"LowCardinality(Int8|UInt8|Enum8)",
			}, ""),
			column: column.New[int8]().LC(),
		},
		{
			name:           "invalid array",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: Array(Int64|UInt64|Float64|Decimal64|DateTime64)",
			column:         column.New[int64]().Array(),
		},
		{
			name:           "invalid array inside",
			columnSelector: "array(number)",
			wantErr:        "mismatch column type: ClickHouse Type: Array(UInt64), column types: Array(Int8|UInt8|Enum8)",
			column:         column.New[int8]().Array(),
		},
		{
			name:           "invalid array nullable",
			columnSelector: "array(number)",
			wantErr:        "mismatch column type: ClickHouse Type: Array(UInt64), column types: Array(Nullable(Int8|UInt8|Enum8))",
			column:         column.New[int8]().Nullable().Array(),
		},
		{
			name:           "invalid map",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: Map(Int8|UInt8|Enum8, Int8|UInt8|Enum8)",
			column:         column.NewMap[int8, int8](column.New[int8](), column.New[int8]()),
		},
		{
			name:           "invalid map key",
			columnSelector: "map(number,number)",
			wantErr:        "mismatch column type: ClickHouse Type: Map(UInt64, UInt64), column types: Map(Int8|UInt8|Enum8, Int8|UInt8|Enum8)",
			column:         column.NewMap[int8, int8](column.New[int8](), column.New[int8]()),
		},
		{
			name:           "invalid map value",
			columnSelector: "map(number,number)",
			wantErr: strings.Join([]string{
				"mismatch column type: ClickHouse Type: Map(UInt64, UInt64), column types: ",
				"Map(Int64|UInt64|Float64|Decimal64|DateTime64, Int8|UInt8|Enum8)",
			}, ""),
			column: column.NewMap[int64, int8](column.New[int64](), column.New[int8]()),
		},
		{
			name:           "invalid tuple",
			columnSelector: "number",
			wantErr: strings.Join([]string{
				"mismatch column type: ClickHouse Type: UInt64, column types: ",
				"Tuple(Int64|UInt64|Float64|Decimal64|DateTime64,Int8|UInt8|Enum8)",
			}, ""),
			column: column.NewTuple(column.New[int64](), column.New[int8]()),
		},
		{
			name:           "invalid tuple inside",
			columnSelector: "tuple(number)",
			wantErr:        "mismatch column type: ClickHouse Type: Tuple(UInt64), column types: Tuple(Int8|UInt8|Enum8)",
			column:         column.NewTuple(column.New[int8]()),
		},
		{
			name:           "invalid tuple columns",
			columnSelector: "tuple(number)",
			wantErr:        "columns number for tuple(number) (Tuple(UInt64)) is not equal to tuple columns number: 1 != 2",
			column:         column.NewTuple(column.New[uint64](), column.New[uint64]()),
		},
		{
			name:           "date time with timezone",
			columnSelector: "toDateTime('2010-01-01', 'America/New_York') + number",
			wantErr: strings.Join([]string{
				"mismatch column type: ClickHouse Type: DateTime('America/New_York'), column types: ",
				"Int64|UInt64|Float64|Decimal64|DateTime64",
			}, ""),
			column: column.New[uint64](),
		},
		{
			name:           "date time 64 with timezone",
			columnSelector: "toDateTime64('2010-01-01', 3, 'America/New_York') + number",
			wantErr: strings.Join([]string{
				"mismatch column type: ClickHouse Type: DateTime64(3, 'America/New_York'), column types: ",
				"Int32|UInt32|Float32|Decimal32|Date32|DateTime|IPv4",
			}, ""),
			column: column.New[uint32](),
		},
		{
			name:           "Decimal",
			columnSelector: "toDecimal32(number,3)",
			wantErr:        "mismatch column type: ClickHouse Type: Decimal(9, 3), column types: Int64|UInt64|Float64|Decimal64|DateTime64",
			column:         column.New[uint64](),
		},
		{
			name:           "Array2",
			columnSelector: "number",
			wantErr:        "mismatch column type: ClickHouse Type: UInt64, column types: Array(Array(Int64|UInt64|Float64|Decimal64|DateTime64))",
			column:         column.New[uint64]().Array().Array(),
		},
		{
			name:           "Array2 inside",
			columnSelector: "array(number,number)",
			wantErr: strings.Join([]string{
				"mismatch column type: ClickHouse Type: Array(UInt64), column types:",
				" Array(Array(Int64|UInt64|Float64|Decimal64|DateTime64))",
			}, ""),
			column: column.New[uint64]().Array().Array(),
		},
		{
			name:           "Array3",
			columnSelector: "number",
			wantErr: strings.Join([]string{
				"mismatch column type: ClickHouse Type: UInt64, column types: ",
				"Array(Array(Array(Int64|UInt64|Float64|Decimal64|DateTime64)))",
			}, ""),
			column: column.New[uint64]().Array().Array().Array(),
		},
		{
			name:           "Array3 inside",
			columnSelector: "array(number,number)",
			wantErr: strings.Join([]string{
				"mismatch column type: ClickHouse Type: Array(UInt64), column types: ",
				"Array(Array(Array(Int64|UInt64|Float64|Decimal64|DateTime64)))",
			}, ""),
			column: column.New[uint64]().Array().Array().Array(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err = chconn.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			stmt, err := c.Select(context.Background(),
				fmt.Sprintf("SELECT %s FROM  system.numbers limit 1", tt.columnSelector),
				tt.column,
			)

			require.NoError(t, err)
			for stmt.Next() {

			}
			require.EqualError(t, errors.Unwrap(stmt.Err()), tt.wantErr)
			assert.True(t, c.IsClosed())
		})
	}
}

func TestMapInvalidColumnNumber(t *testing.T) {
	m := column.NewMap[uint8, uint8](column.New[uint8](), column.New[uint8]())
	m.SetType([]byte("Map(UInt8,UInt8,UInt8)"))
	err := m.Validate()
	assert.Equal(t, err.Error(), "columns number is not equal to map columns number: 3 != 2")
}

func TestFixedStringInvalidType(t *testing.T) {
	m := column.New[[20]byte]()
	m.SetType([]byte("FixedString(a)"))
	err := m.Validate()
	assert.Equal(t, err.Error(), "invalid size: strconv.Atoi: parsing \"a\": invalid syntax")
}

func TestDecimalInvalidType(t *testing.T) {
	m := column.New[[20]byte]()
	m.SetType([]byte("Decimal()"))
	err := m.Validate()
	assert.Equal(t, err.Error(), "invalid decimal type (should have precision and scale): Decimal()")

	m.SetType([]byte("Decimal(a, a)"))
	err = m.Validate()
	assert.Equal(t, err.Error(), "invalid precision: strconv.Atoi: parsing \"a\": invalid syntax")

	m.SetType([]byte("Decimal(3, a)"))
	err = m.Validate()
	assert.Equal(t, err.Error(), "invalid scale: strconv.Atoi: parsing \"a\": invalid syntax")

	m.SetType([]byte("Decimal(200, 3)"))
	err = m.Validate()
	assert.Equal(t, err.Error(), "invalid precision: 200. it should be between 1 and 76")
}

func TestInvalidDate(t *testing.T) {
	m := column.NewDate[types.DateTime]()
	m.SetType([]byte("DateTime('InvalidTimeZone')"))
	err := m.Validate()
	assert.NoError(t, err)
	assert.Equal(t, m.Location(), time.Local)
}

func TestInvalidSimpleAggregateFunction(t *testing.T) {
	m := column.New[int]()
	m.SetType([]byte("SimpleAggregateFunction(sum))"))
	assert.Panics(t, func() {
		m.Validate()
	})
}
