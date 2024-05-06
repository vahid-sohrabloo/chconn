package column_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/types"
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
	startValidReader := 38

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column name length",
			wantErr:     "read column header \"\": read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column header \"\x00\": read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type length",
			wantErr:     "read column header \"t\": read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type error",
			wantErr:     "read column header \"t\": read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read custom serialization",
			wantErr:     "read column header \"t\": read custom serialization: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "error reading keys serialization version",
			wantErr:     "read column header \"t\": error reading keys serialization version: timeout",
			numberValid: startValidReader + 5,
		},
		{
			name:        "error reading serialization type",
			wantErr:     "read data \"t\": error reading serialization type: timeout",
			numberValid: startValidReader + 6,
		},
		{
			name:        "error reading dictionary size",
			wantErr:     "read data \"t\": error reading dictionary size: timeout",
			numberValid: startValidReader + 7,
		},
		{
			name:        "error reading dictionary",
			wantErr:     "read data \"t\": error reading dictionary: error read string len: timeout",
			numberValid: startValidReader + 8,
		},
		{
			name:        "error reading string len",
			wantErr:     "read data \"t\": error reading dictionary: error read string len: timeout",
			numberValid: startValidReader + 9,
		},
		{
			name:        "error reading string",
			wantErr:     "read data \"t\": error reading dictionary: error read string: timeout",
			numberValid: startValidReader + 10,
		},
		{
			name:        "error reading indices size",
			wantErr:     "read data \"t\": error reading indices size: timeout",
			numberValid: startValidReader + 11,
		},
		{
			name:        "error reading indices",
			wantErr:     "read data \"t\": error reading indices: read data: timeout",
			numberValid: startValidReader + 12,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader, c chconn.Conn) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.NewString().LC()
			stmt, err := c.Select(context.Background(), "SELECT toLowCardinality(toString(number)) as t FROM system.numbers LIMIT 1;", col)
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
	startValidReader := 38

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column name length",
			wantErr:     "read column header \"\": read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column header \"\x00\": read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type length",
			wantErr:     "read column header \"t\": read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type error",
			wantErr:     "read column header \"t\": read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read custom serialization",
			wantErr:     "read column header \"t\": read custom serialization: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "read offset error",
			wantErr:     "read data \"t\": array: read offset column: read data: timeout",
			numberValid: startValidReader + 5,
		},
		{
			name:        "read data column",
			wantErr:     "read data \"t\": array: read data column: read data: timeout",
			numberValid: startValidReader + 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader, c chconn.Conn) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.New[uint64]().Array()
			stmt, err := c.Select(context.Background(), "SELECT array(number,number) as t FROM system.numbers LIMIT 1;", col)
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
	startValidReader := 41

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column type error",
			wantErr:     "read column header \"t\": read column type: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read custom serialization",
			wantErr:     "read column header \"t\": read custom serialization: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read offset error",
			wantErr:     "read data \"t\": array: read offset column: read data: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read data column",
			wantErr:     "read data \"t\": array: read data column: read nullable data: read nullable data: timeout",
			numberValid: startValidReader + 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader, c chconn.Conn) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.New[uint64]().Nullable().Array()
			stmt, err := c.Select(context.Background(), "SELECT array(toNullable(number)) as t FROM system.numbers LIMIT 1;", col)
			require.NoError(t, err)
			stmt.Next()

			assert.EqualError(t, stmt.Err(), tt.wantErr)
		})
	}
}

func TestSelectReadNullableError(t *testing.T) {
	startValidReader := 41

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column type error",
			wantErr:     "read column header \"t\": read column type: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read custom serialization",
			wantErr:     "read column header \"t\": read custom serialization: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read nullable data",
			wantErr:     "read data \"t\": read nullable data: read nullable data: timeout",
			numberValid: startValidReader + 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader, c chconn.Conn) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.New[uint64]().Nullable()
			stmt, err := c.Select(context.Background(), "SELECT toNullable(number) as t FROM system.numbers LIMIT 1;", col)
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
	startValidReader := 38

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column name length",
			wantErr:     "read column header \"\": read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column header \"\x00\": read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type length",
			wantErr:     "read column header \"t\": read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type error",
			wantErr:     "read column header \"t\": read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read custom serialization",
			wantErr:     "read column header \"t\": read custom serialization: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "read offset error",
			wantErr:     "read data \"t\": array: read offset column: read data: timeout",
			numberValid: startValidReader + 5,
		},
		{
			name:        "read data column",
			wantErr:     "read data \"t\": array: read data column: array: read offset column: read data: timeout",
			numberValid: startValidReader + 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader, c chconn.Conn) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.New[uint64]().Array().Array()
			stmt, err := c.Select(context.Background(), "SELECT array(array(number,number)) as t FROM system.numbers LIMIT 1;", col)
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
	startValidReader := 38

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column header \"\": read column name length",
			wantErr:     "read column header \"\": read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column header \"\": read column name",
			wantErr:     "read column header \"\x00\": read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column header \"t\": read column type length",
			wantErr:     "read column header \"t\": read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column header \"t\": read column type error",
			wantErr:     "read column header \"t\": read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read custom serialization",
			wantErr:     "read column header \"t\": read custom serialization: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "read offset error",
			wantErr:     "read data \"t\": array: read offset column: read data: timeout",
			numberValid: startValidReader + 5,
		},
		{
			name:        "read data column",
			wantErr:     "read data \"t\": array: read data column: array: read offset column: read data: timeout",
			numberValid: startValidReader + 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader, c chconn.Conn) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			col := column.New[uint64]().Array().Array().Array()
			stmt, err := c.Select(context.Background(), "SELECT array(array(array(number,number))) as t FROM system.numbers LIMIT 1;", col)
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
	startValidReader := 38

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
		lc          bool
	}{
		{
			name:        "read column name length",
			wantErr:     "read column header \"\": read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column header \"\x00\": read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type length",
			wantErr:     "read column header \"t\": read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type error",
			wantErr:     "read column header \"t\": read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read custom serialization",
			wantErr:     "read column header \"t\": read custom serialization: timeout",
			numberValid: startValidReader + 4,
			lc:          true,
		},
		{
			name:        "read sub column header",
			wantErr:     "read column header \"t\": tuple: read column header index 0: error reading keys serialization version: timeout",
			numberValid: startValidReader + 5,
			lc:          true,
		},
		{
			name:        "read column index 2",
			wantErr:     "read data \"t\": tuple: read column index 0: read data: timeout",
			numberValid: startValidReader + 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader, c chconn.Conn) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := chconn.ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			// we can't use tuple(toLowCardinality('1')) so we use this tricky way
			// https://github.com/ClickHouse/ClickHouse/issues/39109
			var col column.ColumnBasic
			if tt.lc {
				col = column.New[uint64]().LC()
			} else {
				col = column.New[uint8]()
			}
			colTuple := column.NewTuple(col)
			stmt, err := c.Select(context.Background(), "SELECT tuple(1) as t;", colTuple)
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
	startValidReader := 38

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
		lc          bool
	}{
		{
			name:        "read column name length",
			wantErr:     "read column header \"\": read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column header \"\x00\": read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type length",
			wantErr:     "read column header \"t\": read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type error",
			wantErr:     "read column header \"t\": read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read custom serialization",
			wantErr:     "read column header \"t\": read custom serialization: timeout",
			numberValid: startValidReader + 4,
			lc:          true,
		},
		{
			name:        "read value header",
			wantErr:     "read column header \"t\": map: read key header: error reading keys serialization version: timeout",
			numberValid: startValidReader + 5,
			lc:          true,
		},
		{
			name:        "read value header",
			wantErr:     "read column header \"t\": map: read value header: error reading keys serialization version: timeout",
			numberValid: startValidReader + 6,
			lc:          true,
		},
		{
			name:        "read offset error",
			wantErr:     "read data \"t\": map: read offset column: read data: timeout",
			numberValid: startValidReader + 5,
		},
		{
			name:        "read key column",
			wantErr:     "read data \"t\": map: read key column: read data: timeout",
			numberValid: startValidReader + 6,
		},
		{
			name:        "read value column",
			wantErr:     "read data \"t\": map: read value column: read data: timeout",
			numberValid: startValidReader + 7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := chconn.ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader, c chconn.Conn) io.Reader {
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
			stmt, err := c.Select(context.Background(), "SELECT map(number,number) as t FROM system.numbers LIMIT 1;", col)
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

	tests := []struct {
		name           string
		columnSelector string
		chType         string
		goToChType     string
		goType         string
		column         column.ColumnBasic
		skip           func(chconn.Conn) bool
		fullErr        string
	}{
		{
			name:           "1 byte invalid",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "Int8|UInt8|Enum8",
			goType:         "column.Base[int8]",
			column:         column.New[int8]().SetStrict(false),
		},
		{
			name:           "2 bytes invalid",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "Int16|UInt16|Enum16|Date",
			goType:         "column.Base[int16]",
			column:         column.New[int16]().SetStrict(false),
		},
		{
			name:           "4 bytes invalid",
			columnSelector: "number",
			chType:         "UInt64",
			goType:         "column.Base[int32]",
			goToChType:     "Int32|UInt32|Float32|Decimal32|Date32|DateTime|IPv4",
			column:         column.New[int32]().SetStrict(false),
		},
		{
			name:           "8 bytes invalid",
			columnSelector: "toInt32(number)",
			chType:         "Int32",
			goToChType:     "Int64|UInt64|Float64|Decimal64|DateTime64",
			goType:         "column.Base[int64]",
			column:         column.New[int64]().SetStrict(false),
		},
		{
			name:           "16 bytes invalid",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "Int128|UInt128|Decimal128|IPv6|UUID",
			goType:         "column.Base[types.Int128]",
			column:         column.New[types.Int128]().SetStrict(false),
		},
		{
			name:           "32 bytes invalid",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "Int256|UInt256|Decimal256",
			goType:         "column.Base[types.Int256]",
			column:         column.New[types.Int256]().SetStrict(false),
		},
		{
			name:           "string invalid",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "String",
			goType:         "column.StringBase[string]",
			column:         column.NewString(),
		},
		{
			name:           "fixed string invalid",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "T(20 bytes size)",
			goType:         "column.Base[[20]uint8]",
			column:         column.New[[20]byte]().SetStrict(false),
		},
		{
			name:           "fixed string invalid size",
			columnSelector: "toFixedString(toString(number),2)",
			chType:         "FixedString(2)",
			goToChType:     "T(20 bytes size)",
			goType:         "column.Base[[20]uint8]",
			column:         column.New[[20]byte]().SetStrict(false),
		},
		{
			name:           "invalid nullable",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "Nullable(Int64|UInt64|Float64|Decimal64|DateTime64)",
			goType:         "column.BaseNullable[int64]",
			column:         column.New[int64]().SetStrict(false).Nullable(),
		},
		{
			name:           "invalid nullable inside",
			columnSelector: "toNullable(number)",
			chType:         "Nullable(UInt64)",
			goToChType:     "Nullable(Int8|UInt8|Enum8)",
			goType:         "column.BaseNullable[int8]",
			column:         column.New[int8]().SetStrict(false).Nullable(),
		},
		{
			name:           "invalid LowCardinality",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "LowCardinality(Int64|UInt64|Float64|Decimal64|DateTime64)",
			goType:         "column.LowCardinality[int64]",
			column:         column.New[int64]().SetStrict(false).LC(),
		},
		{
			name:           "invalid LowCardinality inside",
			columnSelector: "toLowCardinality(number)",
			chType:         "LowCardinality(UInt64)",
			goToChType:     "LowCardinality(Int8|UInt8|Enum8)",
			goType:         "column.LowCardinality[int8]",
			column:         column.New[int8]().SetStrict(false).LC(),
		},
		{
			name:           "invalid nullable LowCardinality",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "LowCardinality(Nullable(Int64|UInt64|Float64|Decimal64|DateTime64))",
			goType:         "column.LowCardinalityNullable[int64]",
			column:         column.New[int64]().SetStrict(false).LC().Nullable(),
		},
		{
			name:           "invalid nullable LowCardinality inside",
			columnSelector: "toLowCardinality(toNullable(number))",
			chType:         "LowCardinality(Nullable(UInt64))",
			goToChType:     "LowCardinality(Int8|UInt8|Enum8)",
			goType:         "column.LowCardinality[int8]",
			column:         column.New[int8]().SetStrict(false).LC(),
		},
		{
			name:           "invalid array",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "Array(Int64|UInt64|Float64|Decimal64|DateTime64)",
			goType:         "column.Array[int64]",
			column:         column.New[int64]().SetStrict(false).Array(),
		},
		{
			name:           "invalid array inside",
			columnSelector: "array(number)",
			chType:         "Array(UInt64)",
			goToChType:     "Array(Int8|UInt8|Enum8)",
			goType:         "column.Array[int8]",
			column:         column.New[int8]().SetStrict(false).Array(),
		},
		{
			name:           "invalid array nullable",
			columnSelector: "array(number)",
			chType:         "Array(UInt64)",
			goToChType:     "Array(Nullable(Int8|UInt8|Enum8))",
			goType:         "column.ArrayNullable[int8]",
			column:         column.New[int8]().SetStrict(false).Nullable().Array(),
		},
		{
			name:           "invalid map",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "Map(Int8|UInt8|Enum8, Int8|UInt8|Enum8)",
			goType:         "column.Map[int8, int8]",
			column: column.NewMap[int8, int8](
				column.New[int8]().SetStrict(false),
				column.New[int8]().SetStrict(false),
			),
		},
		{
			name:           "invalid map key",
			columnSelector: "map(number,number)",
			chType:         "Map(UInt64, UInt64)",
			goToChType:     "Map(Int8|UInt8|Enum8, Int8|UInt8|Enum8)",
			goType:         "column.Map[int8, int8]",
			column: column.NewMap[int8, int8](
				column.New[int8]().SetStrict(false),
				column.New[int8]().SetStrict(false),
			),
		},
		{
			name:           "invalid map value",
			columnSelector: "map(number,number)",
			chType:         "Map(UInt64, UInt64)",
			goToChType:     "Map(Int64|UInt64|Float64|Decimal64|DateTime64, Int8|UInt8|Enum8)",
			goType:         "column.Map[int64, int8]",
			column: column.NewMap[int64, int8](
				column.New[int64]().SetStrict(false),
				column.New[int8]().SetStrict(false),
			),
		},
		{
			name:           "invalid tuple",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "Tuple(Int64|UInt64|Float64|Decimal64|DateTime64,Int8|UInt8|Enum8)",
			goType:         "column.Tuple(column.Base[int64], column.Base[int8])",
			column:         column.NewTuple(column.New[int64]().SetStrict(false), column.New[int8]().SetStrict(false)),
		},
		{
			name:           "invalid tuple inside",
			columnSelector: "tuple(number)",
			chType:         "Tuple(UInt64)",
			goToChType:     "Tuple(Int8|UInt8|Enum8)",
			goType:         "column.Tuple(column.Base[int8])",
			column:         column.NewTuple(column.New[int8]().SetStrict(false)),
		},
		{
			name:           "date time with timezone",
			columnSelector: "toDateTime('2010-01-01', 'America/New_York') + number",
			chType:         "DateTime('America/New_York')",
			goToChType:     "Int64|UInt64|Float64|Decimal64|DateTime64",
			goType:         "column.Base[uint64]",
			column:         column.New[uint64]().SetStrict(false),
		},
		{
			name:           "date time 64 with timezone",
			columnSelector: "toDateTime64('2010-01-01', 3, 'America/New_York') + number",
			chType:         "DateTime64(3, 'America/New_York')",
			goToChType:     "Int32|UInt32|Float32|Decimal32|Date32|DateTime|IPv4",
			goType:         "column.Base[uint32]",
			column:         column.New[uint32]().SetStrict(false),
		},
		{
			name:           "Decimal",
			columnSelector: "toDecimal32(number,3)",
			chType:         "Decimal(9, 3)",
			goToChType:     "Int64|UInt64|Float64|Decimal64|DateTime64",
			goType:         "column.Base[uint64]",
			column:         column.New[uint64]().SetStrict(false),
		},
		{
			name:           "Array2",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "Array(Array(Int64|UInt64|Float64|Decimal64|DateTime64))",
			goType:         "column.Array2[uint64]",
			column:         column.New[uint64]().SetStrict(false).Array().Array(),
		},
		{
			name:           "Array2 inside",
			columnSelector: "array(number,number)",
			chType:         "Array(UInt64)",
			goToChType:     "Array(Array(Int64|UInt64|Float64|Decimal64|DateTime64))",
			goType:         "column.Array2[uint64]",
			column:         column.New[uint64]().SetStrict(false).Array().Array(),
		},
		{
			name:           "Array3",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "Array(Array(Array(Int64|UInt64|Float64|Decimal64|DateTime64)))",
			goType:         "column.Array3[uint64]",
			column:         column.New[uint64]().SetStrict(false).Array().Array().Array(),
		},
		{
			name:           "Array3 inside",
			columnSelector: "array(number,number)",
			chType:         "Array(UInt64)",
			goToChType:     "Array(Array(Array(Int64|UInt64|Float64|Decimal64|DateTime64)))",
			goType:         "column.Array3[uint64]",
			column:         column.New[uint64]().SetStrict(false).Array().Array().Array(),
		},
		{
			name:           "NothingNullable",
			columnSelector: "NULL",
			chType:         "Nullable(Nothing)",
			goToChType:     "Int64|UInt64|Float64|Decimal64|DateTime64",
			goType:         "column.Base[uint64]",
			column:         column.New[uint64]().SetStrict(false),
		},
		{
			name:           "Variant",
			columnSelector: "number",
			chType:         "UInt64",
			goToChType:     "Variant(Int64|UInt64|Float64|Decimal64|DateTime64, String)",
			goType:         "column.Variant(column.Base[int64], column.StringBase[string])",
			column:         column.NewVariant(column.NewString(), column.New[int64]().SetStrict(false)),
		}, {
			name:           "Variant number column",
			columnSelector: "number::Variant(UInt64, String, Array(UInt64)) as a",
			fullErr:        "columns number for a (Variant(Array(UInt64), String, UInt64)) is not equal to Variant columns number: 3 != 1",
			column:         column.NewVariant(column.NewString()),
			skip: func(c chconn.Conn) bool {
				return c.ServerInfo().MajorVersion < 24
			},
		}, {
			name:           "Variant inside",
			columnSelector: "number::Variant(UInt64, String) as a",
			chType:         "Variant(String, UInt64)",
			goToChType:     "Variant(Int32, String)",
			goType:         "column.Variant(column.Base[int32], column.StringBase[string])",
			column:         column.NewVariant(column.NewString(), column.New[int32]()),
			skip: func(c chconn.Conn) bool {
				return c.ServerInfo().MajorVersion < 24
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != nil && tt.skip(c) {
				t.Skip("ClickHouse version does not support this test")
			}
			c, err = chconn.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			set := chconn.Settings{
				{
					Name:  "allow_suspicious_low_cardinality_types",
					Value: "true",
				},
				{
					Name:  "allow_experimental_variant_type",
					Value: "1",
				},
			}
			stmt, err := c.SelectWithOption(context.Background(),
				fmt.Sprintf("SELECT %s FROM  system.numbers limit 1", tt.columnSelector),
				&chconn.QueryOptions{
					Settings: set,
				},
				tt.column,
			)

			require.NoError(t, err)
			for stmt.Next() {

			}
			if tt.fullErr != "" {
				require.EqualError(t, errors.Unwrap(stmt.Err()), tt.fullErr)
			} else {
				require.EqualError(t, errors.Unwrap(stmt.Err()),
					fmt.Sprintf("the chconn type '%s' is mapped to ClickHouse type '%s', which does not match the expected ClickHouse type '%s'",
						tt.goType,
						tt.goToChType,
						tt.chType,
					),
				)
			}
			assert.True(t, c.IsClosed())
		})
	}
}

func TestTupleInvalidColumnNumber(t *testing.T) {
	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := chconn.ParseConfig(connString)
	require.NoError(t, err)

	c, err := chconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	stmt, err := c.Select(context.Background(),
		"SELECT tuple(number) as t FROM  system.numbers limit 1",
		column.NewTuple(column.New[uint64]().SetStrict(false), column.New[uint64]().SetStrict(false)),
	)

	require.NoError(t, err)
	for stmt.Next() {

	}
	require.EqualError(t, errors.Unwrap(stmt.Err()),
		"columns number for t (Tuple(UInt64)) is not equal to tuple columns number: 1 != 2",
	)
	assert.True(t, c.IsClosed())
}

func TestMapInvalidColumnNumber(t *testing.T) {
	m := column.NewMap[uint8, uint8](column.New[uint8](), column.New[uint8]())
	m.SetType([]byte("Map(UInt8,UInt8,UInt8)"))
	err := m.Validate(false)
	assert.Equal(t, err.Error(), "columns number is not equal to map columns number: 3 != 2")
}

func TestFixedStringInvalidType(t *testing.T) {
	m := column.New[[20]byte]()
	m.SetType([]byte("FixedString(a)"))
	err := m.Validate(false)
	assert.Equal(t, err.Error(), "invalid size: strconv.Atoi: parsing \"a\": invalid syntax")
}

func TestEnum8InvalidType(t *testing.T) {
	m := column.New[int16]()
	m.SetType([]byte("Enum8()"))
	err := m.Validate(false)
	assert.Equal(t,
		err.Error(),
		"the chconn type 'column.Base[int16]' is mapped to ClickHouse type 'Int16', which does not match the expected ClickHouse type 'Enum8()'")
}
func TestEnum16InvalidType(t *testing.T) {
	m := column.New[int32]()
	m.SetType([]byte("Enum16()"))
	assert.Equal(t,
		m.SetStrict(false).Validate(false).Error(),
		"the chconn type 'column.Base[int32]' is mapped to ClickHouse type 'Int32|UInt32|Float32|Decimal32|Date32|DateTime|IPv4', "+
			"which does not match the expected ClickHouse type 'Enum16()'")

	assert.Equal(t,
		m.SetStrict(true).Validate(false).Error(),
		"the chconn type 'column.Base[int32]' is mapped to ClickHouse type 'Int32', which does not match the expected ClickHouse type 'Enum16()'")
}

func TestDecimalInvalidType(t *testing.T) {
	m := column.New[[20]byte]()
	m.SetType([]byte("Decimal()"))
	err := m.Validate(false)
	assert.Equal(t, err.Error(), "invalid decimal type (should have precision and scale): Decimal()")

	m.SetType([]byte("Decimal(a, a)"))
	err = m.Validate(false)
	assert.Equal(t, err.Error(), "invalid precision: strconv.Atoi: parsing \"a\": invalid syntax")

	m.SetType([]byte("Decimal(3, a)"))
	err = m.Validate(false)
	assert.Equal(t, err.Error(), "invalid scale: strconv.Atoi: parsing \"a\": invalid syntax")

	m.SetType([]byte("Decimal(200, 3)"))
	err = m.Validate(false)
	assert.Equal(t, err.Error(), "invalid precision: 200. it should be between 1 and 76")
}

func TestInvalidDate(t *testing.T) {
	m := column.NewDate[types.DateTime]()
	m.SetType([]byte("DateTime('InvalidTimeZone')"))
	err := m.Validate(false)
	assert.NoError(t, err)
	assert.Equal(t, m.Location(), time.Local)
}

func TestInvalidSimpleAggregateFunction(t *testing.T) {
	m := column.New[int32]()
	m.SetType([]byte("SimpleAggregateFunction(sum))"))
	assert.Panics(t, func() {
		m.Validate(false)
	})
}
