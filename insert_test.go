package chconn

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/column"
	"github.com/vahid-sohrabloo/chconn/setting"
)

func TestInsertError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	// test lock error
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	c.(*conn).status = connStatusUninitialized
	err = c.Insert(context.Background(), "insert into system.numbers VALUES")
	require.EqualError(t, err, "conn uninitialized")
	require.EqualError(t, c.(*conn).lock(), "conn uninitialized")
	c.Close()

	// test write block info error
	config.WriterFunc = func(w io.Writer) io.Writer {
		return &writerErrorHelper{
			err:         errors.New("timeout"),
			w:           w,
			numberValid: 1,
		}
	}
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	err = c.Insert(context.Background(), "insert into system.numbers VALUES")
	require.EqualError(t, err, "write block info (timeout)")
	assert.True(t, c.IsClosed())

	// test insert server error
	config.WriterFunc = nil
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	err = c.Insert(context.Background(), "insert into system.numbers VALUES")
	require.EqualError(t, err, " DB::Exception (48): Method write is not supported by storage SystemNumbers")
	assert.True(t, c.IsClosed())

	// test not block data error
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	err = c.Insert(context.Background(), "SET enable_http_compression=1")
	require.EqualError(t, err, "Unexpected packet from server (expected serverData got <nil>)")
	assert.True(t, c.IsClosed())

	// test read column error
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	_, err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_error`)
	require.NoError(t, err)
	settings := setting.NewSettings()
	settings.JoinUseNulls(false)
	_, err = c.ExecWithSetting(context.Background(), `CREATE TABLE clickhouse_test_insert_error (
				int8  Int8
			) Engine=Memory`, settings)

	require.NoError(t, err)

	config.ReaderFunc = func(r io.Reader) io.Reader {
		return &readErrorHelper{
			err:         errors.New("timeout"),
			r:           r,
			numberValid: 27,
		}
	}
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	err = c.Insert(context.Background(), `INSERT INTO clickhouse_test_insert_error (
				int8
			) VALUES`)
	require.EqualError(t, err, "block: read column name (timeout)")
	assert.True(t, c.IsClosed())

	config, err = ParseConfig(connString)
	require.NoError(t, err)

	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	err = c.Insert(context.Background(), `INSERT INTO clickhouse_test_insert_error (
				int8
			) VALUES`)
	require.EqualError(t, err, ErrInsertMinColumn.Error())
	assert.True(t, c.IsClosed())
}

func TestInsertCtxError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = c.Insert(ctx, `INSERT INTO clickhouse_test_insert_error (
				int8
			) VALUES`)
	require.EqualError(t, err, "timeout: context already done: context canceled")
	assert.False(t, c.IsClosed())

	config, err = ParseConfig(connString)
	require.NoError(t, err)

	config.WriterFunc = func(w io.Writer) io.Writer {
		return &writerSlowHelper{
			w:     w,
			sleep: time.Second,
		}
	}

	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	err = c.Insert(ctx, `INSERT INTO clickhouse_test_insert_error (
		int8
	) VALUES`)
	require.EqualError(t, errors.Unwrap(err), "context deadline exceeded")
	assert.True(t, c.IsClosed())
}

func TestInsertMoreColumnsError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	_, err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_error_column`)
	require.NoError(t, err)

	_, err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_error_column (
		int8  Int8
	) Engine=Memory`)

	require.NoError(t, err)

	err = c.Insert(context.Background(), `INSERT INTO clickhouse_test_insert_error_column (
			int8
		) VALUES`, column.NewInt8(false), column.NewInt8(false))
	require.EqualError(t, err, "write 2 column(s) but insert query needs 1 column(s)")
	assert.True(t, c.IsClosed())
}

func TestInsertMoreRowsError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	_, err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_error_rows`)
	require.NoError(t, err)

	_, err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_error_rows (
		int8  Int8,
		int16 Int16
	) Engine=Memory`)
	require.NoError(t, err)

	col1 := column.NewInt8(false)
	col2 := column.NewInt16(false)
	col1.Append(1)
	col1.Append(2)
	col2.Append(2)
	err = c.Insert(context.Background(), `INSERT INTO clickhouse_test_insert_error_rows (
			int8,
			int16
		) VALUES`, col1, col2)
	require.EqualError(t, err, "first column has 2 rows but \"int16\" column has 1 rows")
	assert.True(t, c.IsClosed())
}

func TestInsert(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_insert`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_insert (
				int8 Int8
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewInt8(false)

	var colInsert []int8

	rows := 10
	for i := 0; i < rows; i++ {
		val := int8(i)

		col.Append(val)
		colInsert = append(colInsert, val)
	}

	err = conn.Insert(context.Background(), `INSERT INTO test_insert (int8) VALUES`, col)

	require.NoError(t, err)

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT int8 FROM test_insert`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewInt8(false)

	var colData []int8

	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colRead)
		require.NoError(t, err)
		colRead.ReadAll(&colData)
	}

	assert.Equal(t, colInsert, colData)
	require.NoError(t, selectStmt.Err())

	selectStmt.Close()

	conn.RawConn().Close()
}

func TestCompressInsert(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")
	parseConfig, err := ParseConfig(connString)
	require.NoError(t, err)
	parseConfig.Compress = true
	conn, err := ConnectConfig(context.Background(), parseConfig)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS test_insert_compress`)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = conn.Exec(context.Background(), `CREATE TABLE test_insert_compress (
				int8 Int8
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	col := column.NewInt8(false)

	var colInsert []int8

	rows := 1000
	for i := 0; i < rows; i++ {
		val := int8(i)

		col.Append(val)
		colInsert = append(colInsert, val)
	}

	err = conn.Insert(context.Background(), `INSERT INTO test_insert_compress (int8) VALUES`, col)

	require.NoError(t, err)

	// example read all
	selectStmt, err := conn.Select(context.Background(), `SELECT int8 FROM test_insert_compress`)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colRead := column.NewInt8(false)

	var colData []int8

	for selectStmt.Next() {
		err = selectStmt.ReadColumns(colRead)
		require.NoError(t, err)
		colRead.ReadAll(&colData)
	}

	assert.Equal(t, colInsert, colData)
	require.NoError(t, selectStmt.Err())

	selectStmt.Close()

	conn.RawConn().Close()
}

func TestInsertColumnError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	_, err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_column_error`)
	require.NoError(t, err)

	_, err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_column_error (
		int8  Int8
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
			wantErr:     "block: write header block data for column int8 (timeout)",
			numberValid: startValidReader,
		},
		{
			name:        "write block data",
			wantErr:     "block: write block data for column int8 (timeout)",
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
			c, err = ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			col := column.NewInt8(false)
			err = c.Insert(context.Background(),
				"insert into clickhouse_test_insert_column_error (int8) VALUES",
				col,
			)
			require.EqualError(t, err, tt.wantErr)
			assert.True(t, c.IsClosed())
		})
	}
}

func TestInsertColumnErrorCompress(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	config.Compress = true
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	_, err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_column_error_compress`)
	require.NoError(t, err)

	_, err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_column_error_compress (
		int8  Int8
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
			wantErr:     "failed to insert data: write block info (timeout)",
			numberValid: startValidReader,
		},
		{
			name:        "flush block info",
			wantErr:     "failed to insert data: flush block info (timeout)",
			numberValid: startValidReader + 1,
		},
		{
			name:        "flush data",
			wantErr:     "block: flush block data (timeout)",
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
			c, err = ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			col := column.NewInt8(false)
			err = c.Insert(context.Background(),
				"insert into clickhouse_test_insert_column_error_compress (int8) VALUES",
				col,
			)
			require.EqualError(t, err, tt.wantErr)
			assert.True(t, c.IsClosed())
		})
	}
}

func TestInsertColumnLowCardinality(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	_, err = c.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_column_error_lc`)
	require.NoError(t, err)

	_, err = c.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_column_error_lc (
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
			c, err = ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			col := column.NewString(false)
			colLC := column.NewLowCardinality(col)
			col.AppendStringDict("test")
			err = c.Insert(context.Background(),
				"insert into clickhouse_test_insert_column_error_lc (col) VALUES",
				colLC,
			)
			require.EqualError(t, err, tt.wantErr)
			assert.True(t, c.IsClosed())
		})
	}
}
