package chconn

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/column"
	"github.com/vahid-sohrabloo/chconn/setting"
)

func TestSelectError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	// test lock error
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	c.(*conn).status = connStatusUninitialized
	res, err := c.Select(context.Background(), "select * from system.numbers limit 5")
	require.Nil(t, res)
	require.EqualError(t, err, "conn uninitialized")
	require.EqualError(t, c.(*conn).lock(), "conn uninitialized")
	c.Close()

	config.WriterFunc = func(w io.Writer) io.Writer {
		return &writerErrorHelper{
			err:         errors.New("timeout"),
			w:           w,
			numberValid: 1,
		}
	}
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	res, err = c.Select(context.Background(), "select * from system.numbers limit 5")
	require.EqualError(t, err, "write block info (timeout)")
	require.Nil(t, res)

	// test read more column error
	config, err = ParseConfig(connString)
	require.NoError(t, err)
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	res, err = c.Select(context.Background(), "select * from system.numbers limit 1")
	require.NotNil(t, res)
	require.NoError(t, err)
	col := column.NewUint64(false)
	col2 := column.NewUint64(false)
	for res.Next() {
		errNext := res.NextColumn(col)
		assert.NoError(t, errNext)
		assert.Equal(t, string(col.Name()), "number")
		assert.Equal(t, string(col.Type()), "UInt64")
		err = res.NextColumn(col2)
		require.EqualError(t, err, "read 2 column(s), but available 1 column(s)")
	}

	c.Close()

	// test read more column error
	config, err = ParseConfig(connString)
	require.NoError(t, err)
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	settings := setting.NewSettings()
	settings.MaxRowsInSet(1000)
	res, err = c.SelectWithSetting(context.Background(), "select number,number+1 from system.numbers limit 1", settings)
	require.NotNil(t, res)
	require.NoError(t, err)
	for res.Next() {
		err = res.NextColumn(col)
		require.NoError(t, err)
	}
	require.EqualError(t, res.Err(), "read 1 column(s), but available 2 column(s)")

	c.Close()

	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	res, err = c.Select(context.Background(), "select number,number+1 from system.numbers limit 1")
	require.NoError(t, err)
	require.NotNil(t, res)
	for res.Next() {
		err = res.ReadColumns(col)
		require.EqualError(t, err, "read 1 column(s), but available 2 column(s)")
	}

	c.Close()
}

func TestSelectprogress(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	// test lock error
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	res, err := c.SelectCallback(context.Background(),
		"SELECT sleep(0.1), * FROM system.numbers LIMIT 400000",
		nil, "", func(p *Progress) {

		}, func(p *Profile) {

		},
	)
	require.NotNil(t, res)
	require.NoError(t, err)

	colNumber := column.NewUint64(false)
	colSleep := column.NewUint8(false)
	for res.Next() {
		err = res.NextColumn(colSleep)
		require.NoError(t, err)
		err = res.NextColumn(colNumber)
		require.NoError(t, err)
	}
	require.NoError(t, res.Err())

	c.Close()
}

func TestSelectReadError(t *testing.T) {
	startValidReader := 35

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column name error",
			wantErr:     "read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type error",
			wantErr:     "read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type",
			wantErr:     "read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read nullable data",
			wantErr:     "read nullable data: read data: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "read data error",
			wantErr:     "read data: timeout",
			numberValid: startValidReader + 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			stmt, err := c.Select(context.Background(), "SELECT toNullable(number) FROM system.numbers LIMIT 1;")
			require.NoError(t, err)
			col := column.NewUint64(true)
			require.True(t, stmt.Next())

			err = stmt.ReadColumns(col)
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestSelectReadErrorMap(t *testing.T) {
	startValidReader := 35

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column name error",
			wantErr:     "read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type error",
			wantErr:     "read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type",
			wantErr:     "read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "read len data",
			wantErr:     "read len data: read data: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "read key column",
			wantErr:     "read key data: read data: timeout",
			numberValid: startValidReader + 5,
		},
		{
			name:        "read value column",
			wantErr:     "read value data: read data: timeout",
			numberValid: startValidReader + 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			stmt, err := c.Select(context.Background(), "SELECT map('key1', number, 'key2', number * 2) FROM system.numbers LIMIT 1;")
			require.NoError(t, err)
			colKey := column.NewUint64(false)
			colValue := column.NewUint64(false)
			col := column.NewMap(colKey, colValue)
			require.True(t, stmt.Next())

			err = stmt.ReadColumns(col)
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestSelectReadErrorLowCardinality(t *testing.T) {
	startValidReader := 35

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "read column name error",
			wantErr:     "read column name length: timeout",
			numberValid: startValidReader,
		},
		{
			name:        "read column name",
			wantErr:     "read column name: timeout",
			numberValid: startValidReader + 1,
		},
		{
			name:        "read column type error",
			wantErr:     "read column type length: timeout",
			numberValid: startValidReader + 2,
		},
		{
			name:        "read column type",
			wantErr:     "read column type: timeout",
			numberValid: startValidReader + 3,
		},
		{
			name:        "error reading keys serialization version",
			wantErr:     "error reading keys serialization version: timeout",
			numberValid: startValidReader + 4,
		},
		{
			name:        "error reading serialization type",
			wantErr:     "error reading serialization type: timeout",
			numberValid: startValidReader + 5,
		},
		{
			name:        "error reading dictionary size",
			wantErr:     "error reading dictionary size: timeout",
			numberValid: startValidReader + 6,
		},
		{
			name:        "error reading dictionary",
			wantErr:     "error reading dictionary: read data: timeout",
			numberValid: startValidReader + 7,
		},
		{
			name:        "error reading keys",
			wantErr:     "timeout",
			numberValid: startValidReader + 8,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			stmt, err := c.Select(context.Background(), "SELECT toLowCardinality(number) FROM system.numbers LIMIT 1;")
			require.NoError(t, err)
			colData := column.NewUint64(false)
			col := column.NewLowCardinality(colData)
			require.True(t, stmt.Next())

			err = stmt.ReadColumns(col)
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}
