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
	c.Close(context.Background())

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
	require.EqualError(t, err, "block: write block info (timeout)")
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
		name, chType, errNext := res.NextColumnDetail(col)
		assert.NoError(t, errNext)
		assert.Equal(t, name, "number")
		assert.Equal(t, chType, "UInt64")
		err = res.NextColumn(col2)
		require.EqualError(t, err, "read 2 column(s), but available 1 column(s)")
	}

	c.Close(context.Background())

	// test read more column error
	config, err = ParseConfig(connString)
	require.NoError(t, err)
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	res, err = c.Select(context.Background(), "select number,number+1 from system.numbers limit 1")
	require.NotNil(t, res)
	require.NoError(t, err)
	for res.Next() {
		err = res.NextColumn(col)
		require.NoError(t, err)
	}
	require.EqualError(t, res.Err(), "read 1 column(s), but available 2 column(s)")

	c.Close(context.Background())
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

	c.Close(context.Background())
}
