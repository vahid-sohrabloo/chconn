package chconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
}

func TestSelect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert`)
	require.NoError(t, err)
	require.Nil(t, res)
	res, err = conn.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert (
				int8  Int8,
				int16 Int16,
				int32 Int32,
				int64 Int64,
				uint8  UInt8,
				uint16 UInt16,
				uint32 UInt32,
				uint64 UInt64,
				float32 Float32,
				float64 Float64,
				string  String,
				string2  String,
				fString FixedString(2),
				array Array(UInt8),
				date    Date,
				datetime DateTime('Iran'),
				datetime64 DateTime64(9,'Iran'),
				decimal32 Decimal32(4),
				decimal64 Decimal64(4),
				uuid UUID,
				tuple Tuple(UInt8, String),
				ipv4  IPv4,
				ipv6  IPv6,
				enum8 Enum8('hello' = 1, 'world' = 2),
				enum16 Enum16('hello' = 1, 'world' = 2)
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	now := time.Now()

	writer := NewChTestSelectRowWriter()
	insertRows := make([]*ChTestSelectRow, 0, 10)
	for i := 1; i <= 10; i++ {
		row := &ChTestSelectRow{}
		insertRows = append(insertRows, row)
		row.Int8 = int8(-1 * i)
		row.Int16 = int16(-2 * i)
		row.Int32 = int32(-4 * i)
		row.Int32 = int32(-4 * i)
		row.Int64 = int64(-8 * i)
		row.Uint8 = uint8(1 * i)
		row.Uint16 = uint16(2 * i)
		row.Uint32 = uint32(4 * i)
		row.Uint64 = uint64(8 * i)
		row.Float32 = 1.32 * float32(i)
		row.Float64 = 1.64 * float64(i)
		row.String = fmt.Sprintf("string %d", i)
		row.String2 = fmt.Sprintf("string %d", i*2)
		row.FString = []byte("01")
		row.Array = []uint8{
			1, 2, 3, 4,
		}

		d := now.AddDate(0, 0, i)
		row.Date = time.Date(
			d.Year(),
			d.Month(),
			d.Day(),
			0,
			0,
			0,
			0,
			time.UTC,
		).In(time.Local)

		row.Datetime = time.Date(
			d.Year(),
			d.Month(),
			d.Day(),
			d.Hour(),
			d.Minute(),
			d.Second(),
			0,
			time.Local,
		)

		row.Datetime64 = time.Date(
			d.Year(),
			d.Month(),
			d.Day(),
			d.Hour(),
			d.Minute(),
			d.Second(),
			d.Nanosecond(),
			time.Local,
		)
		row.Decimal32 = math.Floor(1.64*float64(i)*10000) / 10000
		row.Decimal64 = math.Floor(1.64*float64(i)*10000) / 10000
		row.Uuid = uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a50")
		row.Tuple.Field1 = uint8(1 * i)
		row.Tuple.Field2 = fmt.Sprintf("string %d", i)
		row.Ipv4 = net.ParseIP("1.2.3.4").To4()
		row.Ipv6 = net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:733").To16()

		if i%2 == 0 {
			row.Enum8 = 1
			row.Enum16 = 2
		} else {
			row.Enum8 = 2
			row.Enum16 = 1
		}
		err = row.Write(writer)
		require.NoError(t, err)
	}

	insertstmt, err := conn.Insert(context.Background(), GetInsertChTestSelectRowQuery("clickhouse_test_insert"))

	require.NoError(t, err)
	require.Nil(t, res)

	err = insertstmt.Commit(context.Background(), writer)
	require.NoError(t, err)
	settings := setting.NewSettings()
	settings.LogQueries(false)
	selectStmt, err := conn.SelectWithSetting(context.Background(), GetSelectChTestSelectRowQuery("clickhouse_test_insert"), settings)
	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	defer func() {
		selectStmt.Close()
		require.False(t, conn.IsBusy())
	}()

	selectRows, err := ReadChTestSelectRow(selectStmt)
	require.NoError(t, err)
	assert.Equal(t, insertRows, selectRows)
	require.NoError(t, selectStmt.Err())
	conn.RawConn().Close()
}

func TestSelectReadError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_read_error`)
	require.NoError(t, err)
	require.Nil(t, res)
	res, err = conn.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_read_error (
				int8  Int8,
				int16 Int16,
				int32 Int32,
				int64 Int64,
				uint8  UInt8,
				uint16 UInt16,
				uint32 UInt32,
				uint64 UInt64,
				float32 Float32,
				float64 Float64,
				string  String,
				string2  String,
				fString FixedString(2),
				date    Date,
				datetime DateTime,
				decimal32 Decimal32(4),
				decimal64 Decimal64(4),
				uuid UUID,
				ipv4  IPv4,
				ipv6  IPv6
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	insertstmt, err := conn.Insert(context.Background(), `INSERT INTO clickhouse_test_insert_read_error (
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				string2,
				fString,
				date,
				datetime,
				decimal32,
				decimal64,
				uuid,
				ipv4,
				ipv6
			) VALUES`)
	require.NoError(t, err)
	require.Nil(t, res)
	now := time.Now()
	writer := insertstmt.Writer()
	for i := 1; i <= 1; i++ {
		writer.AddRow(1)
		writer.Int8(0, int8(-1*i))
		writer.Int16(1, int16(-2*i))
		writer.Int32(2, int32(-4*i))
		writer.Int64(3, int64(-8*i))
		writer.Uint8(4, uint8(1*i))
		writer.Uint16(5, uint16(2*i))
		writer.Uint32(6, uint32(4*i))
		writer.Uint64(7, uint64(8*i))
		writer.Float32(8, 1.32*float32(i))
		writer.Float64(9, 1.64*float64(i))
		writer.String(10, fmt.Sprintf("string %d", i))
		writer.Buffer(11, []byte{10, 20, 30, 40})
		writer.FixedString(12, []byte("01"))

		d := now.AddDate(0, 0, i)
		writer.Date(13, d)
		writer.DateTime(14, d)

		writer.Decimal32(15, 1.64*float64(i), 4)
		writer.Decimal64(16, 1.64*float64(i), 4)

		writer.UUID(17, uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a50"))

		err = writer.IPv4(18, net.ParseIP("1.2.3.4").To4())
		require.NoError(t, err)

		err = writer.IPv6(19, net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:733").To16())
		require.NoError(t, err)
	}

	err = insertstmt.Commit(context.Background(), writer)
	require.NoError(t, err)

	writer.Reset()

	startValidReader := 38

	tests := []struct {
		colName     string
		readFunc    func(SelectStmt) (interface{}, error)
		wantErr     string
		numberValid int
	}{
		{
			colName: "int8",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Int8()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "int16",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Int16()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "int32",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Int32()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "int64",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Int64()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "uint8",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Uint8()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "uint16",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Uint16()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "uint32",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Uint32()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "uint64",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Uint64()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "float32",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Float32()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "float64",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Float64()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "string",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.String()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "string2",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.ByteArray()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "fString",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.FixedString(2)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "date",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Date()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "datetime",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.DateTime()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "decimal32",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Decimal32(4)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "decimal64",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.Decimal64(4)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "uuid",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.UUID()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "ipv4",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.IPv4()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "ipv6",
			readFunc: func(stmt SelectStmt) (interface{}, error) {
				return stmt.IPv6()
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		},
	}
	for _, tt := range tests {
		t.Run(tt.colName, func(t *testing.T) {
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
			require.NoError(t, err)
			defer c.Close(context.Background())

			selectStmt, err := c.Select(context.Background(), `SELECT 
				`+tt.colName+`
	 FROM clickhouse_test_insert_read_error limit 1`)
			require.NoError(t, err)
			defer selectStmt.Close()
			assert.True(t, selectStmt.Next())
			require.NoError(t, selectStmt.Err())
			_, err = selectStmt.NextColumn()
			require.Error(t, err)
			_, err = tt.readFunc(selectStmt)
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestSelectSimpleAggregateFunction(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_simple_agg`)
	require.NoError(t, err)
	require.Nil(t, res)
	res, err = conn.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_simple_agg (
		id UInt64,
		val SimpleAggregateFunction(sum, Double)
		) ENGINE=AggregatingMergeTree ORDER BY id;`)

	require.NoError(t, err)
	require.Nil(t, res)

	insertstmt, err := conn.Insert(context.Background(), `INSERT INTO clickhouse_test_insert_simple_agg (
				id,
				val
			) VALUES`)

	require.NoError(t, err)
	require.Nil(t, res)

	writer := insertstmt.Writer()
	writer.AddRow(1)
	writer.Uint64(0, 1)
	writer.Float64(1, 2)
	err = insertstmt.Commit(context.Background(), writer)
	require.NoError(t, err)
	writer.Reset()

	selectStmt, err := conn.Select(context.Background(), `SELECT 
				id,val
	 FROM clickhouse_test_insert_simple_agg`)
	require.NoError(t, err)

	require.True(t, conn.IsBusy())

	defer func() {
		selectStmt.Close()
		require.False(t, conn.IsBusy())
	}()
	for selectStmt.Next() {
		assert.Equal(t, selectStmt.RowsInBlock(), uint64(1))

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)

		id, errRead := selectStmt.Uint64()
		require.NoError(t, errRead)
		assert.Equal(t, id, uint64(1))

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)

		val, errRead := selectStmt.Float64()
		require.NoError(t, errRead)
		assert.Equal(t, val, float64(2))
	}

	require.NoError(t, selectStmt.Err())

	conn.RawConn().Close()
}
