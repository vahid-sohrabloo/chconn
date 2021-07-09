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
				datetime DateTime,
				decimal32 Decimal32(4),
				decimal64 Decimal64(4),
				uuid UUID,
				tuple Tuple(UInt8, String),
				ipv4  IPv4,
				ipv6  IPv6
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	insertstmt, err := conn.Insert(context.Background(), `INSERT INTO clickhouse_test_insert (
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
				array,
				date,
				datetime,
				decimal32,
				decimal64,
				uuid,
				tuple,
				ipv4,
				ipv6
			) VALUES`)

	require.NoError(t, err)
	require.Nil(t, res)
	now := time.Now()
	var int8Insert []int8
	var int16Insert []int16
	var int32Insert []int32
	var int64Insert []int64
	var uint8Insert []uint8
	var uint16Insert []uint16
	var uint32Insert []uint32
	var uint64Insert []uint64
	var float32Insert []float32
	var float64Insert []float64
	var stringInsert []string
	var byteInsert [][]byte
	var fixedStringInsert [][]byte
	var arrayInsert [][]uint8
	var dateInsert []time.Time
	var decimalInsert []float64
	var datetimeInsert []time.Time
	var uuidInsert [][16]byte
	var ipv4Insert []net.IP
	var ipv6Insert []net.IP

	for i := 1; i <= 10; i++ {
		insertstmt.AddRow(1)
		int8Insert = append(int8Insert, int8(-1*i))
		insertstmt.Int8(0, int8(-1*i))
		int16Insert = append(int16Insert, int16(-2*i))
		insertstmt.Int16(1, int16(-2*i))
		int32Insert = append(int32Insert, int32(-4*i))
		insertstmt.Int32(2, int32(-4*i))
		int64Insert = append(int64Insert, int64(-8*i))
		insertstmt.Int64(3, int64(-8*i))
		uint8Insert = append(uint8Insert, uint8(1*i))
		insertstmt.Uint8(4, uint8(1*i))
		uint16Insert = append(uint16Insert, uint16(2*i))
		insertstmt.Uint16(5, uint16(2*i))
		uint32Insert = append(uint32Insert, uint32(4*i))
		insertstmt.Uint32(6, uint32(4*i))
		uint64Insert = append(uint64Insert, uint64(8*i))
		insertstmt.Uint64(7, uint64(8*i))
		float32Insert = append(float32Insert, 1.32*float32(i))
		insertstmt.Float32(8, 1.32*float32(i))
		float64Insert = append(float64Insert, 1.64*float64(i))
		insertstmt.Float64(9, 1.64*float64(i))
		stringInsert = append(stringInsert, fmt.Sprintf("string %d", i))
		insertstmt.String(10, fmt.Sprintf("string %d", i))
		byteInsert = append(byteInsert, []byte{10, 20, 30, 40})
		insertstmt.Buffer(11, []byte{10, 20, 30, 40})
		fixedStringInsert = append(fixedStringInsert, []byte("01"))
		insertstmt.FixedString(12, []byte("01"))
		array := []uint8{
			1, 2, 3, 4,
		}
		insertstmt.AddLen(13, uint64(len(array)))
		for _, a := range array {
			insertstmt.Uint8(14, a)
		}
		arrayInsert = append(arrayInsert, array)
		d := now.AddDate(0, 0, i)
		insertstmt.Date(15, d)
		dateInsert = append(dateInsert, time.Date(
			d.Year(),
			d.Month(),
			d.Day(),
			0,
			0,
			0,
			0,
			time.UTC,
		).In(time.Local))
		insertstmt.DateTime(16, d)
		datetimeInsert = append(datetimeInsert, time.Date(
			d.Year(),
			d.Month(),
			d.Day(),
			d.Hour(),
			d.Minute(),
			d.Second(),
			0,
			time.Local,
		))

		insertstmt.Decimal32(17, 1.64*float64(i), 4)
		insertstmt.Decimal64(18, 1.64*float64(i), 4)
		decimalInsert = append(decimalInsert, math.Floor(1.64*float64(i)*10000)/10000)

		insertstmt.UUID(19, uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a50"))
		uuidInsert = append(uuidInsert, uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a50"))

		insertstmt.Uint8(20, uint8(1*i))
		insertstmt.String(21, fmt.Sprintf("string %d", i))

		err = insertstmt.IPv4(22, net.ParseIP("1.2.3.4").To4())
		require.NoError(t, err)
		ipv4Insert = append(ipv4Insert, net.ParseIP("1.2.3.4").To4())

		err = insertstmt.IPv6(23, net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:733").To16())
		require.NoError(t, err)
		ipv6Insert = append(ipv6Insert, net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:733").To16())
	}

	err = insertstmt.Commit(context.Background())
	require.NoError(t, err)

	selectStmt, err := conn.Select(context.Background(), `SELECT 
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
				array,
				date,
				datetime,
				decimal32,
				decimal64,
				uuid,
				tuple,
				ipv4,
				ipv6
	 FROM clickhouse_test_insert`)
	require.NoError(t, err)
	var int8Data []int8
	var int16Data []int16
	var int32Data []int32
	var int64Data []int64
	var uint8Data []uint8
	var uint16Data []uint16
	var uint32Data []uint32
	var uint64Data []uint64
	var float32Data []float32
	var float64Data []float64
	var stringData []string
	var byteData [][]byte
	var fixedStringData [][]byte
	var arrayData [][]uint8
	var len1 []int
	var dateData []time.Time
	var datetimeData []time.Time
	var decimal32Data []float64
	var decimal64Data []float64
	var uuidData [][16]byte
	var tuple1Data []uint8
	var tuple2Data []string
	var ipv4Data []net.IP
	var ipv6Data []net.IP
	require.True(t, conn.IsBusy())

	defer func() {
		selectStmt.Close()
		require.False(t, conn.IsBusy())
	}()
	for selectStmt.Next() {
		_, err := selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Int8()
			require.NoError(t, errRead)
			int8Data = append(int8Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Int16()
			require.NoError(t, errRead)
			int16Data = append(int16Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Int32()
			require.NoError(t, errRead)
			int32Data = append(int32Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Int64()
			require.NoError(t, errRead)
			int64Data = append(int64Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Uint8()
			require.NoError(t, errRead)
			uint8Data = append(uint8Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Uint16()
			require.NoError(t, errRead)
			uint16Data = append(uint16Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Uint32()
			require.NoError(t, errRead)
			uint32Data = append(uint32Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Uint64()
			require.NoError(t, errRead)
			uint64Data = append(uint64Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Float32()
			require.NoError(t, errRead)
			float32Data = append(float32Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Float64()
			require.NoError(t, errRead)
			float64Data = append(float64Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.String()
			require.NoError(t, errRead)
			stringData = append(stringData, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.ByteArray()
			require.NoError(t, errRead)
			byteData = append(byteData, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.FixedString(2)
			require.NoError(t, errRead)
			fixedStringData = append(fixedStringData, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		// clear array
		len1 = len1[:0]
		// get array lens
		_, err = selectStmt.LenAll(&len1)
		require.NoError(t, err)

		for _, l := range len1 {
			arr := make([]uint8, l)
			for i := 0; i < l; i++ {
				val, errRead := selectStmt.Uint8()
				require.NoError(t, errRead)
				arr[i] = val
			}
			arrayData = append(arrayData, arr)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Date()
			require.NoError(t, errRead)
			dateData = append(dateData, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.DateTime()
			require.NoError(t, errRead)
			datetimeData = append(datetimeData, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Decimal32(4)
			require.NoError(t, errRead)
			decimal32Data = append(decimal32Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Decimal64(4)
			require.NoError(t, errRead)
			decimal64Data = append(decimal64Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.UUID()
			require.NoError(t, errRead)
			uuidData = append(uuidData, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.Uint8()
			require.NoError(t, errRead)
			tuple1Data = append(tuple1Data, val)
		}
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.String()
			require.NoError(t, errRead)
			tuple2Data = append(tuple2Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.IPv4()
			require.NoError(t, errRead)
			ipv4Data = append(ipv4Data, val)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		for i := uint64(0); i < selectStmt.RowsInBlock(); i++ {
			val, errRead := selectStmt.IPv6()
			require.NoError(t, errRead)
			ipv6Data = append(ipv6Data, val)
		}
	}

	require.NoError(t, selectStmt.Err())
	assert.Equal(t, int8Insert, int8Data)
	assert.Equal(t, int16Insert, int16Data)
	assert.Equal(t, int32Insert, int32Data)
	assert.Equal(t, int64Insert, int64Data)
	assert.Equal(t, uint8Insert, uint8Data)
	assert.Equal(t, uint16Insert, uint16Data)
	assert.Equal(t, uint32Insert, uint32Data)
	assert.Equal(t, uint64Insert, uint64Data)
	assert.Equal(t, float32Insert, float32Data)
	assert.Equal(t, float64Insert, float64Data)
	assert.Equal(t, stringInsert, stringData)
	assert.Equal(t, byteInsert, byteData)
	assert.Equal(t, fixedStringInsert, fixedStringData)
	assert.Equal(t, arrayInsert, arrayData)
	assert.Equal(t, dateInsert, dateData)
	assert.Equal(t, datetimeInsert, datetimeData)
	assert.Equal(t, decimalInsert, decimal32Data)
	assert.Equal(t, decimalInsert, decimal64Data)
	assert.Equal(t, uuidInsert, uuidData)
	assert.Equal(t, uint8Insert, tuple1Data)
	assert.Equal(t, stringInsert, tuple2Data)
	assert.Equal(t, ipv4Insert, ipv4Data)
	assert.Equal(t, ipv6Insert, ipv6Data)
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

	for i := 1; i <= 1; i++ {
		insertstmt.AddRow(1)
		insertstmt.Int8(0, int8(-1*i))
		insertstmt.Int16(1, int16(-2*i))
		insertstmt.Int32(2, int32(-4*i))
		insertstmt.Int64(3, int64(-8*i))
		insertstmt.Uint8(4, uint8(1*i))
		insertstmt.Uint16(5, uint16(2*i))
		insertstmt.Uint32(6, uint32(4*i))
		insertstmt.Uint64(7, uint64(8*i))
		insertstmt.Float32(8, 1.32*float32(i))
		insertstmt.Float64(9, 1.64*float64(i))
		insertstmt.String(10, fmt.Sprintf("string %d", i))
		insertstmt.Buffer(11, []byte{10, 20, 30, 40})
		insertstmt.FixedString(12, []byte("01"))

		d := now.AddDate(0, 0, i)
		insertstmt.Date(13, d)
		insertstmt.DateTime(14, d)

		insertstmt.Decimal32(15, 1.64*float64(i), 4)
		insertstmt.Decimal64(16, 1.64*float64(i), 4)

		insertstmt.UUID(17, uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a50"))

		err = insertstmt.IPv4(18, net.ParseIP("1.2.3.4").To4())
		require.NoError(t, err)

		err = insertstmt.IPv6(19, net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:733").To16())
		require.NoError(t, err)
	}

	err = insertstmt.Commit(context.Background())
	require.NoError(t, err)

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
