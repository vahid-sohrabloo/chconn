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

func TestSelectSlice(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_slice`)
	require.NoError(t, err)
	require.Nil(t, res)
	res, err = conn.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_slice (
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
				array Array(Array(UInt8)) ,
				date    Date,
				datetime DateTime,
				decimal32 Decimal32(4),
				decimal64 Decimal64(4),
				uuid UUID,
				tuple Tuple(UInt8, String),
				ipv4  IPv4,
				ipv6  IPv6,
				sLowCardinality LowCardinality(String),
				asLowCardinality Array(LowCardinality(String)),
				sfLowCardinality LowCardinality(FixedString(10))
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	insertStmt, err := conn.Insert(context.Background(), `INSERT INTO clickhouse_test_insert_slice (
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
				ipv6,
				sLowCardinality,
				sfLowCardinality,
				asLowCardinality
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
	var arrayInsert [][][]uint8
	var dateInsert []time.Time
	var decimalInsert []float64
	var datetimeInsert []time.Time
	var uuidInsert [][16]byte
	var ipv4Insert []net.IP
	var ipv6Insert []net.IP
	var sLowCardinalityInsert []string
	var asLowCardinalityInsert [][]string
	var sfLowCardinalityInsert [][]byte
	writer := insertStmt.Writer()
	for i := 1; i <= 10; i++ {
		writer.AddRow(1)
		int8Insert = append(int8Insert, int8(-1*i))
		writer.Int8(0, int8(-1*i))
		int16Insert = append(int16Insert, int16(-2*i))
		writer.Int16(1, int16(-2*i))
		int32Insert = append(int32Insert, int32(-4*i))
		writer.Int32(2, int32(-4*i))
		int64Insert = append(int64Insert, int64(-8*i))
		writer.Int64(3, int64(-8*i))
		uint8Insert = append(uint8Insert, uint8(1*i))
		writer.Uint8(4, uint8(1*i))
		uint16Insert = append(uint16Insert, uint16(2*i))
		writer.Uint16(5, uint16(2*i))
		uint32Insert = append(uint32Insert, uint32(4*i))
		writer.Uint32(6, uint32(4*i))
		uint64Insert = append(uint64Insert, uint64(8*i))
		writer.Uint64(7, uint64(8*i))
		float32Insert = append(float32Insert, 1.32*float32(i))
		writer.Float32(8, 1.32*float32(i))
		float64Insert = append(float64Insert, 1.64*float64(i))
		writer.Float64(9, 1.64*float64(i))
		stringInsert = append(stringInsert, fmt.Sprintf("string %d", i))
		writer.String(10, fmt.Sprintf("string %d", i))
		byteInsert = append(byteInsert, []byte{10, 20, 30, 40})
		writer.Buffer(11, []byte{10, 20, 30, 40})
		fixedStringInsert = append(fixedStringInsert, []byte("01"))
		writer.FixedString(12, []byte("01"))
		array := [][]uint8{
			{
				1, 2, 3, 4,
			}, {
				5, 6, 7, 8, 9,
			},
		}
		writer.AddLen(13, uint64(len(array)))
		for _, a := range array {
			writer.AddLen(14, uint64(len(a)))
			for _, u8 := range a {
				writer.Uint8(15, u8)
			}
		}
		arrayInsert = append(arrayInsert, array)
		d := now.AddDate(0, 0, i)
		writer.Date(16, d)
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
		writer.DateTime(17, d)
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

		writer.Decimal32(18, 1.64*float64(i), 4)
		writer.Decimal64(19, 1.64*float64(i), 4)
		decimalInsert = append(decimalInsert, math.Floor(1.64*float64(i)*10000)/10000)

		writer.UUID(20, uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a50"))
		uuidInsert = append(uuidInsert, uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a50"))

		writer.Uint8(21, uint8(1*i))
		writer.String(22, fmt.Sprintf("string %d", i))

		err = writer.IPv4(23, net.ParseIP("1.2.3.4").To4())
		require.NoError(t, err)
		ipv4Insert = append(ipv4Insert, net.ParseIP("1.2.3.4").To4())

		err = writer.IPv6(24, net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:733").To16())
		require.NoError(t, err)
		ipv6Insert = append(ipv6Insert, net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:733").To16())

		if i%2 == 0 {
			writer.AddStringLowCardinality(25, "string 1")
			sLowCardinalityInsert = append(sLowCardinalityInsert, "string 1")
		} else {
			writer.AddStringLowCardinality(25, "string 2")
			sLowCardinalityInsert = append(sLowCardinalityInsert, "string 2")
		}

		if i%2 == 0 {
			writer.AddFixedStringLowCardinality(26, []byte("0123456789"))
			sfLowCardinalityInsert = append(sfLowCardinalityInsert, []byte("0123456789"))
		} else {
			writer.AddFixedStringLowCardinality(26, []byte("0987654321"))
			sfLowCardinalityInsert = append(sfLowCardinalityInsert, []byte("0987654321"))
		}

		arrayString := []string{

			"string 1",
			"string 2",
		}
		writer.AddLen(27, uint64(len(arrayString)))
		for _, s := range arrayString {
			writer.AddStringLowCardinality(28, s)
		}

		asLowCardinalityInsert = append(asLowCardinalityInsert, arrayString)
	}

	err = insertStmt.Commit(context.Background(), writer)
	writer.Reset()
	require.NoError(t, err)

	selectStmt, err := conn.SelectCallback(context.Background(), `SELECT 
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
				ipv6,
				sLowCardinality,
				asLowCardinality,
				sfLowCardinality

	 FROM clickhouse_test_insert_slice`, nil, func(*Progress) {}, func(*Profile) {})
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
	var arrayData [][][]uint8
	var len1 []int
	var len2 []int
	var index1 int
	var index2 int

	var dateData []time.Time
	var datetimeData []time.Time
	var decimal32Data []float64
	var decimal64Data []float64
	var uuidData [][16]byte
	var tuple1Data []uint8
	var tuple2Data []string
	var ipv4Data []net.IP
	var ipv6Data []net.IP
	var sLowCardinalityData []string
	var sfLowCardinalityData [][]byte
	// start offset from 1 to better calc in foreach
	var lenAsLowCardinality []int
	var asLowCardinalityData [][]string

	require.True(t, conn.IsBusy())
	defer func() {
		selectStmt.Close()
		require.False(t, conn.IsBusy())
	}()
	for selectStmt.Next() {
		_, err := selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Int8All(&int8Data)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Int16All(&int16Data)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Int32All(&int32Data)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Int64All(&int64Data)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Uint8All(&uint8Data)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Uint16All(&uint16Data)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Uint32All(&uint32Data)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Uint64All(&uint64Data)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Float32All(&float32Data)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Float64All(&float64Data)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.StringAll(&stringData)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.ByteArrayAll(&byteData)
		require.NoError(t, err)
		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.FixedStringAll(&fixedStringData, 2)
		require.NoError(t, err)

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		// clear array
		len1 = len1[:0]
		// get array lens
		lastOffset, err := selectStmt.LenAll(&len1)
		require.NoError(t, err)
		// get second dimension by last offset from prev array
		_, err = selectStmt.LenS(lastOffset, &len2)
		require.NoError(t, err)
		index1 = 0
		index2 = 0
		for index1 < len(len1) {
			arr1 := make([][]uint8, len1[index1])
			for i := range arr1 {
				arr1[i] = make([]uint8, 0, len2[index2])
				err = selectStmt.Uint8S(uint64(len2[index2]), &arr1[i])
				require.NoError(t, err)
				index2++
			}
			index1++
			arrayData = append(arrayData, arr1)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.DateAll(&dateData)
		require.NoError(t, err)

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.DateTimeAll(&datetimeData)
		require.NoError(t, err)

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Decimal32All(&decimal32Data, 4)
		require.NoError(t, err)

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Decimal64All(&decimal64Data, 4)
		require.NoError(t, err)

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.UUIDAll(&uuidData)
		require.NoError(t, err)

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.Uint8All(&tuple1Data)
		require.NoError(t, err)
		err = selectStmt.StringAll(&tuple2Data)
		require.NoError(t, err)

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.IPv4All(&ipv4Data)
		require.NoError(t, err)

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		err = selectStmt.IPv6All(&ipv6Data)
		require.NoError(t, err)

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		// version
		_, err = selectStmt.Uint64()
		require.NoError(t, err)
		err = selectStmt.LowCardinalityString(&sLowCardinalityData)
		require.NoError(t, err)

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		// version
		_, err = selectStmt.Uint64()
		require.NoError(t, err)

		lenAsLowCardinality = lenAsLowCardinality[:0]
		lastOffset, err = selectStmt.LenAll(&lenAsLowCardinality)
		require.NoError(t, err)
		allAsLowCardinalityData := make([]string, 0, lastOffset)
		err = selectStmt.LowCardinalityString(&allAsLowCardinalityData)
		require.NoError(t, err)
		indexAll := 0
		for _, l := range lenAsLowCardinality {
			stringArray := make([]string, l)
			for index := range stringArray {
				stringArray[index] = allAsLowCardinalityData[indexAll]
				indexAll++
			}
			asLowCardinalityData = append(asLowCardinalityData, stringArray)
		}

		_, err = selectStmt.NextColumn()
		require.NoError(t, err)
		// version
		_, err = selectStmt.Uint64()
		require.NoError(t, err)
		err = selectStmt.LowCardinalityFixedString(&sfLowCardinalityData, 10)
		require.NoError(t, err)
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
	assert.Equal(t, sLowCardinalityInsert, sLowCardinalityData)
	assert.Equal(t, asLowCardinalityInsert, asLowCardinalityData)
	assert.Equal(t, sfLowCardinalityInsert, sfLowCardinalityData)
	conn.RawConn().Close()
}

func TestSelectSliceReadError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_slice_read_error`)
	require.NoError(t, err)
	require.Nil(t, res)
	res, err = conn.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_slice_read_error (
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
				ipv6  IPv6,
				array Array(Int8)
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	insertStmt, err := conn.Insert(context.Background(), `INSERT INTO clickhouse_test_insert_slice_read_error (
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
				ipv6,
				array
			) VALUES`)
	require.NoError(t, err)
	require.Nil(t, res)
	now := time.Now()
	writer := insertStmt.Writer()

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

		// array
		writer.AddLen(20, 2)
		writer.Int8(21, 1)
		writer.Int8(21, 2)
	}

	err = insertStmt.Commit(context.Background(), writer)
	require.NoError(t, err)

	startValidReader := 38

	tests := []struct {
		colName     string
		readFunc    func(SelectStmt) error
		wantErr     string
		numberValid int
	}{
		{
			colName: "int8",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Int8All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "int16",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Int16All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "int32",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Int32All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "int64",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Int64All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "uint8",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Uint8All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "uint16",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Uint16All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "uint32",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Uint32All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "uint64",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Uint64All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "float32",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Float32All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "float64",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Float64All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "string",
			readFunc: func(stmt SelectStmt) error {
				return stmt.StringAll(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "string2",
			readFunc: func(stmt SelectStmt) error {
				return stmt.ByteArrayAll(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "fString",
			readFunc: func(stmt SelectStmt) error {
				return stmt.FixedStringAll(nil, 2)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "date",
			readFunc: func(stmt SelectStmt) error {
				return stmt.DateAll(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "datetime",
			readFunc: func(stmt SelectStmt) error {
				return stmt.DateTimeAll(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "decimal32",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Decimal32All(nil, 4)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "decimal64",
			readFunc: func(stmt SelectStmt) error {
				return stmt.Decimal64All(nil, 4)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "uuid",
			readFunc: func(stmt SelectStmt) error {
				return stmt.UUIDAll(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "ipv4",
			readFunc: func(stmt SelectStmt) error {
				return stmt.IPv4All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "ipv6",
			readFunc: func(stmt SelectStmt) error {
				return stmt.IPv6All(nil)
			},
			wantErr:     "timeout",
			numberValid: startValidReader,
		}, {
			colName: "array",
			readFunc: func(stmt SelectStmt) error {
				_, err := stmt.LenAll(nil)
				return err
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
	 				FROM clickhouse_test_insert_slice_read_error limit 1`)
			require.NoError(t, err)
			defer selectStmt.Close()
			assert.True(t, selectStmt.Next())
			require.NoError(t, selectStmt.Err())
			_, err = selectStmt.NextColumn()
			require.Error(t, err)
			err = tt.readFunc(selectStmt)
			require.EqualError(t, err, tt.wantErr)
		})
	}
}
