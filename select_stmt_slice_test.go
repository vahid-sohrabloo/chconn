package chconn

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "CHX_TEST_TCP_CONN_STRING")
	}

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
				array Array(Array(UInt8)) ,
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

	insertStmt, err := conn.Insert(context.Background(), `INSERT INTO clickhouse_test_insert (
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
	var arrayInsert [][][]uint8
	var dateInsert []time.Time
	var decimalInsert []float64
	var datetimeInsert []time.Time
	var uuidInsert [][16]byte
	var tuple1Insert []uint8
	var tuple2Insert []string
	for i := 1; i <= 10; i++ {
		insertStmt.Block.NumRows++
		int8Insert = append(int8Insert, int8(-1*i))
		insertStmt.Int8(0, int8(-1*i))
		int16Insert = append(int16Insert, int16(-2*i))
		insertStmt.Int16(1, int16(-2*i))
		int32Insert = append(int32Insert, int32(-4*i))
		insertStmt.Int32(2, int32(-4*i))
		int64Insert = append(int64Insert, int64(-8*i))
		insertStmt.Int64(3, int64(-8*i))
		uint8Insert = append(uint8Insert, uint8(1*i))
		insertStmt.Uint8(4, uint8(1*i))
		uint16Insert = append(uint16Insert, uint16(2*i))
		insertStmt.Uint16(5, uint16(2*i))
		uint32Insert = append(uint32Insert, uint32(4*i))
		insertStmt.Uint32(6, uint32(4*i))
		uint64Insert = append(uint64Insert, uint64(8*i))
		insertStmt.Uint64(7, uint64(8*i))
		float32Insert = append(float32Insert, 1.32*float32(i))
		insertStmt.Float32(8, 1.32*float32(i))
		float64Insert = append(float64Insert, 1.64*float64(i))
		insertStmt.Float64(9, 1.64*float64(i))
		stringInsert = append(stringInsert, fmt.Sprintf("string %d", i))
		insertStmt.String(10, fmt.Sprintf("string %d", i))
		byteInsert = append(byteInsert, []byte{10, 20, 30, 40})
		insertStmt.Buffer(11, []byte{10, 20, 30, 40})
		fixedStringInsert = append(fixedStringInsert, []byte("01"))
		insertStmt.FixedString(12, []byte("01"))
		array := [][]uint8{
			{
				1, 2, 3, 4,
			}, {
				5, 6, 7, 8, 9,
			},
		}
		insertStmt.AddLen(13, uint64(len(array)))
		for _, a := range array {
			insertStmt.AddLen(14, uint64(len(a)))
			for _, u8 := range a {
				insertStmt.Uint8(15, u8)
			}
		}
		arrayInsert = append(arrayInsert, array)
		d := now.AddDate(0, 0, i)
		insertStmt.Date(16, d)
		dateInsert = append(dateInsert, d.Truncate(time.Hour*24))
		insertStmt.DateTime(17, d)
		datetimeInsert = append(datetimeInsert, d.Truncate(time.Second))
		insertStmt.Decimal32(18, 1.64*float64(i), 4)

		decimalInsert = append(decimalInsert, math.Floor(1.64*float64(i)*10000)/10000)

		insertStmt.Decimal64(19, 1.64*float64(i), 4)

		insertStmt.UUID(20, uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a50"))
		uuidInsert = append(uuidInsert, uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a50"))

		insertStmt.Uint8(21, uint8(1*i))
		tuple1Insert = append(tuple1Insert, uint8(1*i))
		insertStmt.String(22, fmt.Sprintf("string %d", i))
		tuple2Insert = append(tuple2Insert, fmt.Sprintf("string %d", i))

		err := insertStmt.IPv4(23, net.ParseIP("1.2.3.4").To4())
		require.NoError(t, err)
		err = insertStmt.IPv6(24, net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:733").To16())
		require.NoError(t, err)
	}

	err = insertStmt.Commit(context.Background())
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
				tuple
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

	for selectStmt.Next() {

		selectStmt.NextColumn()
		err := selectStmt.Int8All(&int8Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Int16All(&int16Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Int32All(&int32Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Int64All(&int64Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Uint8All(&uint8Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Uint16All(&uint16Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Uint32All(&uint32Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Uint64All(&uint64Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Float32All(&float32Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Float64All(&float64Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.StringAll(&stringData)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.ByteArrayAll(&byteData)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.FixedStringAll(2, &fixedStringData)
		require.NoError(t, err)

		selectStmt.NextColumn()
		// clear array
		len1 = len1[:]
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

		selectStmt.NextColumn()
		err = selectStmt.DateAll(&dateData)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.DateTimeAll(&datetimeData)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Decimal32All(&decimal32Data, 4)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Decimal64All(&decimal64Data, 4)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.UUIDAll(&uuidData)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Uint8All(&tuple1Data)
		require.NoError(t, err)
		err = selectStmt.StringAll(&tuple2Data)
		require.NoError(t, err)

	}

	require.NoError(t, selectStmt.LastErr)
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
	assert.Equal(t, tuple1Insert, tuple1Data)
	assert.Equal(t, tuple2Insert, tuple2Data)
	conn.conn.Close()
}
