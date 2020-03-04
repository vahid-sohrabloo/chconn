package chconn

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "CHX_TEST_TCP_CONN_STRING")
	}

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))

	// conn.conn.Close()
}

func TestEndOfStream(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "CHX_TEST_TCP_CONN_STRING")
	}

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))
	res, err := conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS example (
				country_code FixedString(2),
				os_id        UInt8,
				browser_id   UInt8,
				categories   Array(Int16),
				action_day   Date,
				action_time  DateTime
			) engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)
}

func TestException(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "CHX_TEST_TCP_CONN_STRING")
	}

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))
	res, err := conn.Exec(context.Background(), `invalid query`)

	require.Nil(t, res)
	var chError *ChError
	require.True(t, errors.As(err, &chError))
	require.Equal(t, chError.Code, int32(62))
	require.Equal(t, chError.Name, "DB::Exception")

}

func TestInsert(t *testing.T) {
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
				nString  Nullable(UInt8),
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
				nString,
				ipv4,
				ipv6
			) VALUES`)
	require.NoError(t, err)
	require.Nil(t, res)
	now := time.Now()
	for i := 1; i <= 10; i++ {
		insertStmt.Block.NumRows++
		insertStmt.Int8(0, int8(-1*i))
		insertStmt.Int16(1, int16(-2*i))
		insertStmt.Int32(2, int32(-4*i))
		insertStmt.Int64(3, int64(-8*i))
		insertStmt.Uint8(4, uint8(1*i))
		insertStmt.Uint16(5, uint16(2*i))
		insertStmt.Uint32(6, uint32(4*i))
		insertStmt.Uint64(7, uint64(8*i))
		insertStmt.Float32(8, 1.32*float32(i))
		insertStmt.Float64(9, 1.64*float64(i))
		insertStmt.String(10, fmt.Sprintf("string %d", i))
		insertStmt.Buffer(11, []byte{10, 20, 30, 40})
		insertStmt.FixedString(12, []byte("01"))
		array := [][]uint8{
			{
				1, 2, 3, 4,
			}, {
				5, 6, 7, 8,
			},
		}
		insertStmt.AddOffset(13, uint64(len(array)))
		for _, a := range array {
			insertStmt.AddOffset(14, uint64(len(a)))
			for _, u8 := range a {
				insertStmt.Uint8(15, u8)
			}
		}

		insertStmt.Date(16, now.AddDate(0, 0, i))
		insertStmt.DateTime(17, now.AddDate(0, 0, i))
		insertStmt.Decimal32(18, 1.64*float64(i), 4)
		insertStmt.Decimal64(19, 1.64*float64(i), 4)
		insertStmt.UUID(20, uuid.MustParse("417ddc5d-e556-4d27-95dd-a34d84e46a50"))

		insertStmt.Uint8(21, uint8(1*i))
		insertStmt.String(22, fmt.Sprintf("string %d", i))

		insertStmt.Uint8(23, 1)
		insertStmt.Uint8(24, 0)
		err := insertStmt.IPv4(25, net.ParseIP("1.2.3.4").To4())
		require.NoError(t, err)
		err = insertStmt.IPv6(26, net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:733").To16())
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
				fString
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
	for selectStmt.Next() {
		selectStmt.NextColumn()
		err := selectStmt.Int8(&int8Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Int16(&int16Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Int32(&int32Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Int64(&int64Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Uint8(&uint8Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Uint16(&uint16Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Uint32(&uint32Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Uint64(&uint64Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Float32(&float32Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.Float64(&float64Data)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.String(&stringData)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.ByteArray(&byteData)
		require.NoError(t, err)
		selectStmt.NextColumn()
		err = selectStmt.FixedString(2, &fixedStringData)
		require.NoError(t, err)
	}
	require.NoError(t, selectStmt.LastErr)
	conn.conn.Close()
}

func TestSelect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "CHX_TEST_TCP_CONN_STRING")
	}

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	selectStmt, err := conn.Select(context.Background(), "SELECT * FROM numbers(10)")
	var numbers []uint64
	for selectStmt.Next() {
		err = selectStmt.Uint64(&numbers)
		require.NoError(t, err)
	}
	conn.conn.Close()
}

func TestTlsConnect(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_TLS_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "CHX_TEST_TCP_TLS_CONN_STRING")
	}

	conn, err := Connect(context.Background(), connString)

	require.NoError(t, err)

	require.NoError(t, conn.Ping(context.Background()))

	if _, ok := conn.conn.(*tls.Conn); !ok {
		t.Error("not a TLS connection")
	}

	conn.conn.Close()
}
