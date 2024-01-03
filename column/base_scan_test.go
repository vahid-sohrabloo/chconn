package column_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/types"
)

func TestScanUint8(t *testing.T) {
	testColumnScan(t, "toUInt8(number)")
}

func TestScanInt8(t *testing.T) {
	testColumnScan(t, "toInt8(number)")
}

func TestScanUint16(t *testing.T) {
	testColumnScan(t, "toUInt16(number)")
}

func TestScanInt16(t *testing.T) {
	testColumnScan(t, "toInt16(number)")
}

func TestScanUint32(t *testing.T) {
	testColumnScan(t, "toUInt32(number)")
}

func TestScanInt32(t *testing.T) {
	testColumnScan(t, "toInt32(number)")
}

func TestScanUint64(t *testing.T) {
	testColumnScan(t, "toUInt64(number)")
}

func TestScanInt64(t *testing.T) {
	testColumnScan(t, "toInt64(number)")
}

func TestScanFloat32(t *testing.T) {
	testColumnScan(t, "toFloat32(number)")
}

func TestScanFloat64(t *testing.T) {
	testColumnScan(t, "toFloat64(number)")
}

func testColumnScan(
	t *testing.T,
	toType string,
) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	selectSql := "SELECT " + toType + " from system.numbers limit 10"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	rows, err := conn.Query(ctx, selectSql)
	require.NoError(t, err)
	var i int64
	for rows.Next() {
		testScanEqual[uint8](t, rows, uint8(i))
		testScanEqual[int8](t, rows, int8(i))
		testScanEqual[uint16](t, rows, uint16(i))
		testScanEqual[int16](t, rows, int16(i))
		testScanEqual[uint32](t, rows, uint32(i))
		testScanEqual[int32](t, rows, int32(i))
		testScanEqual[uint64](t, rows, uint64(i))
		testScanEqual[int64](t, rows, int64(i))
		testScanEqual[float32](t, rows, float32(i))
		testScanEqual[float64](t, rows, float64(i))
		testScanEqual[string](t, rows, strconv.Itoa(int(i)))
		testScanEqual[types.Decimal32](t, rows, types.Decimal32(i))
		testScanEqual[types.Decimal64](t, rows, types.Decimal64(i))
		testScanEqual[types.Decimal128](t, rows, types.Decimal128(types.Int128From64(i)))
		// testScanEqual[bool](t, rows, bool(i))
		i++
	}
	require.NoError(t, rows.Err())

}

func testScanEqual[T any](t *testing.T, rows chconn.Rows, i T) {
	var varT T
	err := rows.Scan(&varT)
	assert.NoError(t, err)
	assert.Equal(t, i, varT)
}
