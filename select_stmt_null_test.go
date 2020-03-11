package chconn

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectNull(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "CHX_TEST_TCP_CONN_STRING")
	}

	conn, err := Connect(context.Background(), connString)
	require.NoError(t, err)

	res, err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS clickhouse_test_insert_null`)
	require.NoError(t, err)
	require.Nil(t, res)
	res, err = conn.Exec(context.Background(), `CREATE TABLE clickhouse_test_insert_null (
				int8  Nullable(Int8),
				int16 Nullable(Int16),
				int32 Nullable(Int32),
				int64 Nullable(Int64),
				uint8  Nullable(UInt8),
				uint16  Nullable(UInt16),
				uint32  Nullable(UInt32),
				uint64  Nullable(UInt64),
				float32  Nullable(Float32),
				float64  Nullable(Float64),
				string  Nullable(String),
				string2  Nullable(String),
				fString Nullable(FixedString(2)),
				array  Array(Array(Nullable(UInt8))),
				date    Nullable(Date),
				datetime    Nullable(DateTime),
				decimal32    Nullable(Decimal32),
				decimal64    Nullable(Decimal64)
			) Engine=Memory`)

	require.NoError(t, err)
	require.Nil(t, res)

	insertStmt, err := conn.Insert(context.Background(), `INSERT INTO clickhouse_test_insert_null (
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
				datetime
				
			) VALUES`)
	require.NoError(t, err)
	require.Nil(t, res)

	var int8Insert []*int8
	var int16Insert []*int16
	var int32Insert []*int32
	var int64Insert []*int64
	var uint8Insert []*uint8
	var uint16Insert []*uint16
	var uint32Insert []*uint32
	var uint64Insert []*uint64
	var float32Insert []*float32
	var float64Insert []*float64
	var stringInsert []*string
	var byteInsert [][]byte
	var fixedStringInsert [][]byte
	var arrayInsert [][][]*uint8
	var dateInsert []*time.Time
	var datetimeInsert []*time.Time
	now := time.Now()
	for i := 1; i <= 10; i++ {
		insertStmt.Block.NumRows++

		if i%2 == 0 {

			int8Val := int8(-1 * i)
			insertStmt.Int8P(0, &int8Val)
			int8Insert = append(int8Insert, &int8Val)

			int16Val := int16(-2 * i)
			insertStmt.Int16P(2, &int16Val)
			int16Insert = append(int16Insert, &int16Val)

			int32Val := int32(-4 * i)
			insertStmt.Int32P(4, &int32Val)
			int32Insert = append(int32Insert, &int32Val)

			int64Val := int64(-8 * i)
			insertStmt.Int64P(6, &int64Val)
			int64Insert = append(int64Insert, &int64Val)

			uint8Val := uint8(1 * i)
			insertStmt.Uint8P(8, &uint8Val)
			uint8Insert = append(uint8Insert, &uint8Val)

			uint16Val := uint16(2 * i)
			insertStmt.Uint16P(10, &uint16Val)
			uint16Insert = append(uint16Insert, &uint16Val)

			uint32Val := uint32(4 * i)
			insertStmt.Uint32P(12, &uint32Val)
			uint32Insert = append(uint32Insert, &uint32Val)

			uint64Val := uint64(8 * i)
			insertStmt.Uint64P(14, &uint64Val)
			uint64Insert = append(uint64Insert, &uint64Val)

			float32Val := 1.32 * float32(i)
			insertStmt.Float32P(16, &float32Val)
			float32Insert = append(float32Insert, &float32Val)

			float64Val := 1.64 * float64(i)
			insertStmt.Float64P(18, &float64Val)
			float64Insert = append(float64Insert, &float64Val)

			stringVal := fmt.Sprintf("string %d", i)
			insertStmt.StringP(20, &stringVal)
			stringInsert = append(stringInsert, &stringVal)

			bufferVal := []byte{10, 20, 30, 40}
			insertStmt.BufferP(22, &bufferVal)
			byteInsert = append(byteInsert, bufferVal)

			fixedStringVal := []byte("01")
			insertStmt.FixedStringP(24, []byte{0, 0}, fixedStringVal)
			fixedStringInsert = append(fixedStringInsert, fixedStringVal)

			array := [][]*uint8{
				{
					&uint8Val, nil, &uint8Val, nil,
				}, {
					&uint8Val, nil, &uint8Val, nil, &uint8Val,
				},
			}

			insertStmt.AddLen(26, uint64(len(array)))
			for _, a := range array {
				insertStmt.AddLen(27, uint64(len(a)))
				for _, u8 := range a {
					insertStmt.Uint8P(28, u8)
				}
			}
			arrayInsert = append(arrayInsert, array)

			d := now.AddDate(0, 0, i)
			d = d.Truncate(time.Hour * 24)
			insertStmt.DateP(30, &d)
			dateInsert = append(dateInsert, &d)

			dt := now.AddDate(0, 0, i)
			dt = dt.Truncate(time.Second)
			insertStmt.DateTimeP(32, &dt)
			datetimeInsert = append(datetimeInsert, &dt)
		} else {

			insertStmt.Int8P(0, nil)
			int8Insert = append(int8Insert, nil)

			insertStmt.Int16P(2, nil)
			int16Insert = append(int16Insert, nil)

			insertStmt.Int32P(4, nil)
			int32Insert = append(int32Insert, nil)

			insertStmt.Int64P(6, nil)
			int64Insert = append(int64Insert, nil)

			insertStmt.Uint8P(8, nil)
			uint8Insert = append(uint8Insert, nil)

			insertStmt.Uint16P(10, nil)
			uint16Insert = append(uint16Insert, nil)

			insertStmt.Uint32P(12, nil)
			uint32Insert = append(uint32Insert, nil)

			insertStmt.Uint64P(14, nil)
			uint64Insert = append(uint64Insert, nil)

			insertStmt.Float32P(16, nil)
			float32Insert = append(float32Insert, nil)

			insertStmt.Float64P(18, nil)
			float64Insert = append(float64Insert, nil)

			insertStmt.StringP(20, nil)
			stringInsert = append(stringInsert, nil)

			insertStmt.BufferP(22, nil)
			byteInsert = append(byteInsert, nil)

			insertStmt.FixedStringP(24, []byte{0, 0}, nil)
			fixedStringInsert = append(fixedStringInsert, nil)

			uint8Val := uint8(1 * i)
			array := [][]*uint8{
				{
					nil, &uint8Val, nil, &uint8Val,
				}, {
					nil, &uint8Val, nil, &uint8Val, nil,
				},
			}
			insertStmt.AddLen(26, uint64(len(array)))
			for _, a := range array {
				insertStmt.AddLen(27, uint64(len(a)))
				for _, u8 := range a {
					insertStmt.Uint8P(28, u8)
				}
			}
			arrayInsert = append(arrayInsert, array)

			insertStmt.DateP(30, nil)
			dateInsert = append(dateInsert, nil)

			insertStmt.DateTimeP(32, nil)
			datetimeInsert = append(datetimeInsert, nil)

		}

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
				datetime
	 FROM clickhouse_test_insert_null`)
	require.NoError(t, err)
	var int8Data []*int8
	var int16Data []*int16
	var int32Data []*int32
	var int64Data []*int64
	var uint8Data []*uint8
	var uint16Data []*uint16
	var uint32Data []*uint32
	var uint64Data []*uint64
	var float32Data []*float32
	var float64Data []*float64
	var stringData []*string
	var byteData [][]byte
	var fixedStringData [][]byte
	var arrayData [][][]*uint8
	var len1 []int
	var len2 []int
	var index1 int
	var index2 int
	var indexNull int
	var dateData []*time.Time
	var datetimeData []*time.Time

	for selectStmt.Next() {

		selectStmt.NextColumn()
		err := selectStmt.Int8PAll(&int8Data)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Int16PAll(&int16Data)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Int32PAll(&int32Data)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Int64PAll(&int64Data)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Uint8PAll(&uint8Data)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Uint16PAll(&uint16Data)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Uint32PAll(&uint32Data)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Uint64PAll(&uint64Data)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Float32PAll(&float32Data)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.Float64PAll(&float64Data)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.StringPAll(&stringData)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.ByteArrayPAll(&byteData)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.FixedStringPAll(2, &fixedStringData)
		require.NoError(t, err)

		selectStmt.NextColumn()
		// clear array
		len1 = len1[:]
		// get array lens
		lastOffset, err := selectStmt.LenAll(&len1)
		require.NoError(t, err)
		// get second dimension by last offset from prev array
		lastoffset2, err := selectStmt.LenS(lastOffset, &len2)
		require.NoError(t, err)
		index1 = 0
		index2 = 0
		indexNull = 0
		nulls, err := selectStmt.GetNullS(lastoffset2)
		require.NoError(t, err)
		for index1 < len(len1) {
			arr1 := make([][]*uint8, len1[index1])
			for i := range arr1 {
				arr1[i] = make([]*uint8, 0, len2[index2])
				err = selectStmt.Uint8PS(uint64(len2[index2]), nulls[indexNull:indexNull+len2[index2]], &arr1[i])
				require.NoError(t, err)
				indexNull += len2[index2]
				index2++
			}
			index1++
			arrayData = append(arrayData, arr1)
		}

		selectStmt.NextColumn()
		err = selectStmt.DatePAll(&dateData)
		require.NoError(t, err)

		selectStmt.NextColumn()
		err = selectStmt.DateTimePAll(&datetimeData)
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
	conn.conn.Close()
}
