package column_test

// func TestScanUint8(t *testing.T) {
// 	testColumnScan(t, "toUInt8(number)", true, nil)
// }

// func TestScanInt8(t *testing.T) {
// 	testColumnScan(t, "toInt8(number)", true, nil)
// }

// func TestScanUint16(t *testing.T) {
// 	testColumnScan(t, "toUInt16(number)", true, nil)
// }

// func TestScanInt16(t *testing.T) {
// 	testColumnScan(t, "toInt16(number)", true, nil)
// }

// func TestScanUint32(t *testing.T) {
// 	testColumnScan(t, "toUInt32(number)", true, nil)
// }

// func TestScanInt32(t *testing.T) {
// 	testColumnScan(t, "toInt32(number)", true, nil)
// }

// func TestScanUint64(t *testing.T) {
// 	testColumnScan(t, "toUInt64(number)", true, nil)
// }

// func TestScanInt64(t *testing.T) {
// 	testColumnScan(t, "toInt64(number)", true, nil)
// }

// func TestScanFloat32(t *testing.T) {
// 	testColumnScan(t, "toFloat32(number)", true, nil)
// }

// func TestScanFloat64(t *testing.T) {
// 	testColumnScan(t, "toFloat64(number)", true, nil)
// }

// func TestScanInt128(t *testing.T) {
// 	testColumnScan(t, "toInt128(number)", false, nil)
// }

// func TestScanInt256(t *testing.T) {
// 	testColumnScan(t, "toInt256(number)", false, nil)
// }

// func TestScanUint128(t *testing.T) {
// 	testColumnScan(t, "toUInt128(number)", false, nil)
// }

// func TestScanUint256(t *testing.T) {
// 	testColumnScan(t, "toUInt256(number)", false, nil)
// }

// func TestScanDecimal32(t *testing.T) {
// 	testColumnScan(t, "toDecimal32(number, 3)", true, strconv.Itoa)
// }

// func TestScanDecimal64(t *testing.T) {
// 	testColumnScan(t, "toDecimal64(number, 3)", true, strconv.Itoa)
// }

// func TestScanDecimal128(t *testing.T) {
// 	testColumnScan(t, "toDecimal128(number, 3)", false, strconv.Itoa)
// }

// func TestScanDecimal256(t *testing.T) {
// 	testColumnScan(t, "toDecimal256(number, 3)", false, strconv.Itoa)
// }
// func testColumnScan(
// 	t *testing.T,
// 	toType string,
// 	testAny bool,
// 	strFunc func(int) string,
// ) {
// 	t.Parallel()

// 	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

// 	conn, err := chconn.Connect(context.Background(), connString)
// 	require.NoError(t, err)

// 	selectSQL := "SELECT " + toType + " from system.numbers limit 10"
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
// 	defer cancel()
// 	rows, err := conn.Query(ctx, selectSQL)
// 	require.NoError(t, err)
// 	var i int64
// 	for rows.Next() {
// 		testScanEqual[uint8](t, rows, uint8(i))
// 		testScanEqual[int8](t, rows, int8(i))
// 		testScanEqual[uint16](t, rows, uint16(i))
// 		testScanEqual[int16](t, rows, int16(i))
// 		testScanEqual[uint32](t, rows, uint32(i))
// 		testScanEqual[int32](t, rows, int32(i))
// 		testScanEqual[uint64](t, rows, uint64(i))
// 		testScanEqual[int64](t, rows, i)
// 		testScanEqual[float32](t, rows, float32(i))
// 		testScanEqual[float64](t, rows, float64(i))
// 		testScanEqual[types.Int128](t, rows, types.Int128From64(i))
// 		testScanEqual[types.Int256](t, rows, types.Int256From64(i))
// 		testScanEqual[types.Uint128](t, rows, types.Uint128From64(uint64(i)))
// 		testScanEqual[types.Uint256](t, rows, types.Uint256From64(uint64(i)))
// 		str := strconv.Itoa(int(i))
// 		if strFunc != nil {
// 			str = strFunc(int(i))
// 		}
// 		testScanEqual[string](t, rows, str)
// 		testScanEqual[*string](t, rows, &str)
// 		testScanEqual[bool](t, rows, i > 0)
// 		if testAny {
// 			var varAny any
// 			err := rows.Scan(&varAny)
// 			assert.NoError(t, err)
// 			assert.EqualValues(t, i, varAny)
// 		}
// 		i++
// 	}
// 	require.NoError(t, rows.Err())
// }

// func testScanEqual[T any](t *testing.T, rows chconn.Rows, i T) {
// 	var varT T
// 	err := rows.Scan(&varT)
// 	assert.NoError(t, err)
// 	assert.Equal(t, i, varT)
// }
