package column_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/format"
	"github.com/vahid-sohrabloo/chconn/v3/types"
)

func TestDate(t *testing.T) {
	testDateColumn(t, true, "Date", "date", func(i int) time.Time {
		return time.Date(2020, 1, i, 0, 0, 0, 0, time.UTC)
	}, func(i int) time.Time {
		return time.Date(2020, 1, i+1, 0, 0, 0, 0, time.UTC)
	}, func() *column.Date[types.Date] {
		return column.NewDate[types.Date]()
	})
}
func TestDate32(t *testing.T) {
	testDateColumn(t, true, "Date32", "date32", func(i int) time.Time {
		return time.Date(2020, 1, i, 0, 0, 0, 0, time.UTC)
	}, func(i int) time.Time {
		return time.Date(2020, 1, i+1, 0, 0, 0, 0, time.UTC)
	}, func() *column.Date[types.Date32] {
		return column.NewDate[types.Date32]()
	})
}

func TestDateTime(t *testing.T) {
	testDateColumn(t, true, "DateTime", "dateTime", func(i int) time.Time {
		return time.Date(2020, 1, i, 0, 0, i+1, 0, time.Local)
	}, func(i int) time.Time {
		return time.Date(2020, 1, i, 0, 0, i+2, 0, time.Local)
	}, func() *column.Date[types.DateTime] {
		return column.NewDate[types.DateTime]()
	})
}
func TestDateTimeTimezone(t *testing.T) {
	testDateColumn(t, true, "DateTime('America/New_York')", "dateTime_timezone", func(i int) time.Time {
		loc, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		return time.Date(2020, 1, i, 0, 0, i+1, 0, loc)
	}, func(i int) time.Time {
		loc, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		return time.Date(2020, 1, i, 0, 0, i+2, 0, loc)
	}, func() *column.Date[types.DateTime] {
		return column.NewDate[types.DateTime]()
	})
}

func TestDateTime64(t *testing.T) {
	testDateColumn(t, false, "DateTime64(9, 'America/New_York')", "dateTime64", func(i int) time.Time {
		loc, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		return time.Date(2020, 1, i, 0, 0, i+1, i+110, loc)
	}, func(i int) time.Time {
		loc, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		return time.Date(2020, 1, i, 0, 0, i+1, i+1101, loc)
	}, func() *column.Date[types.DateTime64] {
		return column.NewDate[types.DateTime64]().SetPrecision(9)
	})
}

func testDateColumn[T column.DateType[T]](
	t *testing.T,
	isLC bool,
	chType, tableName string,
	firstVal func(i int) time.Time,
	secondVal func(i int) time.Time,
	getBaseColumn func() *column.Date[T],
) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	err = conn.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS test_%s`, tableName),
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:  "allow_suspicious_low_cardinality_types",
			Value: "true",
		},
	}

	var sqlCreate string
	if isLC {
		sqlCreate = fmt.Sprintf(`CREATE TABLE test_%[1]s (
			block_id UInt8,
			%[1]s %[2]s,
			%[1]s_nullable Nullable(%[2]s),
			%[1]s_array Array(%[2]s),
			%[1]s_array_nullable Array(Nullable(%[2]s)),
			%[1]s_lc LowCardinality(%[2]s),
			%[1]s_nullable_lc LowCardinality(Nullable(%[2]s)),
			%[1]s_array_lc Array(LowCardinality(%[2]s)),
			%[1]s_array_lc_nullable Array(LowCardinality(Nullable(%[2]s)))
		) Engine=Memory`, tableName, chType)
	} else {
		sqlCreate = fmt.Sprintf(`CREATE TABLE test_%[1]s (
			block_id UInt8,
			%[1]s %[2]s,
			%[1]s_nullable Nullable(%[2]s),
			%[1]s_array Array(%[2]s),
			%[1]s_array_nullable Array(Nullable(%[2]s))
		) Engine=Memory`, tableName, chType)
	}
	err = conn.ExecWithOption(context.Background(), sqlCreate, &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)
	blockID := column.New[uint8]()
	col := getBaseColumn()
	colNullable := getBaseColumn().Nullable()
	colArray := getBaseColumn().Array()
	colNullableArray := getBaseColumn().Nullable().Array()
	colLC := getBaseColumn().LC()
	colLCNullable := getBaseColumn().LC().Nullable()
	colArrayLC := getBaseColumn().LC().Array()
	colArrayLCNullable := getBaseColumn().LC().Nullable().Array()
	var colInsert []time.Time
	var colNullableInsert []*time.Time
	var colArrayInsert [][]time.Time
	var colArrayNullableInsert [][]*time.Time
	var colLCInsert []time.Time
	var colLCNullableInsert []*time.Time
	var colLCArrayInsert [][]time.Time
	var colLCNullableArrayInsert [][]*time.Time

	// SetWriteBufferSize is not necessary. this just to show how to set the write buffer
	col.SetWriteBufferSize(10)
	colNullable.SetWriteBufferSize(10)
	colArray.SetWriteBufferSize(10)
	colNullableArray.SetWriteBufferSize(10)
	colLC.SetWriteBufferSize(10)
	colLCNullable.SetWriteBufferSize(10)
	colArrayLC.SetWriteBufferSize(10)
	colArrayLCNullable.SetWriteBufferSize(10)
	for insertN := 0; insertN < 2; insertN++ {
		rows := 1
		for i := 0; i < rows; i++ {
			blockID.Append(uint8(insertN))
			val := firstVal(i)
			val2 := secondVal(i)
			valArray := []time.Time{val, val2}
			valArrayNil := []*time.Time{&val, nil}

			col.Append(val)
			colInsert = append(colInsert, val)

			// example add nullable
			if i%2 == 0 {
				colNullableInsert = append(colNullableInsert, &val)
				colNullable.Append(val)
				colLCNullableInsert = append(colLCNullableInsert, &val)
				colLCNullable.Append(val)
			} else {
				colNullableInsert = append(colNullableInsert, nil)
				colNullable.AppendNil()
				colLCNullableInsert = append(colLCNullableInsert, nil)
				colLCNullable.AppendNil()
			}

			colArray.Append(valArray)
			colArrayInsert = append(colArrayInsert, valArray)

			colNullableArray.AppendP(valArrayNil)
			colArrayNullableInsert = append(colArrayNullableInsert, valArrayNil)

			colLCInsert = append(colLCInsert, val)
			colLC.Append(val)

			colLCArrayInsert = append(colLCArrayInsert, valArray)
			colArrayLC.Append(valArray)

			colLCNullableArrayInsert = append(colLCNullableArrayInsert, valArrayNil)
			colArrayLCNullable.AppendP(valArrayNil)
		}
		if isLC {
			err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				block_id,
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable,
				%[1]s_lc,
				%[1]s_nullable_lc,
				%[1]s_array_lc,
				%[1]s_array_lc_nullable
			)
		VALUES`, tableName),
				blockID,
				col,
				colNullable,
				colArray,
				colNullableArray,
				colLC,
				colLCNullable,
				colArrayLC,
				colArrayLCNullable,
			)
		} else {
			err = conn.Insert(context.Background(), fmt.Sprintf(`INSERT INTO
			test_%[1]s (
				block_id,
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable
			)
		VALUES`, tableName),
				blockID,
				col,
				colNullable,
				colArray,
				colNullableArray,
			)
		}

		require.NoError(t, err)
	}

	// test read all
	colRead := getBaseColumn()
	colNullableRead := getBaseColumn().Nullable()
	colArrayRead := getBaseColumn().Array()
	colNullableArrayRead := getBaseColumn().Nullable().Array()
	colLCRead := getBaseColumn().LC()
	colLCNullableRead := getBaseColumn().LC().Nullable()
	colArrayLCRead := getBaseColumn().LC().Array()
	colArrayLCNullableRead := getBaseColumn().LC().Nullable().Array()
	var selectQuery string
	if isLC {
		selectQuery = fmt.Sprintf(`SELECT
		%[1]s,
		%[1]s_nullable,
		%[1]s_array,
		%[1]s_array_nullable,
		%[1]s_lc,
		%[1]s_nullable_lc,
		%[1]s_array_lc,
		%[1]s_array_lc_nullable
	FROM test_%[1]s order by block_id`, tableName)
	} else {
		selectQuery = fmt.Sprintf(`SELECT
			%[1]s,
			%[1]s_nullable,
			%[1]s_array,
			%[1]s_array_nullable
		FROM test_%[1]s order by block_id`, tableName)
	}
	var selectStmt chconn.SelectStmt
	if isLC {
		selectStmt, err = conn.Select(context.Background(), selectQuery,
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
			colLCRead,
			colLCNullableRead,
			colArrayLCRead,
			colArrayLCNullableRead,
		)
	} else {
		selectStmt, err = conn.Select(context.Background(), selectQuery,
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
		)
	}

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	var colData []time.Time
	var colNullableData []*time.Time
	var colArrayData [][]time.Time
	var colArrayNullableData [][]*time.Time
	var colLCData []time.Time
	var colLCNullableData []*time.Time
	var colLCArrayData [][]time.Time
	var colLCNullableArrayData [][]*time.Time

	for selectStmt.Next() {
		colData = colRead.Read(colData)
		colNullableData = colNullableRead.ReadP(colNullableData)
		colArrayData = colArrayRead.Read(colArrayData)
		colArrayNullableData = colNullableArrayRead.ReadP(colArrayNullableData)
		if isLC {
			colLCData = colLCRead.Read(colLCData)
			colLCNullableData = colLCNullableRead.ReadP(colLCNullableData)
			colLCArrayData = colArrayLCRead.Read(colLCArrayData)
			colLCNullableArrayData = colArrayLCNullableRead.ReadP(colLCNullableArrayData)
		}
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNullableInsert, colNullableData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
	if isLC {
		assert.Equal(t, colLCInsert, colLCData)
		assert.Equal(t, colLCNullableInsert, colLCNullableData)
		assert.Equal(t, colLCArrayInsert, colLCArrayData)
		assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)
	}

	// test row
	colRead = getBaseColumn()
	colNullableRead = getBaseColumn().Nullable()
	colArrayRead = getBaseColumn().Array()
	colNullableArrayRead = getBaseColumn().Nullable().Array()
	colLCRead = getBaseColumn().LowCardinality()
	colLCNullableRead = getBaseColumn().LowCardinality().Nullable()
	colArrayLCRead = getBaseColumn().LowCardinality().Array()
	colArrayLCNullableRead = getBaseColumn().LowCardinality().Nullable().Array()
	if isLC {
		selectStmt, err = conn.Select(context.Background(), selectQuery,
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
			colLCRead,
			colLCNullableRead,
			colArrayLCRead,
			colArrayLCNullableRead,
		)
	} else {
		selectStmt, err = conn.Select(context.Background(), selectQuery,
			colRead,
			colNullableRead,
			colArrayRead,
			colNullableArrayRead,
		)
	}

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	colData = colData[:0]
	colNullableData = colNullableData[:0]
	colArrayData = colArrayData[:0]
	colArrayNullableData = colArrayNullableData[:0]
	colLCData = colLCData[:0]
	colLCNullableData = colLCNullableData[:0]
	colLCArrayData = colLCArrayData[:0]
	colLCNullableArrayData = colLCNullableArrayData[:0]

	for selectStmt.Next() {
		for i := 0; i < selectStmt.RowsInBlock(); i++ {
			colData = append(colData, colRead.Row(i))
			colNullableData = append(colNullableData, colNullableRead.RowP(i))
			colArrayData = append(colArrayData, colArrayRead.Row(i))
			colArrayNullableData = append(colArrayNullableData, colNullableArrayRead.RowP(i))
			if isLC {
				colLCData = append(colLCData, colLCRead.Row(i))
				colLCNullableData = append(colLCNullableData, colLCNullableRead.RowP(i))
				colLCArrayData = append(colLCArrayData, colArrayLCRead.Row(i))
				colLCNullableArrayData = append(colLCNullableArrayData, colArrayLCNullableRead.RowP(i))
			}
		}
	}

	require.NoError(t, selectStmt.Err())

	assert.Equal(t, colInsert, colData)
	assert.Equal(t, colNullableInsert, colNullableData)
	assert.Equal(t, colArrayInsert, colArrayData)
	assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
	if isLC {
		assert.Equal(t, colLCInsert, colLCData)
		assert.Equal(t, colLCNullableInsert, colLCNullableData)
		assert.Equal(t, colLCArrayInsert, colLCArrayData)
		assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)
	}

	// check dynamic column

	selectStmt, err = conn.Select(context.Background(), selectQuery)

	require.NoError(t, err)
	autoColumns := selectStmt.Columns()
	if isLC {
		assert.Len(t, autoColumns, 8)
		assert.Equal(t, colRead.FullType(), autoColumns[0].FullType())
		assert.Equal(t, colNullableRead.FullType(), autoColumns[1].FullType())
		assert.Equal(t, colArrayRead.FullType(), autoColumns[2].FullType())
		assert.Equal(t, colNullableArrayRead.FullType(), autoColumns[3].FullType())
		assert.Equal(t, colLCRead.FullType(), autoColumns[4].FullType())
		assert.Equal(t, colLCNullableRead.FullType(), autoColumns[5].FullType())
		assert.Equal(t, colArrayLCRead.FullType(), autoColumns[6].FullType())
		assert.Equal(t, colArrayLCNullableRead.FullType(), autoColumns[7].FullType())
	} else {
		assert.Len(t, autoColumns, 4)
		assert.Equal(t, colRead.FullType(), autoColumns[0].FullType())
		assert.Equal(t, colNullableRead.FullType(), autoColumns[1].FullType())
		assert.Equal(t, colArrayRead.FullType(), autoColumns[2].FullType())
		assert.Equal(t, colNullableArrayRead.FullType(), autoColumns[3].FullType())
	}

	for selectStmt.Next() {
	}
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()

	// check dynamic column
	if isLC {
		selectStmt, err = conn.SelectWithOption(context.Background(), fmt.Sprintf(`SELECT
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable,
				%[1]s_lc,
				%[1]s_nullable_lc,
				%[1]s_array_lc,
				%[1]s_array_lc_nullable
				FROM test_%[1]s`,
			tableName),
			&chconn.QueryOptions{},
		)
	} else {
		selectStmt, err = conn.SelectWithOption(context.Background(), fmt.Sprintf(`SELECT
						%[1]s,
						%[1]s_nullable,
						%[1]s_array,
						%[1]s_array_nullable
					FROM test_%[1]s`, tableName,
		),
			&chconn.QueryOptions{},
		)
	}
	require.NoError(t, err)
	autoColumns = selectStmt.Columns()
	if isLC {
		assert.Len(t, autoColumns, 8)
		assert.Equal(t, colRead.FullType(), autoColumns[0].FullType())
		assert.Equal(t, colNullableRead.FullType(), autoColumns[1].FullType())
		assert.Equal(t, colArrayRead.FullType(), autoColumns[2].FullType())
		assert.Equal(t, colNullableArrayRead.FullType(), autoColumns[3].FullType())
		assert.Equal(t, colLCRead.FullType(), autoColumns[4].FullType())
		assert.Equal(t, colLCNullableRead.FullType(), autoColumns[5].FullType())
		assert.Equal(t, colArrayLCRead.FullType(), autoColumns[6].FullType())
		assert.Equal(t, colArrayLCNullableRead.FullType(), autoColumns[7].FullType())
	} else {
		assert.Len(t, autoColumns, 4)
		assert.Equal(t, colRead.FullType(), autoColumns[0].FullType())
		assert.Equal(t, colNullableRead.FullType(), autoColumns[1].FullType())
		assert.Equal(t, colArrayRead.FullType(), autoColumns[2].FullType())
		assert.Equal(t, colNullableArrayRead.FullType(), autoColumns[3].FullType())
	}

	rows := selectStmt.Rows()

	colData = colData[:0]
	colNullableData = colNullableData[:0]
	colArrayData = colArrayData[:0]
	colArrayNullableData = colArrayNullableData[:0]
	colLCData = colLCData[:0]
	colLCNullableData = colLCNullableData[:0]
	colLCArrayData = colLCArrayData[:0]
	colLCNullableArrayData = colLCNullableArrayData[:0]

	for rows.Next() {
		var colVal time.Time
		var colNullableVal *time.Time
		var colArrayVal []time.Time
		var colArrayNullableVal []*time.Time
		var colLCVal time.Time
		var colLCNullableVal *time.Time
		var colLCArrayVal []time.Time
		var colLCNullableArrayVal []*time.Time
		if isLC {
			err := rows.Scan(
				&colVal,
				&colNullableVal,
				&colArrayVal,
				&colArrayNullableVal,
				&colLCVal,
				&colLCNullableVal,
				&colLCArrayVal,
				&colLCNullableArrayVal,
			)
			require.NoError(t, err)
		} else {
			err := rows.Scan(
				&colVal,
				&colNullableVal,
				&colArrayVal,
				&colArrayNullableVal,
			)
			require.NoError(t, err)
		}

		colData = append(colData, colVal)
		colNullableData = append(colNullableData, colNullableVal)
		colArrayData = append(colArrayData, colArrayVal)
		colArrayNullableData = append(colArrayNullableData, colArrayNullableVal)
		colLCData = append(colLCData, colLCVal)
		colLCNullableData = append(colLCNullableData, colLCNullableVal)
		colLCArrayData = append(colLCArrayData, colLCArrayVal)
		colLCNullableArrayData = append(colLCNullableArrayData, colLCNullableArrayVal)
	}
	require.NoError(t, selectStmt.Err())
	if isLC {
		assert.Equal(t, colInsert, colData)
		assert.Equal(t, colNullableInsert, colNullableData)
		assert.Equal(t, colArrayInsert, colArrayData)
		assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
		assert.Equal(t, colLCInsert, colLCData)
		assert.Equal(t, colLCNullableInsert, colLCNullableData)
		assert.Equal(t, colLCArrayInsert, colLCArrayData)
		assert.Equal(t, colLCNullableArrayInsert, colLCNullableArrayData)
	} else {
		assert.Equal(t, colInsert, colData)
		assert.Equal(t, colNullableInsert, colNullableData)
		assert.Equal(t, colArrayInsert, colArrayData)
		assert.Equal(t, colArrayNullableInsert, colArrayNullableData)
	}

	selectStmt.Close()
	
	var chconnJSON []string
	jsonFormat := format.NewJSON(1000, func(b []byte, cb []column.ColumnBasic) {
		chconnJSON = append(chconnJSON, string(b))
	})

	// check JSON
	selectStmt, err = conn.Select(context.Background(), selectQuery)

	require.NoError(t, err)

	err = jsonFormat.ReadEachRow(selectStmt)
	require.NoError(t, err)

	jsonFromClickhouse := httpJSON(selectQuery)

	d := json.NewDecoder(strings.NewReader(strings.Join(chconnJSON, "\n")))
	var valsChconn []any
	iff := 0
	for {
		var v any
		if err := d.Decode(&v); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		iff++
		valsChconn = append(valsChconn, v)
	}

	d = json.NewDecoder(bytes.NewReader(jsonFromClickhouse))
	var valsClickhouse []any
	for {
		var v any
		if err := d.Decode(&v); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		valsClickhouse = append(valsClickhouse, v)
	}

	assert.Equal(t, valsClickhouse, valsChconn)
}

func TestInvalidNegativeTimes(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	err = conn.Exec(context.Background(),
		`DROP TABLE IF EXISTS test_invalid_dates`,
	)
	require.NoError(t, err)
	set := chconn.Settings{
		{
			Name:  "allow_suspicious_low_cardinality_types",
			Value: "true",
		},
	}

	sqlCreate := `CREATE TABLE test_invalid_dates (
			date Date,
			date32 Date32,
			dateTime DateTime,
			dateTime64 DateTime64(3)
		) Engine=Memory`

	err = conn.ExecWithOption(context.Background(), sqlCreate, &chconn.QueryOptions{
		Settings: set,
	})

	require.NoError(t, err)

	colDate := column.NewDate[types.Date]()
	colDate32 := column.NewDate[types.Date32]()
	colDateTime := column.NewDate[types.DateTime]()
	colDateTime64 := column.NewDate[types.DateTime64]()
	invalidTime := time.Unix(-3208988700, 0) // 1868
	colDate.Append(invalidTime)
	colDate32.Append(invalidTime)
	colDateTime.Append(invalidTime)
	colDateTime64.Append(invalidTime)

	err = conn.Insert(context.Background(), `INSERT INTO
	test_invalid_dates (
				date,
				date32,
				dateTime,
				dateTime64
			)
		VALUES`,
		colDate,
		colDate32,
		colDateTime,
		colDateTime64,
	)
	require.NoError(t, err)

	// test read all
	colDateRead := column.NewDate[types.Date]()
	colDate32Read := column.NewDate[types.Date32]()
	colDateTimeRead := column.NewDate[types.DateTime]()
	colDateTime64Read := column.NewDate[types.DateTime64]()
	var selectStmt chconn.SelectStmt
	selectStmt, err = conn.Select(context.Background(), `SELECT
		date,
		date32,
		dateTime,
		dateTime64
		FROM test_invalid_dates`,
		colDateRead,
		colDate32Read,
		colDateTimeRead,
		colDateTime64Read,
	)

	require.NoError(t, err)
	require.True(t, conn.IsBusy())

	for selectStmt.Next() {
	}
	// YYYY-MM-DD hh:mm:ss
	assert.Equal(t, colDateRead.Row(0).In(time.UTC).Format("2006-01-02 15:04:05"), "1970-01-01 00:00:00")
	assert.Equal(t, colDate32Read.Row(0).In(time.UTC).Format("2006-01-02 15:04:05"), "1900-01-01 00:00:00")
	assert.Equal(t, colDateTimeRead.Row(0).In(time.UTC).Format("2006-01-02 15:04:05"), "1970-01-01 00:00:00")
	assert.Equal(t, colDateTime64Read.Row(0).In(time.UTC).Format("2006-01-02 15:04:05"), "1900-01-01 00:00:00")

	require.NoError(t, selectStmt.Err())
}
