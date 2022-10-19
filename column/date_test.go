package column_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v2"
	"github.com/vahid-sohrabloo/chconn/v2/column"
	"github.com/vahid-sohrabloo/chconn/v2/types"
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

	col := getBaseColumn()
	colNullable := getBaseColumn().Nullable()
	colArray := getBaseColumn().Array()
	colNullableArray := getBaseColumn().Nullable().Array()
	colLC := getBaseColumn().LC()
	colLCNullable := getBaseColumn().Nullable().LC()
	colArrayLC := getBaseColumn().LC().Array()
	colArrayLCNullable := getBaseColumn().Nullable().LC().Array()
	var colInsert []time.Time
	var colNullableInsert []*time.Time
	var colArrayInsert [][]time.Time
	var colArrayNullableInsert [][]*time.Time
	var colLCInsert []time.Time
	var colLCNullableInsert []*time.Time
	var colLCArrayInsert [][]time.Time
	var colLCNullableArrayInsert [][]*time.Time

	// SetWriteBufferSize is not necessary. this just to show how to set write buffer
	col.SetWriteBufferSize(10)
	colNullable.SetWriteBufferSize(10)
	colArray.SetWriteBufferSize(10)
	colNullableArray.SetWriteBufferSize(10)
	colLC.SetWriteBufferSize(10)
	colLCNullable.SetWriteBufferSize(10)
	colArrayLC.SetWriteBufferSize(10)
	colArrayLCNullable.SetWriteBufferSize(10)
	for insertN := 0; insertN < 2; insertN++ {
		rows := 10
		for i := 0; i < rows; i++ {
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
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable
			)
		VALUES`, tableName),
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
	colLCNullableRead := getBaseColumn().Nullable().LC()
	colArrayLCRead := getBaseColumn().LC().Array()
	colArrayLCNullableRead := getBaseColumn().Nullable().LC().Array()
	var selectStmt chconn.SelectStmt
	if isLC {
		selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
		%[1]s,
		%[1]s_nullable,
		%[1]s_array,
		%[1]s_array_nullable,
		%[1]s_lc,
		%[1]s_nullable_lc,
		%[1]s_array_lc,
		%[1]s_array_lc_nullable
	FROM test_%[1]s`, tableName),
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
		selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
			%[1]s,
			%[1]s_nullable,
			%[1]s_array,
			%[1]s_array_nullable
		FROM test_%[1]s`, tableName),
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
	colLCNullableRead = getBaseColumn().Nullable().LowCardinality()
	colArrayLCRead = getBaseColumn().LowCardinality().Array()
	colArrayLCNullableRead = getBaseColumn().Nullable().LowCardinality().Array()
	if isLC {
		selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
			%[1]s,
			%[1]s_nullable,
			%[1]s_array,
			%[1]s_array_nullable,
			%[1]s_lc,
			%[1]s_nullable_lc,
			%[1]s_array_lc,
			%[1]s_array_lc_nullable
		FROM test_%[1]s`, tableName),
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
		selectStmt, err = conn.Select(context.Background(), fmt.Sprintf(`SELECT
				%[1]s,
				%[1]s_nullable,
				%[1]s_array,
				%[1]s_array_nullable
			FROM test_%[1]s`, tableName),
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
			&chconn.QueryOptions{
				UseGoTime: false,
			},
		)
	} else {
		selectStmt, err = conn.SelectWithOption(context.Background(), fmt.Sprintf(`SELECT
					%[1]s,
					%[1]s_nullable,
					%[1]s_array,
					%[1]s_array_nullable
				FROM test_%[1]s`, tableName,
		),
			&chconn.QueryOptions{
				UseGoTime: false,
			},
		)
	}
	require.NoError(t, err)
	autoColumns := selectStmt.Columns()
	if isLC {
		assert.Len(t, autoColumns, 8)
		assert.Equal(t, column.New[T]().ColumnType(), autoColumns[0].ColumnType())
		assert.Equal(t, column.New[T]().Nullable().ColumnType(), autoColumns[1].ColumnType())
		assert.Equal(t, column.New[T]().Array().ColumnType(), autoColumns[2].ColumnType())
		assert.Equal(t, column.New[T]().Nullable().Array().ColumnType(), autoColumns[3].ColumnType())
		assert.Equal(t, column.New[T]().LowCardinality().ColumnType(), autoColumns[4].ColumnType())
		assert.Equal(t, column.New[T]().Nullable().LowCardinality().ColumnType(), autoColumns[5].ColumnType())
		assert.Equal(t, column.New[T]().LowCardinality().Array().ColumnType(), autoColumns[6].ColumnType())
		assert.Equal(t, column.New[T]().Nullable().LowCardinality().Array().ColumnType(), autoColumns[7].ColumnType())
	} else {
		assert.Len(t, autoColumns, 4)
		assert.Equal(t, column.New[T]().ColumnType(), autoColumns[0].ColumnType())
		assert.Equal(t, column.New[T]().Nullable().ColumnType(), autoColumns[1].ColumnType())
		assert.Equal(t, column.New[T]().Array().ColumnType(), autoColumns[2].ColumnType())
		assert.Equal(t, column.New[T]().Nullable().Array().ColumnType(), autoColumns[3].ColumnType())
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
			&chconn.QueryOptions{
				UseGoTime: true,
			},
		)
	} else {
		selectStmt, err = conn.SelectWithOption(context.Background(), fmt.Sprintf(`SELECT
						%[1]s,
						%[1]s_nullable,
						%[1]s_array,
						%[1]s_array_nullable
					FROM test_%[1]s`, tableName,
		),
			&chconn.QueryOptions{
				UseGoTime: true,
			},
		)
	}
	require.NoError(t, err)
	autoColumns = selectStmt.Columns()
	if isLC {
		assert.Len(t, autoColumns, 8)
		assert.Equal(t, colRead.ColumnType(), autoColumns[0].ColumnType())
		assert.Equal(t, colNullableRead.ColumnType(), autoColumns[1].ColumnType())
		assert.Equal(t, colArrayRead.ColumnType(), autoColumns[2].ColumnType())
		assert.Equal(t, colNullableArrayRead.ColumnType(), autoColumns[3].ColumnType())
		assert.Equal(t, colLCRead.ColumnType(), autoColumns[4].ColumnType())
		assert.Equal(t, colLCNullableRead.ColumnType(), autoColumns[5].ColumnType())
		assert.Equal(t, colArrayLCRead.ColumnType(), autoColumns[6].ColumnType())
		assert.Equal(t, colArrayLCNullableRead.ColumnType(), autoColumns[7].ColumnType())
	} else {
		assert.Len(t, autoColumns, 4)
		assert.Equal(t, colRead.ColumnType(), autoColumns[0].ColumnType())
		assert.Equal(t, colNullableRead.ColumnType(), autoColumns[1].ColumnType())
		assert.Equal(t, colArrayRead.ColumnType(), autoColumns[2].ColumnType())
		assert.Equal(t, colNullableArrayRead.ColumnType(), autoColumns[3].ColumnType())
	}

	for selectStmt.Next() {
	}
	require.NoError(t, selectStmt.Err())
	selectStmt.Close()
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
	assert.Equal(t, colDateRead.Row(0).In(time.UTC).Format(time.RFC3339), "1970-01-01T00:00:00Z")
	assert.Equal(t, colDate32Read.Row(0).In(time.UTC).Format(time.RFC3339), "1900-01-01T00:00:00Z")
	assert.Equal(t, colDateTimeRead.Row(0).In(time.UTC).Format(time.RFC3339), "1970-01-01T00:00:00Z")
	assert.Equal(t, colDateTime64Read.Row(0).In(time.UTC).Format(time.RFC3339), "1900-01-01T00:00:00Z")

	require.NoError(t, selectStmt.Err())
}
