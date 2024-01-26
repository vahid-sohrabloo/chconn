package column_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
)

func TestNothing(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	conn, err := chconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT NULL")
	require.NoError(t, err)
	require.True(t, rows.Next())

	var data int
	err = rows.Scan(&data)
	require.NoError(t, err)
	require.Equal(t, 0, data)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())

	rows, err = conn.Query(ctx, "SELECT array()")
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	var dataArr []int
	err = rows.Scan(&dataArr)
	require.NoError(t, err)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.Len(t, dataArr, 0)

	rows, err = conn.Query(ctx, "SELECT array(NULL)")
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	err = rows.Scan(&dataArr)
	require.NoError(t, err)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	assert.Equal(t, dataArr, []int{0})

	nothingCol := column.NewNothing()
	var dataCol int
	err = nothingCol.Scan(0, &data)
	assert.NoError(t, err)
	assert.Equal(t, 0, dataCol)
	refValue := reflect.ValueOf(&data)
	err = nothingCol.Scan(0, refValue)
	assert.NoError(t, err)
	assert.Equal(t, 0, dataCol)
	assert.Equal(t, int64(0), refValue.Elem().Int())
	assert.Equal(t, "Nothing", nothingCol.FullType())
	assert.Equal(t, "", nothingCol.String(0))
	nothingCol.SetName([]byte("test"))
	assert.Equal(t, "test Nothing", nothingCol.FullType())

	nothingNullableCol := column.NewNothing().Nullable()
	stmt, err := conn.Select(ctx, "SELECT NULL from system.numbers limit 2", nothingNullableCol)
	require.NoError(t, err)
	require.True(t, stmt.Next())
	assert.Equal(t, []column.NothingData{{}, {}}, nothingNullableCol.Data())
	assert.Equal(t, []column.NothingData{{}, {}}, nothingNullableCol.Read(nil))
	assert.Equal(t, column.NothingData{}, nothingNullableCol.Row(0))
	assert.Equal(t, (*column.NothingData)(nil), nothingNullableCol.RowP(0))
	assert.Equal(t, []*column.NothingData{(*column.NothingData)(nil), (*column.NothingData)(nil)}, nothingNullableCol.DataP())
	assert.Equal(t, []*column.NothingData{(*column.NothingData)(nil), (*column.NothingData)(nil)}, nothingNullableCol.ReadP(nil))
	assert.Equal(t, (*column.NothingData)(nil), nothingNullableCol.RowP(0))
	assert.Equal(t, (*column.NothingData)(nil), nothingNullableCol.RowAny(0))
	assert.Equal(t, []bool{true, true}, nothingNullableCol.DataNil())
	assert.Equal(t, []bool{true, true}, nothingNullableCol.ReadNil(nil))
	assert.Equal(t, true, nothingNullableCol.RowIsNil(0))
	assert.Equal(t, true, nothingNullableCol.RowIsNil(0))

	err = nothingNullableCol.Scan(0, refValue)
	assert.NoError(t, err)
	assert.Equal(t, 0, dataCol)
	assert.Equal(t, int64(0), refValue.Elem().Int())

}
