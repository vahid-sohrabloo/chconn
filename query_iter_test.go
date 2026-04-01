package chconn

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryIterScalar(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	var numbers []int32
	for n, err := range QueryIter[int32](context.Background(), conn, `select toInt32(number) from system.numbers limit 5`) {
		require.NoError(t, err)
		numbers = append(numbers, n)
	}
	assert.Equal(t, []int32{0, 1, 2, 3, 4}, numbers)
}

func TestQueryIterStruct(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	type person struct {
		Name string
		Age  int32
	}

	var people []person
	for p, err := range QueryIter[person](context.Background(), conn,
		`select 'Joe' as name, toInt32(number) as age from system.numbers limit 3`) {
		require.NoError(t, err)
		people = append(people, p)
	}
	require.Len(t, people, 3)
	assert.Equal(t, "Joe", people[0].Name)
	assert.Equal(t, int32(0), people[0].Age)
	assert.Equal(t, int32(2), people[2].Age)
}

func TestQueryIterBreak(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	var count int
	for _, err := range QueryIter[int32](context.Background(), conn,
		`select toInt32(number) from system.numbers limit 1000`) {
		require.NoError(t, err)
		count++
		if count >= 3 {
			break
		}
	}
	assert.Equal(t, 3, count)
}

func TestQueryIterWith(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	type person struct {
		Name string
		Age  int32
	}

	var people []person
	for p, err := range QueryIterWith(context.Background(), conn, RowToByPos[person],
		`select 'Joe' as name, toInt32(number) as age from system.numbers limit 3`) {
		require.NoError(t, err)
		people = append(people, p)
	}
	require.Len(t, people, 3)
	assert.Equal(t, "Joe", people[0].Name)
}

func TestQueryAll(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	numbers, err := QueryAll[int32](context.Background(), conn,
		`select toInt32(number) from system.numbers limit 100`)
	require.NoError(t, err)
	assert.Len(t, numbers, 100)
	for i := range numbers {
		assert.Equal(t, int32(i), numbers[i])
	}
}

func TestQueryAllStruct(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	type person struct {
		Name string
		Age  int32
	}

	people, err := QueryAll[person](context.Background(), conn,
		`select 'Joe' as name, toInt32(number) as age from system.numbers limit 3`)
	require.NoError(t, err)
	require.Len(t, people, 3)
	assert.Equal(t, "Joe", people[0].Name)
	assert.Equal(t, int32(0), people[0].Age)
}

func TestQueryAllWithParams(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	numbers, err := QueryAll[int32](context.Background(), conn,
		`select toInt32(number) from system.numbers where number > 0 limit {n: UInt32}`,
		IntParameter("n", 3))
	require.NoError(t, err)
	assert.Equal(t, []int32{1, 2, 3}, numbers)
}

func TestQueryOne(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	n, err := QueryOne[int32](context.Background(), conn, `select toInt32(42)`)
	require.NoError(t, err)
	assert.Equal(t, int32(42), n)
}

func TestQueryOneNotFound(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	_, err := QueryOne[int32](context.Background(), conn, `select toInt32(42) where false`)
	assert.ErrorIs(t, err, ErrNoRows)
}

func TestQueryExactlyOne(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	n, err := QueryExactlyOne[int32](context.Background(), conn, `select toInt32(42)`)
	require.NoError(t, err)
	assert.Equal(t, int32(42), n)
}

func TestQueryExactlyOneNotFound(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	_, err := QueryExactlyOne[int32](context.Background(), conn, `select toInt32(42) where false`)
	assert.ErrorIs(t, err, ErrNoRows)
}

func TestQueryExactlyOneTooMany(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)

	_, err := QueryExactlyOne[int32](context.Background(), conn,
		`select toInt32(number) from system.numbers limit 5`)
	assert.ErrorIs(t, err, ErrTooManyRows)
}
