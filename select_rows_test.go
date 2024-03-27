package chconn

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testRowScanner struct {
	name string
	age  int32
}

func (rs *testRowScanner) ScanRow(rows Rows) error {
	return rows.Scan(&rs.name, &rs.age)
}

func getConnection(t testing.TB) Conn {
	config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	require.NoError(t, err)
	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	return c
}

func TestRowScanner(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)
	var s testRowScanner
	err := conn.QueryRow(context.Background(), "select 'Adam' as name, toInt32(72) as height").Scan(&s)
	require.NoError(t, err)
	require.Equal(t, "Adam", s.name)
	require.Equal(t, int32(72), s.age)
}

type testErrRowScanner string

func (ers *testErrRowScanner) ScanRow(rows Rows) error {
	return errors.New(string(*ers))
}

func TestRowScannerErrorIsFatalToRows(t *testing.T) {
	t.Parallel()
	conn := getConnection(t)
	s := testErrRowScanner("foo")
	err := conn.QueryRow(context.Background(), "select 'Adam' as name, 72 as height").Scan(&s)
	require.EqualError(t, err, "foo")
}

func TestForEachRow(t *testing.T) {
	t.Parallel()

	conn := getConnection(t)

	var actualResults []any

	rows, _ := conn.Query(
		context.Background(),
		"select number, number * 2 from system.numbers where number > 0 limit {n: UInt32}",
		IntParameter("n", 3),
	)
	var a, b uint64
	err := ForEachRow(rows, []any{&a, &b}, func() error {
		actualResults = append(actualResults, []any{a, b})
		return nil
	})
	require.NoError(t, err)

	expectedResults := []any{
		[]any{uint64(1), uint64(2)},
		[]any{uint64(2), uint64(4)},
		[]any{uint64(3), uint64(6)},
	}
	require.Equal(t, expectedResults, actualResults)
}

func TestForEachRowScanError(t *testing.T) {
	t.Parallel()

	var actualResults []any
	conn := getConnection(t)
	rows, _ := conn.Query(
		context.Background(),
		"select 'foo', 'bar' from system.numbers where number > 0 limit {n: UInt32}",
		IntParameter("n", 3),
	)
	var a, b uint64
	err := ForEachRow(rows, []any{&a, &b}, func() error {
		actualResults = append(actualResults, []any{a, b})
		return nil
	})
	require.EqualError(t, err, "can't scan into dest[0]: cannot scan type '*string' into dest type '*uint64'")
}

func TestForEachRowAbort(t *testing.T) {
	t.Parallel()

	conn := getConnection(t)
	rows, _ := conn.Query(
		context.Background(),
		"select number, number * 2 from system.numbers where number > 0 limit {n: UInt32}",
		IntParameter("n", 3),
	)
	var a, b uint64
	err := ForEachRow(rows, []any{&a, &b}, func() error {
		return errors.New("abort")
	})
	require.EqualError(t, err, "abort")
}

func ExampleForEachRow() {
	conn, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(
		context.Background(),
		"select number, number * 2 from system.numbers where number > 0 limit {n: UInt32}",
		IntParameter("n", 3),
	)
	var a, b uint64
	err = ForEachRow(rows, []any{&a, &b}, func() error {
		fmt.Printf("%v, %v\n", a, b)
		return nil
	})
	if err != nil {
		fmt.Printf("ForEachRow error: %v", err)
		return
	}

	// Output:
	// 1, 2
	// 2, 4
	// 3, 6
}

func TestCollectRows(t *testing.T) {
	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select toInt32(number) from system.numbers limit 100`)
	numbers, err := CollectRows(rows, func(row CollectableRow) (int32, error) {
		var n int32
		err := row.Scan(&n)
		return n, err
	})
	require.NoError(t, err)

	assert.Len(t, numbers, 100)
	for i := range numbers {
		assert.Equal(t, int32(i), numbers[i])
	}
}

// This example uses CollectRows with a manually written collector function. In most cases RowTo, RowToAddrOf,
// RowToStructByPos, RowToAddrOfStructByPos, or another generic function would be used.
func ExampleCollectRows() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(context.Background(), `select toInt32(number) from system.numbers where number > 0 limit 5`)
	numbers, err := CollectRows(rows, func(row CollectableRow) (int32, error) {
		var n int32
		err := row.Scan(&n)
		return n, err
	})
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	fmt.Println(numbers)

	// Output:
	// [1 2 3 4 5]
}

func TestCollectOneRow(t *testing.T) {
	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select toInt32(42)`)
	n, err := CollectOneRow(rows, func(row CollectableRow) (int32, error) {
		var n int32
		err := row.Scan(&n)
		return n, err
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(42), n)
}

func TestCollectOneRowNotFound(t *testing.T) {
	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select toInt32(42) where false`)
	n, err := CollectOneRow(rows, func(row CollectableRow) (int32, error) {
		var n int32
		err := row.Scan(&n)
		return n, err
	})
	assert.ErrorIs(t, err, ErrNoRows)
	assert.Equal(t, int32(0), n)
}

func TestCollectOneRowIgnoresExtraRows(t *testing.T) {
	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select toInt32(number) from system.numbers where number >= 42 limit 100`)
	n, err := CollectOneRow(rows, func(row CollectableRow) (int32, error) {
		var n int32
		err := row.Scan(&n)
		return n, err
	})
	require.NoError(t, err)

	assert.NoError(t, err)
	assert.Equal(t, int32(42), n)
}

func TestCollectExactlyOneRow(t *testing.T) {
	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select toInt32(42)`)
	n, err := CollectExactlyOneRow(rows, func(row CollectableRow) (int32, error) {
		var n int32
		err := row.Scan(&n)
		return n, err
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(42), n)
}

func TestCollectExactlyOneRowNotFound(t *testing.T) {
	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select toInt32(42) where false`)
	n, err := CollectExactlyOneRow(rows, func(row CollectableRow) (int32, error) {
		var n int32
		err := row.Scan(&n)
		return n, err
	})
	assert.ErrorIs(t, err, ErrNoRows)
	assert.Equal(t, int32(0), n)
}

func TestCollectExactlyOneRowExtraRows(t *testing.T) {
	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select toInt32(number) from system.numbers where number > 41 limit 100`)
	n, err := CollectExactlyOneRow(rows, func(row CollectableRow) (int32, error) {
		var n int32
		err := row.Scan(&n)
		return n, err
	})
	assert.ErrorIs(t, err, ErrTooManyRows)
	assert.Equal(t, int32(0), n)
}

func TestRowTo(t *testing.T) {
	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select toInt32(number) from system.numbers limit 100`)
	numbers, err := CollectRows(rows, RowTo[int32])
	require.NoError(t, err)

	assert.Len(t, numbers, 100)
	for i := range numbers {
		assert.Equal(t, int32(i), numbers[i])
	}
}

func ExampleRowTo() {
	conn, err := Connect(context.Background(), os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(context.Background(), `select toInt32(number) from system.numbers where number > 0 limit 5`)
	numbers, err := CollectRows(rows, RowTo[int32])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	fmt.Println(numbers)

	// Output:
	// [1 2 3 4 5]
}

func TestRowToAddrOf(t *testing.T) {
	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select toInt32(number) from system.numbers limit 100`)
	numbers, err := CollectRows(rows, RowToAddrOf[int32])
	require.NoError(t, err)

	assert.Len(t, numbers, 100)
	for i := range numbers {
		assert.Equal(t, int32(i), *numbers[i])
	}
}

func ExampleRowToAddrOf() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	rows, _ := conn.Query(context.Background(), `select toInt32(number) from system.numbers where number > 0 limit 5`)
	pNumbers, err := CollectRows(rows, RowToAddrOf[int32])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range pNumbers {
		fmt.Println(*p)
	}

	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
}

func TestRowToMap(t *testing.T) {
	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select 'Joe' as name, toInt32(number) as age from system.numbers limit 10`)
	slice, err := CollectRows(rows, RowToMap)
	require.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "Joe", slice[i]["name"])
		assert.EqualValues(t, i, slice[i]["age"])
	}
}

func TestRowToStructByPos(t *testing.T) {
	type person struct {
		Name string
		Age  int32
	}

	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select 'Joe' as name, toInt32(number) as age from system.numbers limit 10`)
	slice, err := CollectRows(rows, RowToStructByPos[person])
	require.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "Joe", slice[i].Name)
		assert.EqualValues(t, i, slice[i].Age)
	}
}

func TestRowToStructByPosIgnoredField(t *testing.T) {
	type person struct {
		Name string
		Age  int32 `db:"-"`
	}

	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(), `select 'Joe' as name from system.numbers limit 10`)
	slice, err := CollectRows(rows, RowToStructByPos[person])
	require.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "Joe", slice[i].Name)
	}
}

func TestRowToStructByPosEmbeddedStruct(t *testing.T) {
	type Name struct {
		First string
		Last  string
	}

	type person struct {
		Name
		Age int32
	}

	conn := getConnection(t)
	rows, _ := conn.Query(
		context.Background(),
		`select 'John' as first_name, 'Smith' as last_name, toInt32(number) as age from system.numbers  limit 10`,
	)
	slice, err := CollectRows(rows, RowToStructByPos[person])
	require.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "John", slice[i].Name.First)
		assert.Equal(t, "Smith", slice[i].Name.Last)
		assert.EqualValues(t, i, slice[i].Age)
	}
}

func TestRowToStructByPosMultipleEmbeddedStruct(t *testing.T) {
	type Sandwich struct {
		Bread string
		Salad string
	}
	type Drink struct {
		Ml int32
	}

	type meal struct {
		Sandwich
		Drink
	}

	conn := getConnection(t)
	rows, _ := conn.Query(
		context.Background(),
		`select 'Baguette' as bread, 'Lettuce' as salad, toInt32(number) as drink_ml from system.numbers  limit 10`,
	)
	slice, err := CollectRows(rows, RowToStructByPos[meal])
	require.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "Baguette", slice[i].Sandwich.Bread)
		assert.Equal(t, "Lettuce", slice[i].Sandwich.Salad)
		assert.EqualValues(t, i, slice[i].Drink.Ml)
	}
}

func TestRowToStructByPosEmbeddedUnexportedStruct(t *testing.T) {
	type name struct {
		First string
		Last  string
	}

	type person struct {
		name
		Age int32
	}

	conn := getConnection(t)
	rows, _ := conn.Query(
		context.Background(),
		`select 'John' as first_name, 'Smith' as last_name, toInt32(number) as age from system.numbers  limit 10`,
	)
	slice, err := CollectRows(rows, RowToStructByPos[person])
	require.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "John", slice[i].name.First)
		assert.Equal(t, "Smith", slice[i].name.Last)
		assert.EqualValues(t, i, slice[i].Age)
	}
}

// Pointer to struct is not supported. But check that we don't panic.
func TestRowToStructByPosEmbeddedPointerToStruct(t *testing.T) {
	type Name struct {
		First string
		Last  string
	}

	type person struct {
		*Name
		Age int32
	}

	conn := getConnection(t)
	rows, _ := conn.Query(
		context.Background(),
		`select 'John' as first_name, 'Smith' as last_name, toInt32(number) as age from system.numbers  limit 10`,
	)
	_, err := CollectRows(rows, RowToStructByPos[person])
	require.EqualError(t, err, "got 3 values, but dst struct has only 2 fields")
}

func ExampleRowToStructByPos() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	// Setup example schema and data.
	err = conn.Exec(ctx, `
create temporary table products (
	id Int32,
	name String,
	price Int32
);`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	err = conn.Insert(ctx, `
insert into products (id, name, price) values
	(1, 'Cheeseburger', 10),
	(2, 'Double Cheeseburger', 14),
	(3, 'Fries', 5),
	(4, 'Soft Drink', 3);
`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	type product struct {
		ID    int32
		Name  string
		Price int32
	}

	rows, _ := conn.Query(
		context.Background(),
		"select * from products where price < {price: Int32} order by price desc",
		IntParameter("price", 12),
	)
	products, err := CollectRows(rows, RowToStructByPos[product])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range products {
		fmt.Printf("%s: $%d\n", p.Name, p.Price)
	}

	// Output:
	// Cheeseburger: $10
	// Fries: $5
	// Soft Drink: $3
}

func TestRowToAddrOfStructPos(t *testing.T) {
	type person struct {
		Name string
		Age  int32
	}

	conn := getConnection(t)
	rows, _ := conn.Query(
		context.Background(),
		`select 'Joe' as name, toInt32(number) as age from system.numbers  limit 10`,
	)
	slice, err := CollectRows(rows, RowToAddrOfStructByPos[person])
	require.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "Joe", slice[i].Name)
		assert.EqualValues(t, i, slice[i].Age)
	}
}

func TestRowToStructByName(t *testing.T) {
	type person struct {
		Last  string
		First string
		Age   int32
	}

	conn := getConnection(t)
	rows, _ := conn.Query(
		context.Background(),
		`select 'John' as first, 'Smith' as last, toInt32(number) as age from system.numbers  limit 10`,
	)
	slice, err := CollectRows(rows, RowToStructByName[person])
	assert.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "Smith", slice[i].Last)
		assert.Equal(t, "John", slice[i].First)
		assert.EqualValues(t, i, slice[i].Age)
	}

	// check missing fields in a returned row
	rows, _ = conn.Query(
		context.Background(),
		`select 'Smith' as last, toInt32(number) as age from system.numbers  limit 10`,
	)
	_, err = CollectRows(rows, RowToStructByName[person])
	assert.ErrorContains(t, err, "cannot find field First in returned row")
	require.True(t, conn.IsClosed())

	conn = getConnection(t)
	// check missing field in a destination struct
	rows, _ = conn.Query(
		context.Background(),
		`select 'John' as first, 'Smith' as last, toInt32(number) as age, null as ignore from system.numbers  limit 10`,
	)
	_, err = CollectRows(rows, RowToAddrOfStructByName[person])
	assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
}

func TestRowToStructByNameEmbeddedStruct(t *testing.T) {
	type Name struct {
		Last  string `db:"last_name"`
		First string `db:"first_name"`
	}

	type person struct {
		Ignore bool `db:"-"`
		Name
		Age int32
	}

	conn := getConnection(t)
	rows, _ := conn.Query(
		context.Background(),
		`select 'John' as first_name, 'Smith' as last_name, toInt32(number) as age from system.numbers  limit 10`,
	)
	slice, err := CollectRows(rows, RowToStructByName[person])
	assert.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "Smith", slice[i].Name.Last)
		assert.Equal(t, "John", slice[i].Name.First)
		assert.EqualValues(t, i, slice[i].Age)
	}

	// check missing fields in a returned row
	rows, _ = conn.Query(context.Background(),
		`select 'Smith' as last_name, toInt32(number) as age from system.numbers  limit 10`,
	)
	_, err = CollectRows(rows, RowToStructByName[person])
	assert.ErrorContains(t, err, "cannot find field first_name in returned row")

	conn = getConnection(t)
	// check missing field in a destination struct
	rows, _ = conn.Query(context.Background(),
		`select 'John' as first_name, 'Smith' as last_name, toInt32(number) as age, null as ignore from system.numbers  limit 10`,
	)
	_, err = CollectRows(rows, RowToAddrOfStructByName[person])
	assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
}

func ExampleRowToStructByName() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	// Setup example schema and data.
	err = conn.Exec(ctx, `
create temporary table products (
	id Int32,
	name String,
	price Int32
);
`)

	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	err = conn.Insert(ctx, `insert into products (id, name, price) values
	(1, 'Cheeseburger', 10),
	(2, 'Double Cheeseburger', 14),
	(3, 'Fries', 5),
	(4, 'Soft Drink', 3);
`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	type product struct {
		ID    int32
		Name  string
		Price int32
	}

	rows, _ := conn.Query(ctx,
		"select * from products where price < {price: Int32} order by price desc",
		IntParameter("price", 12),
	)
	products, err := CollectRows(rows, RowToStructByName[product])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range products {
		fmt.Printf("%s: $%d\n", p.Name, p.Price)
	}

	// Output:
	// Cheeseburger: $10
	// Fries: $5
	// Soft Drink: $3
}

func TestRowToStructByNameLax(t *testing.T) {
	type person struct {
		Last   string
		First  string
		Age    int32
		Ignore bool `db:"-"`
	}

	conn := getConnection(t)
	rows, _ := conn.Query(context.Background(),
		`select 'John' as first, 'Smith' as last, toInt32(number) as age from system.numbers  limit 10`,
	)
	slice, err := CollectRows(rows, RowToStructByNameLax[person])
	assert.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "Smith", slice[i].Last)
		assert.Equal(t, "John", slice[i].First)
		assert.EqualValues(t, i, slice[i].Age)
	}

	// check missing fields in a returned row
	rows, _ = conn.Query(context.Background(),
		`select 'John' as first, toInt32(number) as age from system.numbers  limit 10`,
	)
	slice, err = CollectRows(rows, RowToStructByNameLax[person])
	assert.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "John", slice[i].First)
		assert.EqualValues(t, i, slice[i].Age)
	}

	// check extra fields in a returned row
	rows, _ = conn.Query(context.Background(),
		`select 'John' as first, 'Smith' as last, toInt32(number) as age, null as ignore from system.numbers  limit 10`,
	)
	_, err = CollectRows(rows, RowToAddrOfStructByNameLax[person])
	assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")

	conn = getConnection(t)
	// check missing fields in a destination struct
	rows, _ = conn.Query(context.Background(),
		`select 'Smith' as last, 'D.' as middle, toInt32(number) as age from system.numbers  limit 10`,
	)
	_, err = CollectRows(rows, RowToAddrOfStructByNameLax[person])
	assert.ErrorContains(t, err, "struct doesn't have corresponding row field middle")

	conn = getConnection(t)
	// check ignored fields in a destination struct
	rows, _ = conn.Query(context.Background(),
		`select 'Smith' as last, toInt32(number) as age, null as ignore from system.numbers  limit 10`,
	)
	_, err = CollectRows(rows, RowToAddrOfStructByNameLax[person])
	assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
}

func TestRowToStructByNameLaxEmbeddedStruct(t *testing.T) {
	type Name struct {
		Last  string `db:"last_name"`
		First string `db:"first_name"`
	}

	type person struct {
		Ignore bool `db:"-"`
		Name
		Age int32
	}

	conn := getConnection(t)
	rows, _ := conn.Query(
		context.Background(),
		`select 'John' as first_name, 'Smith' as last_name, toInt32(number) as age from system.numbers  limit 10`,
	)
	slice, err := CollectRows(rows, RowToStructByNameLax[person])
	assert.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "Smith", slice[i].Name.Last)
		assert.Equal(t, "John", slice[i].Name.First)
		assert.EqualValues(t, i, slice[i].Age)
	}

	// check missing fields in a returned row
	rows, _ = conn.Query(
		context.Background(),
		`select 'John' as first_name, toInt32(number) as age from system.numbers  limit 10`,
	)
	slice, err = CollectRows(rows, RowToStructByNameLax[person])
	assert.NoError(t, err)

	assert.Len(t, slice, 10)
	for i := range slice {
		assert.Equal(t, "John", slice[i].Name.First)
		assert.EqualValues(t, i, slice[i].Age)
	}

	// check extra fields in a returned row
	rows, _ = conn.Query(
		context.Background(),
		`select 'John' as first_name, 'Smith' as last_name, toInt32(number) as age, null as ignore from system.numbers  limit 10`,
	)
	_, err = CollectRows(rows, RowToAddrOfStructByNameLax[person])
	assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
	conn = getConnection(t)
	// check missing fields in a destination struct
	rows, _ = conn.Query(
		context.Background(),
		`select 'Smith' as last_name, 'D.' as middle_name, toInt32(number) as age from system.numbers  limit 10`,
	)
	_, err = CollectRows(rows, RowToAddrOfStructByNameLax[person])
	assert.ErrorContains(t, err, "struct doesn't have corresponding row field middle_name")

	conn = getConnection(t)

	// check ignored fields in a destination struct
	rows, _ = conn.Query(
		context.Background(),
		`select 'Smith' as last_name, toInt32(number) as age, null as ignore from system.numbers  limit 10`)
	_, err = CollectRows(rows, RowToAddrOfStructByNameLax[person])
	assert.ErrorContains(t, err, "struct doesn't have corresponding row field ignore")
}

func ExampleRowToStructByNameLax() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := Connect(ctx, os.Getenv("CHX_TEST_TCP_CONN_STRING"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	// Setup example schema and data.
	err = conn.Exec(ctx, `
create temporary table products (
	id Int32,
	name String,
	price Int32
);`)

	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}
	err = conn.Insert(ctx, `insert into products (id, name, price) values
	(1, 'Cheeseburger', 10),
	(2, 'Double Cheeseburger', 14),
	(3, 'Fries', 5),
	(4, 'Soft Drink', 3);
`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	type product struct {
		ID    int32
		Name  string
		Type  string
		Price int32
	}

	rows, _ := conn.Query(context.Background(),
		"select * from products where price < {price: Int32} order by price desc",
		IntParameter("price", 12))
	products, err := CollectRows(rows, RowToStructByNameLax[product])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, p := range products {
		fmt.Printf("%s: $%d\n", p.Name, p.Price)
	}

	// Output:
	// Cheeseburger: $10
	// Fries: $5
	// Soft Drink: $3
}
