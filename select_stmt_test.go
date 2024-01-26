package chconn

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/types"
)

func TestExecReturnBlock(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	err = c.Exec(context.Background(), "SELECT 1")
	require.NoError(t, err)
}

func TestSelectError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	c.(*conn).status = connStatusUninitialized
	res, err := c.Select(context.Background(), "select * from system.numbers limit 5")
	require.NotNil(t, res)
	require.EqualError(t, err, "conn uninitialized")
	require.EqualError(t, c.(*conn).lock(), "conn uninitialized")
	c.Close()

	config.WriterFunc = func(w io.Writer) io.Writer {
		return &writerErrorHelper{
			err:         errors.New("timeout"),
			w:           w,
			numberValid: 1,
		}
	}
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	res, err = c.Select(context.Background(), "select * from system.numbers limit 5")
	require.EqualError(t, err, "write block info (timeout)")
	require.NotNil(t, res)
	assert.True(t, c.IsClosed())

	config.WriterFunc = nil
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	colNumber := column.New[int64]()
	res, err = c.Select(context.Background(), "select number,toNullable(number) from system.numbers limit 5", colNumber)
	require.NoError(t, err)
	for res.Next() {
	}
	assert.False(t, res.Next())
	require.EqualError(t, res.Err(), "read 1 column(s) but 2 column(s) available")
	assert.True(t, c.IsClosed())
}

func TestSelectCtxError(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := c.Select(ctx, "select * from system.numbers limit 1")
	require.EqualError(t, err, "timeout: context already done: context canceled")
	require.NotNil(t, res)
	assert.False(t, c.IsClosed())

	config.WriterFunc = func(w io.Writer) io.Writer {
		return &writerSlowHelper{
			w:     w,
			sleep: time.Second,
		}
	}
	c, err = ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	res, err = c.Select(ctx, "select * from system.numbers")
	require.EqualError(t, errors.Unwrap(err), "context deadline exceeded")
	require.NotNil(t, res)
	assert.True(t, c.IsClosed())
}

func TestSelectProgress(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	colSleep := column.New[uint8]()
	colNumber := column.New[uint64]()
	res, err := c.SelectWithOption(context.Background(),
		"SELECT sleep(1), * FROM system.numbers LIMIT 1",
		&QueryOptions{
			OnProgress: func(p *Progress) {
			},
			OnProfile: func(p *Profile) {
			},
			OnProfileEvent: func(p *ProfileEvent) {

			},
		},
		colSleep,
		colNumber,
	)
	require.NotNil(t, res)
	require.NoError(t, err)

	for res.Next() {
	}
	require.NoError(t, res.Err())

	c.Close()
}

func TestSelectParameters(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("CHX_TEST_TCP_CONN_STRING")

	config, err := ParseConfig(connString)
	require.NoError(t, err)

	c, err := ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	colA := column.New[int32]()
	colAS := column.New[int32]().Array()
	colB := column.NewString()
	colBS := column.NewString().Array()
	colC := column.NewDate[types.DateTime]()
	colD := column.NewMap[string, uint8](column.NewString(), column.New[uint8]())
	colE := column.New[uint32]()
	colES := column.New[uint32]().Array()
	colF32 := column.New[float32]()
	colF32S := column.New[float32]().Array()
	colF64 := column.New[float64]()
	colF64S := column.New[float64]().Array()

	res, err := c.SelectWithOption(context.Background(),
		`SELECT {a: Int32},
				{as: Array(Int32)},
				{b: String},
				{bs: Array(String)},
				{c: DateTime},
				{d: Map(String, UInt8)},
				{e: UInt32},
				{es: Array(UInt32)},
				{f32: Float32},
				{f64: Float64},
				{f32s: Array(Float32)},
				{f64s: Array(Float64)}
				`,
		&QueryOptions{
			Parameters: NewParameters(
				IntParameter("a", 13),
				IntSliceParameter("as", []int32{-15, -16}),
				StringParameter("b", "str'"),
				StringSliceParameter("bs", []string{"str", "str2\\'"}),
				StringParameter("c", "2022-08-04 18:30:53"),
				StringParameter("d", `{'a': 1, 'b': 2}`),
				UintParameter("e", uint64(14)),
				UintSliceParameter("es", []uint32{15, 16}),
				Float32Parameter("f32", float32(1.5)),
				Float64Parameter("f64", float64(1.8)),
				Float32SliceParameter("f32s", []float32{1.5, 1.6}),
				Float64SliceParameter("f64s", []float64{1.8, 1.9}),
			),
		},
		colA,
		colAS,
		colB,
		colBS,
		colC,
		colD,
		colE,
		colES,
		colF32,
		colF64,
		colF32S,
		colF64S,
	)

	if err != nil && err.Error() == "parameters are not supported by the server" {
		t.SkipNow()
	}
	require.NoError(t, err)
	require.NotNil(t, res)

	for res.Next() {
	}
	require.NoError(t, res.Err())
	require.Len(t, colA.Data(), 1)
	require.Len(t, colAS.Data(), 1)
	require.Len(t, colB.Data(), 1)
	require.Len(t, colBS.Data(), 1)
	require.Len(t, colC.Data(), 1)
	require.Len(t, colD.Data(), 1)
	require.Len(t, colE.Data(), 1)
	require.Len(t, colES.Data(), 1)
	assert.Equal(t, int32(13), colA.Data()[0])
	assert.Equal(t, []int32{-15, -16}, colAS.Data()[0])
	assert.Equal(t, "str'", colB.Data()[0])
	assert.Equal(t, []string{"str", "str2\\'"}, colBS.Data()[0])
	assert.Equal(t, "2022-08-04 18:30:53", colC.Data()[0].Format("2006-01-02 15:04:05"))
	assert.Equal(t, map[string]uint8{
		"a": 1,
		"b": 2,
	}, colD.Data()[0])
	assert.Equal(t, uint32(14), colE.Data()[0])
	assert.Equal(t, []uint32{15, 16}, colES.Data()[0])
	assert.Equal(t, float32(1.5), colF32.Data()[0])
	assert.Equal(t, float64(1.8), colF64.Data()[0])
	assert.Equal(t, []float32{1.5, 1.6}, colF32S.Data()[0])
	assert.Equal(t, []float64{1.8, 1.9}, colF64S.Data()[0])

	c.Close()
}

func TestSelectProgressError(t *testing.T) {
	startValidReader := 35

	tests := []struct {
		name        string
		wantErr     string
		numberValid func(c Conn) int
		minRevision uint64
	}{
		{
			name:        "read ReadRows",
			wantErr:     "progress: read ReadRows (timeout)",
			numberValid: func(c Conn) int { return startValidReader },
		},
		{
			name:        "read ReadBytes",
			wantErr:     "progress: read ReadBytes (timeout)",
			numberValid: func(c Conn) int { return startValidReader + 1 },
		},
		{
			name:        "read TotalRows ",
			wantErr:     "progress: read TotalRows (timeout)",
			numberValid: func(c Conn) int { return startValidReader + 2 },
		},
		{
			name:        "read TotalBytes",
			wantErr:     "progress: read TotalBytes (timeout)",
			numberValid: func(c Conn) int { return startValidReader + 3 },
			minRevision: helper.DbmsMinProtocolVersionWithTotalBytesInProgress,
		},
		{
			name:    "read WriterRows",
			wantErr: "progress: read WriterRows (timeout)",
			numberValid: func(c Conn) int {
				moreIncrement := 0
				if c.ServerInfo().Revision >= helper.DbmsMinProtocolVersionWithTotalBytesInProgress {
					moreIncrement++
				}
				return startValidReader + 3 + moreIncrement
			},
		},
		{
			name:    "read WrittenBytes",
			wantErr: "progress: read WrittenBytes (timeout)",
			numberValid: func(c Conn) int {
				moreIncrement := 0
				if c.ServerInfo().Revision >= helper.DbmsMinProtocolVersionWithTotalBytesInProgress {
					moreIncrement++
				}
				return startValidReader + 4 + moreIncrement
			},
		},
		{
			name:    "read ElapsedNS",
			wantErr: "progress: read ElapsedNS (timeout)",
			numberValid: func(c Conn) int {
				moreIncrement := 0
				if c.ServerInfo().Revision >= helper.DbmsMinProtocolVersionWithTotalBytesInProgress {
					moreIncrement++
				}
				return startValidReader + 5 + moreIncrement
			},
			minRevision: helper.DbmsMinProtocolWithServerQueryTimeInProgress,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader, c Conn) io.Reader {
				return &readErrorHelper{
					err:             errors.New("timeout"),
					r:               r,
					c:               c,
					numberValidFunc: tt.numberValid,
				}
			}
			c, err := ConnectConfig(context.Background(), config)
			require.NoError(t, err)

			if c.ServerInfo().Revision < tt.minRevision {
				c.Close()
				return
			}
			colSleep := column.New[uint8]()
			colNumber := column.New[uint64]()
			res, err := c.SelectWithOption(context.Background(),
				"SELECT sleep(1), * FROM system.numbers LIMIT 1",
				&QueryOptions{
					OnProgress: func(p *Progress) {
					},
				},
				colSleep,
				colNumber,
			)
			require.NoError(t, err)
			require.NotNil(t, res)

			for res.Next() {
			}

			assert.EqualError(t, res.Err(), tt.wantErr)
		})
	}
}

func TestGetFixedstructType(t *testing.T) {
	tests := []struct {
		name string
		len  int
		col  column.ColumnBasic
	}{
		{
			name: "fixed 1",
			len:  1,
			col:  column.New[[1]byte](),
		},
		{
			name: "fixed 2",
			len:  2,
			col:  column.New[[2]byte](),
		},
		{
			name: "fixed 3",
			len:  3,
			col:  column.New[[3]byte](),
		},
		{
			name: "fixed 4",
			len:  4,
			col:  column.New[[4]byte](),
		},
		{
			name: "fixed 5",
			len:  5,
			col:  column.New[[5]byte](),
		},
		{
			name: "fixed 6",
			len:  6,
			col:  column.New[[6]byte](),
		},
		{
			name: "fixed 7",
			len:  7,
			col:  column.New[[7]byte](),
		},
		{
			name: "fixed 8",
			len:  8,
			col:  column.New[[8]byte](),
		},
		{
			name: "fixed 9",
			len:  9,
			col:  column.New[[9]byte](),
		},
		{
			name: "fixed 10",
			len:  10,
			col:  column.New[[10]byte](),
		},
		{
			name: "fixed 11",
			len:  11,
			col:  column.New[[11]byte](),
		},
		{
			name: "fixed 12",
			len:  12,
			col:  column.New[[12]byte](),
		},
		{
			name: "fixed 13",
			len:  13,
			col:  column.New[[13]byte](),
		},
		{
			name: "fixed 14",
			len:  14,
			col:  column.New[[14]byte](),
		},
		{
			name: "fixed 15",
			len:  15,
			col:  column.New[[15]byte](),
		},
		{
			name: "fixed 16",
			len:  16,
			col:  column.New[[16]byte](),
		},
		{
			name: "fixed 17",
			len:  17,
			col:  column.New[[17]byte](),
		},
		{
			name: "fixed 18",
			len:  18,
			col:  column.New[[18]byte](),
		},
		{
			name: "fixed 19",
			len:  19,
			col:  column.New[[19]byte](),
		},
		{
			name: "fixed 20",
			len:  20,
			col:  column.New[[20]byte](),
		},
		{
			name: "fixed 21",
			len:  21,
			col:  column.New[[21]byte](),
		},
		{
			name: "fixed 22",
			len:  22,
			col:  column.New[[22]byte](),
		},
		{
			name: "fixed 23",
			len:  23,
			col:  column.New[[23]byte](),
		},
		{
			name: "fixed 24",
			len:  24,
			col:  column.New[[24]byte](),
		},
		{
			name: "fixed 25",
			len:  25,
			col:  column.New[[25]byte](),
		},
		{
			name: "fixed 26",
			len:  26,
			col:  column.New[[26]byte](),
		},
		{
			name: "fixed 27",
			len:  27,
			col:  column.New[[27]byte](),
		},
		{
			name: "fixed 28",
			len:  28,
			col:  column.New[[28]byte](),
		},
		{
			name: "fixed 29",
			len:  29,
			col:  column.New[[29]byte](),
		},
		{
			name: "fixed 30",
			len:  30,
			col:  column.New[[30]byte](),
		},
		{
			name: "fixed 31",
			len:  31,
			col:  column.New[[31]byte](),
		},
		{
			name: "fixed 32",
			len:  32,
			col:  column.New[[32]byte](),
		},
		{
			name: "fixed 33",
			len:  33,
			col:  column.New[[33]byte](),
		},
		{
			name: "fixed 34",
			len:  34,
			col:  column.New[[34]byte](),
		},
		{
			name: "fixed 35",
			len:  35,
			col:  column.New[[35]byte](),
		},
		{
			name: "fixed 36",
			len:  36,
			col:  column.New[[36]byte](),
		},
		{
			name: "fixed 37",
			len:  37,
			col:  column.New[[37]byte](),
		},
		{
			name: "fixed 38",
			len:  38,
			col:  column.New[[38]byte](),
		},
		{
			name: "fixed 39",
			len:  39,
			col:  column.New[[39]byte](),
		},
		{
			name: "fixed 40",
			len:  40,
			col:  column.New[[40]byte](),
		},
		{
			name: "fixed 41",
			len:  41,
			col:  column.New[[41]byte](),
		},
		{
			name: "fixed 42",
			len:  42,
			col:  column.New[[42]byte](),
		},
		{
			name: "fixed 43",
			len:  43,
			col:  column.New[[43]byte](),
		},
		{
			name: "fixed 44",
			len:  44,
			col:  column.New[[44]byte](),
		},
		{
			name: "fixed 45",
			len:  45,
			col:  column.New[[45]byte](),
		},
		{
			name: "fixed 46",
			len:  46,
			col:  column.New[[46]byte](),
		},
		{
			name: "fixed 47",
			len:  47,
			col:  column.New[[47]byte](),
		},
		{
			name: "fixed 48",
			len:  48,
			col:  column.New[[48]byte](),
		},
		{
			name: "fixed 49",
			len:  49,
			col:  column.New[[49]byte](),
		},
		{
			name: "fixed 50",
			len:  50,
			col:  column.New[[50]byte](),
		},
		{
			name: "fixed 51",
			len:  51,
			col:  column.New[[51]byte](),
		},
		{
			name: "fixed 52",
			len:  52,
			col:  column.New[[52]byte](),
		},
		{
			name: "fixed 53",
			len:  53,
			col:  column.New[[53]byte](),
		},
		{
			name: "fixed 54",
			len:  54,
			col:  column.New[[54]byte](),
		},
		{
			name: "fixed 55",
			len:  55,
			col:  column.New[[55]byte](),
		},
		{
			name: "fixed 56",
			len:  56,
			col:  column.New[[56]byte](),
		},
		{
			name: "fixed 57",
			len:  57,
			col:  column.New[[57]byte](),
		},
		{
			name: "fixed 58",
			len:  58,
			col:  column.New[[58]byte](),
		},
		{
			name: "fixed 59",
			len:  59,
			col:  column.New[[59]byte](),
		},
		{
			name: "fixed 60",
			len:  60,
			col:  column.New[[60]byte](),
		},
		{
			name: "fixed 61",
			len:  61,
			col:  column.New[[61]byte](),
		},
		{
			name: "fixed 62",
			len:  62,
			col:  column.New[[62]byte](),
		},
		{
			name: "fixed 63",
			len:  63,
			col:  column.New[[63]byte](),
		},
		{
			name: "fixed 64",
			len:  64,
			col:  column.New[[64]byte](),
		},
		{
			name: "fixed 65",
			len:  65,
			col:  column.New[[65]byte](),
		},
		{
			name: "fixed 66",
			len:  66,
			col:  column.New[[66]byte](),
		},
		{
			name: "fixed 67",
			len:  67,
			col:  column.New[[67]byte](),
		},
		{
			name: "fixed 68",
			len:  68,
			col:  column.New[[68]byte](),
		},
		{
			name: "fixed 69",
			len:  69,
			col:  column.New[[69]byte](),
		},
		{
			name: "fixed 70",
			len:  70,
			col:  column.New[[70]byte](),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := getFixedType(tt.len, 0, false, false)
			require.NoError(t, err)
			assert.IsType(t, f, tt.col)
		})
	}
}
