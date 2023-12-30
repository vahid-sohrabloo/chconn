package chconn

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockReadError(t *testing.T) {
	startValidReader := 17

	tests := []struct {
		name        string
		wantErr     string
		numberValid int
	}{
		{
			name:        "blockInfo: temporary table",
			wantErr:     "block: temporary table",
			numberValid: startValidReader - 1,
		}, {
			name:        "blockInfo: read field1",
			wantErr:     "blockInfo: read field1",
			numberValid: startValidReader,
		}, {
			name:        "blockInfo: read isOverflows",
			wantErr:     "blockInfo: read isOverflows",
			numberValid: startValidReader + 1,
		}, {
			name:        "blockInfo: read field2",
			wantErr:     "blockInfo: read field2",
			numberValid: startValidReader + 2,
		}, {
			name:        "blockInfo: read bucketNum",
			wantErr:     "blockInfo: read bucketNum",
			numberValid: startValidReader + 3,
		}, {
			name:        "blockInfo: read num3",
			wantErr:     "blockInfo: read num3",
			numberValid: startValidReader + 4,
		}, {
			name:        "block: read NumColumns",
			wantErr:     "block: read NumColumns",
			numberValid: startValidReader + 5,
		}, {
			name:        "block: read NumRows",
			wantErr:     "block: read NumRows",
			numberValid: startValidReader + 6,
		}, {
			name:        "block: read column name",
			wantErr:     "block: read column name",
			numberValid: startValidReader + 8,
		}, {
			name:        "block: read column type",
			wantErr:     "block: read column type",
			numberValid: startValidReader + 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConfig(os.Getenv("CHX_TEST_TCP_CONN_STRING"))
			require.NoError(t, err)
			config.ReaderFunc = func(r io.Reader, c Conn) io.Reader {
				return &readErrorHelper{
					err:         errors.New("timeout"),
					r:           r,
					numberValid: tt.numberValid,
				}
			}

			c, err := ConnectConfig(context.Background(), config)
			assert.NoError(t, err)
			stmt, err := c.Select(context.Background(), "SELECT * FROM system.numbers LIMIT 5;")
			require.Error(t, err)
			require.NotNil(t, stmt)

			readErr, ok := err.(*readError)
			require.True(t, ok)
			require.Equal(t, readErr.msg, tt.wantErr)
			require.EqualError(t, readErr.Unwrap(), "timeout")
			assert.True(t, c.IsClosed())
		})
	}
}

func TestBlockColumnByTypeError(t *testing.T) {
	tests := []struct {
		name       string
		wantErr    string
		chType     string
		arrayLevel int
	}{
		{
			name:    "FixedString",
			chType:  "FixedString(Invalid)",
			wantErr: "invalid fixed string length: FixedString(Invalid): strconv.Atoi: parsing \"Invalid\": invalid syntax",
		},
		{
			name:    "DateTime64 invalid param",
			chType:  "DateTime64()",
			wantErr: "DateTime64 invalid params: precision is required: DateTime64()",
		},
		{
			name:    "DateTime64 invalid precision",
			chType:  "DateTime64(invalid)",
			wantErr: "DateTime64 invalid precision (DateTime64(invalid)): strconv.Atoi: parsing \"invalid\": invalid syntax",
		},
		{
			name:    "Decimal",
			chType:  "Decimal(80)",
			wantErr: "max precision is 76 but got 80: Decimal(80)",
		},
		{
			name:    "Array max level",
			chType:  "Array(Array(Array(Array(string))))",
			wantErr: "max array level is 3",
		},
		{
			name:    "Array nullable",
			chType:  "Nullable(Array(string))",
			wantErr: "array is not allowed in nullable",
		},
		{
			name:    "Array LowCardinality",
			chType:  "LowCardinality(Array(string))",
			wantErr: "LowCardinality is not allowed in nullable",
		},
		{
			name:    "Tuple",
			chType:  "Tuple(`date f Array(String))",
			wantErr: "tuple invalid types: cannot find closing backtick in date f Array(String)",
		},
		{
			name:    "Map",
			chType:  "Map(`date f Array(String))",
			wantErr: "map invalid types: cannot find closing backtick in date f Array(String)",
		},
		{
			name:    "Map one column",
			chType:  "Map(String)",
			wantErr: "map must have 2 columns",
		},
		{
			name:    "Unknown",
			chType:  "Unknown",
			wantErr: "unknown type: Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBlock(nil)
			c, err := b.columnByType([]byte(tt.chType), 0, false, false)
			assert.Nil(t, c)
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}
