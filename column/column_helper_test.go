package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnByTypeError(t *testing.T) {
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
			wantErr: "array invalid type: array invalid type: array invalid type: max array level is 3",
		},
		{
			name:    "Array nullable",
			chType:  "Nullable(Array(string))",
			wantErr: "nullable invalid type: array is not allowed in nullable",
		},
		{
			name:    "Array LowCardinality",
			chType:  "LowCardinality(Array(string))",
			wantErr: "low cardinality invalid type: LowCardinality is not allowed in nullable",
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
			c, err := ColumnByType([]byte(tt.chType), 0, false, false, "")
			assert.Nil(t, c)
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestGetFixedStringType(t *testing.T) {
	tests := []struct {
		name string
		len  int
		col  ColumnCore
	}{
		{
			name: "fixed 1",
			len:  1,
			col:  New[[1]byte](),
		},
		{
			name: "fixed 2",
			len:  2,
			col:  New[[2]byte](),
		},
		{
			name: "fixed 3",
			len:  3,
			col:  New[[3]byte](),
		},
		{
			name: "fixed 4",
			len:  4,
			col:  New[[4]byte](),
		},
		{
			name: "fixed 5",
			len:  5,
			col:  New[[5]byte](),
		},
		{
			name: "fixed 6",
			len:  6,
			col:  New[[6]byte](),
		},
		{
			name: "fixed 7",
			len:  7,
			col:  New[[7]byte](),
		},
		{
			name: "fixed 8",
			len:  8,
			col:  New[[8]byte](),
		},
		{
			name: "fixed 9",
			len:  9,
			col:  New[[9]byte](),
		},
		{
			name: "fixed 10",
			len:  10,
			col:  New[[10]byte](),
		},
		{
			name: "fixed 11",
			len:  11,
			col:  New[[11]byte](),
		},
		{
			name: "fixed 12",
			len:  12,
			col:  New[[12]byte](),
		},
		{
			name: "fixed 13",
			len:  13,
			col:  New[[13]byte](),
		},
		{
			name: "fixed 14",
			len:  14,
			col:  New[[14]byte](),
		},
		{
			name: "fixed 15",
			len:  15,
			col:  New[[15]byte](),
		},
		{
			name: "fixed 16",
			len:  16,
			col:  New[[16]byte](),
		},
		{
			name: "fixed 17",
			len:  17,
			col:  New[[17]byte](),
		},
		{
			name: "fixed 18",
			len:  18,
			col:  New[[18]byte](),
		},
		{
			name: "fixed 19",
			len:  19,
			col:  New[[19]byte](),
		},
		{
			name: "fixed 20",
			len:  20,
			col:  New[[20]byte](),
		},
		{
			name: "fixed 21",
			len:  21,
			col:  New[[21]byte](),
		},
		{
			name: "fixed 22",
			len:  22,
			col:  New[[22]byte](),
		},
		{
			name: "fixed 23",
			len:  23,
			col:  New[[23]byte](),
		},
		{
			name: "fixed 24",
			len:  24,
			col:  New[[24]byte](),
		},
		{
			name: "fixed 25",
			len:  25,
			col:  New[[25]byte](),
		},
		{
			name: "fixed 26",
			len:  26,
			col:  New[[26]byte](),
		},
		{
			name: "fixed 27",
			len:  27,
			col:  New[[27]byte](),
		},
		{
			name: "fixed 28",
			len:  28,
			col:  New[[28]byte](),
		},
		{
			name: "fixed 29",
			len:  29,
			col:  New[[29]byte](),
		},
		{
			name: "fixed 30",
			len:  30,
			col:  New[[30]byte](),
		},
		{
			name: "fixed 31",
			len:  31,
			col:  New[[31]byte](),
		},
		{
			name: "fixed 32",
			len:  32,
			col:  New[[32]byte](),
		},
		{
			name: "fixed 33",
			len:  33,
			col:  New[[33]byte](),
		},
		{
			name: "fixed 34",
			len:  34,
			col:  New[[34]byte](),
		},
		{
			name: "fixed 35",
			len:  35,
			col:  New[[35]byte](),
		},
		{
			name: "fixed 36",
			len:  36,
			col:  New[[36]byte](),
		},
		{
			name: "fixed 37",
			len:  37,
			col:  New[[37]byte](),
		},
		{
			name: "fixed 38",
			len:  38,
			col:  New[[38]byte](),
		},
		{
			name: "fixed 39",
			len:  39,
			col:  New[[39]byte](),
		},
		{
			name: "fixed 40",
			len:  40,
			col:  New[[40]byte](),
		},
		{
			name: "fixed 41",
			len:  41,
			col:  New[[41]byte](),
		},
		{
			name: "fixed 42",
			len:  42,
			col:  New[[42]byte](),
		},
		{
			name: "fixed 43",
			len:  43,
			col:  New[[43]byte](),
		},
		{
			name: "fixed 44",
			len:  44,
			col:  New[[44]byte](),
		},
		{
			name: "fixed 45",
			len:  45,
			col:  New[[45]byte](),
		},
		{
			name: "fixed 46",
			len:  46,
			col:  New[[46]byte](),
		},
		{
			name: "fixed 47",
			len:  47,
			col:  New[[47]byte](),
		},
		{
			name: "fixed 48",
			len:  48,
			col:  New[[48]byte](),
		},
		{
			name: "fixed 49",
			len:  49,
			col:  New[[49]byte](),
		},
		{
			name: "fixed 50",
			len:  50,
			col:  New[[50]byte](),
		},
		{
			name: "fixed 51",
			len:  51,
			col:  New[[51]byte](),
		},
		{
			name: "fixed 52",
			len:  52,
			col:  New[[52]byte](),
		},
		{
			name: "fixed 53",
			len:  53,
			col:  New[[53]byte](),
		},
		{
			name: "fixed 54",
			len:  54,
			col:  New[[54]byte](),
		},
		{
			name: "fixed 55",
			len:  55,
			col:  New[[55]byte](),
		},
		{
			name: "fixed 56",
			len:  56,
			col:  New[[56]byte](),
		},
		{
			name: "fixed 57",
			len:  57,
			col:  New[[57]byte](),
		},
		{
			name: "fixed 58",
			len:  58,
			col:  New[[58]byte](),
		},
		{
			name: "fixed 59",
			len:  59,
			col:  New[[59]byte](),
		},
		{
			name: "fixed 60",
			len:  60,
			col:  New[[60]byte](),
		},
		{
			name: "fixed 61",
			len:  61,
			col:  New[[61]byte](),
		},
		{
			name: "fixed 62",
			len:  62,
			col:  New[[62]byte](),
		},
		{
			name: "fixed 63",
			len:  63,
			col:  New[[63]byte](),
		},
		{
			name: "fixed 64",
			len:  64,
			col:  New[[64]byte](),
		},
		{
			name: "fixed 65",
			len:  65,
			col:  New[[65]byte](),
		},
		{
			name: "fixed 66",
			len:  66,
			col:  New[[66]byte](),
		},
		{
			name: "fixed 67",
			len:  67,
			col:  New[[67]byte](),
		},
		{
			name: "fixed 68",
			len:  68,
			col:  New[[68]byte](),
		},
		{
			name: "fixed 69",
			len:  69,
			col:  New[[69]byte](),
		},
		{
			name: "fixed 70",
			len:  70,
			col:  New[[70]byte](),
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
