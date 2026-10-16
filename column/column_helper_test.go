package column

import (
	"strconv"
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
		{
			name:    "Nullable JSON",
			chType:  "Nullable(JSON)",
			wantErr: "nullable invalid type: nullable JSON is not supported yet",
		},
		{
			name:    "Array JSON",
			chType:  "Array(JSON)",
			wantErr: "array invalid type: array of JSON is not supported yet",
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

func TestColumnByTypeJSON(t *testing.T) {
	col, err := ColumnByType([]byte("JSON"), 0, false, false, "")
	require.NoError(t, err)
	_, ok := col.(*JSON)
	assert.True(t, ok, "expected JSON column, got %T", col)
}

func TestJSONSetColumnHeaderTypedDefinition(t *testing.T) {
	c := NewJSON()
	// Typed definitions without a name have no Name in ColumnData, so they are skipped.
	// This should not error — typed JSON definitions are now supported.
	err := c.SetColumnHeader(ColumnHeader{ChType: []byte("JSON(a String)")})
	assert.NoError(t, err)
}

func TestGetFixedStringType(t *testing.T) {
	for _, n := range []int{1, 2, 7, 16, 70, 100} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			c, err := getFixedType(n, 0, false, false)
			require.NoError(t, err)
			require.IsType(t, &FixedString{}, c)
			assert.Equal(t, n, c.(*FixedString).Size())

			c, err = getFixedType(n, 0, true, false)
			require.NoError(t, err)
			assert.IsType(t, &FixedStringNullable{}, c)

			c, err = getFixedType(n, 1, false, false)
			require.NoError(t, err)
			assert.IsType(t, &Array[string]{}, c)

			c, err = getFixedType(n, 1, true, false)
			require.NoError(t, err)
			assert.IsType(t, &ArrayNullable[string]{}, c)

			c, err = getFixedType(n, 0, false, true)
			require.NoError(t, err)
			assert.IsType(t, &LowCardinality[string]{}, c)

			c, err = getFixedType(n, 0, true, true)
			require.NoError(t, err)
			assert.IsType(t, &LowCardinalityNullable[string]{}, c)
		})
	}

	_, err := getFixedType(0, 0, false, false)
	require.Error(t, err)
}
