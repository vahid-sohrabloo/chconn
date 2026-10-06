package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnsGenerate_AllTypes(t *testing.T) {
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "all_types_model_columns_gen.go")

	err := generateColumns("testdata/all_types_model.go", outFile, false)
	require.NoError(t, err)

	got, err := os.ReadFile(outFile)
	require.NoError(t, err)

	testSnapshot(t, "testdata/all_types_model_columns_gen.go.golden", string(got))
}

func TestColumnsGenerate_AllTypesWithIter(t *testing.T) {
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "all_types_model_columns_gen.go")

	err := generateColumns("testdata/all_types_model.go", outFile, true)
	require.NoError(t, err)

	got, err := os.ReadFile(outFile)
	require.NoError(t, err)

	testSnapshot(t, "testdata/all_types_model_columns_iter_gen.go.golden", string(got))
}

func TestColumnsGenerate_Tuple(t *testing.T) {
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "tuple_model_columns_gen.go")

	err := generateColumns("testdata/tuple_model.go", outFile, false)
	require.NoError(t, err)

	got, err := os.ReadFile(outFile)
	require.NoError(t, err)

	testSnapshot(t, "testdata/tuple_model_columns_gen.go.golden", string(got))
}

func TestColumnsGenerate_SkipsUntaggedFields(t *testing.T) {
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "out.go")

	err := generateColumns("testdata/all_types_model.go", outFile, false)
	require.NoError(t, err)

	got, err := os.ReadFile(outFile)
	require.NoError(t, err)
	content := string(got)

	require.NotContains(t, content, "IgnoredNoTag")
	require.NotContains(t, content, "IgnoredNoDb")
	require.NotContains(t, content, "IgnoredDbDash")
	require.NotContains(t, content, "IgnoredNoChtype")
	require.NotContains(t, content, "IgnoredPrivate")
	require.NotContains(t, content, "ColTuple")
	require.NotContains(t, content, "ColNested")
}

func TestColumnsGenerate_Errors(t *testing.T) {
	cases := []struct {
		file    string
		wantErr string
	}{
		{"top_bad.go", "TopBad.Bad: incompatible"},
		{"sub_bad.go", "SubBad.T: BadElem.X: incompatible"},
		{"sub_missing.go", `SubMissing.T: OneElem.X: db name "x" is not in chtype`},
		{"sub_type_mismatch.go", `SubTypeMismatch.T: OneElem.X: chtype "String" does not match "Int8" in "Tuple(x Int8)"`},
		{"sub_order.go", `SubOrder.T: TwoElem fields are in order [a b], chtype "Tuple(b Int8, a String)" needs [b a]`},
		{"sub_extra.go", `SubExtra.T: chtype "Tuple(x String, z Int8)" element "z" has no field in OneElem`},
		{"conv_unknown.go", "ConvUnknown.V: chconv: no package-level func noSuchFunc"},
		{"conv_multi.go", "ConvMulti.V: chconv: multiResult must take 1 argument and return exactly one value"},
		{"conv_arg_type.go", "ConvArgType.V: chconv: takesInt64 takes int64, field is int32"},
		{"named_pointer.go", "NamedPointer.V: incompatible"},
		{"conv_mismatch.go", "ConvMismatch.V: chconv: toString returns string: incompatible"},
		{"conv_no_method.go", "ConvNoMethod.V: chconv: int8 has no method Nope"},
	}
	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			outFile := filepath.Join(t.TempDir(), "out.go")
			err := generateColumns(filepath.Join("testdata", "errcases", tc.file), outFile, false)
			require.ErrorContains(t, err, tc.wantErr)
			require.NoFileExists(t, outFile)
		})
	}
}

func TestColumnsGenerate_WriteOverride(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "out.go")
	err := generateColumns("testdata/simple_model.go", outFile, false, "testdata/templates/write_override.tmpl")
	require.NoError(t, err)

	got, err := os.ReadFile(outFile)
	require.NoError(t, err)
	testSnapshot(t, "testdata/simple_model_write_override_gen.go.golden", string(got))
}

func TestColumnsGenerate_TemplateErrors(t *testing.T) {
	cases := []struct {
		name      string
		templates []string
		wantErr   string
	}{
		{"invalid Go", []string{"testdata/templates/broken.tmpl"}, "generated code is not valid Go (templates testdata/templates/broken.tmpl)"},
		{
			"duplicate block",
			[]string{"testdata/templates/extra_a.tmpl", "testdata/templates/extra_b.tmpl"},
			`block "extra" is defined in both testdata/templates/extra_a.tmpl and testdata/templates/extra_b.tmpl`,
		},
		{"missing file", []string{"testdata/templates/nope.tmpl"}, "reading template"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outFile := filepath.Join(t.TempDir(), "out.go")
			err := generateColumns("testdata/simple_model.go", outFile, false, tc.templates...)
			require.ErrorContains(t, err, tc.wantErr)
			require.NoFileExists(t, outFile)
		})
	}
}

func TestColumnsGenerate_EmptyBlockRemovesMethod(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "out.go")
	err := generateColumns("testdata/simple_model.go", outFile, false, "testdata/templates/blank_reset.tmpl")
	require.NoError(t, err)

	got, err := os.ReadFile(outFile)
	require.NoError(t, err)
	require.NotContains(t, string(got), "Reset()")
	require.Contains(t, string(got), "SetWriteBufferSize(")
}

func TestColumnsGenerate_FromModelOutput(t *testing.T) {
	dir, err := os.MkdirTemp("testdata", "modelpipe-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	model, err := os.ReadFile("testdata/model_output.go.golden")
	require.NoError(t, err)
	input := filepath.Join(dir, "events.go")
	require.NoError(t, os.WriteFile(input, model, 0o600))

	outFile := filepath.Join(dir, "events_columns_gen.go")
	require.NoError(t, generateColumns(input, outFile, false))

	got, err := os.ReadFile(outFile)
	require.NoError(t, err)
	require.Contains(t, string(got), "t.Id.SetName")
	require.NotContains(t, string(got), "NestedData")
}

func TestColumnsGenerate_GoTypeFromTypeChecker(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "out.go")
	err := generateColumns("chgentest/conv_model.go", outFile, false, "testdata/templates/gotype.tmpl")
	require.NoError(t, err)

	got, err := os.ReadFile(outFile)
	require.NoError(t, err)
	require.Contains(t, string(got), "// GoType Opt: ConvOpt[int]")
	require.Contains(t, string(got), "// GoType Byte: byte")
}
