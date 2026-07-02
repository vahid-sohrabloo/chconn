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
