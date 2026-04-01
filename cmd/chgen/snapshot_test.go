package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const updateGolden = false // set to true to update golden files

func testSnapshot(t *testing.T, goldenPath, actual string) {
	t.Helper()
	if updateGolden {
		err := os.WriteFile(goldenPath, []byte(actual), 0o644)
		require.NoError(t, err, "failed to update golden file")
		return
	}
	expected, err := os.ReadFile(goldenPath)
	require.NoError(t, err, "golden file not found: %s\nRun with updateGolden=true to create it", goldenPath)
	require.Equal(t, string(expected), actual, "output differs from golden file: %s", goldenPath)
}
