package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func testSnapshot(t *testing.T, goldenPath, actual string) {
	t.Helper()
	if os.Getenv("UPDATE_GOLDEN") != "" {
		err := os.WriteFile(goldenPath, []byte(actual), 0o644)
		require.NoError(t, err, "failed to update golden file")
		return
	}
	expected, err := os.ReadFile(goldenPath)
	require.NoError(t, err, "golden file not found: %s\nRun with UPDATE_GOLDEN=1 to create it", goldenPath)
	require.Equal(t, string(expected), actual, "output differs from golden file: %s", goldenPath)
}
