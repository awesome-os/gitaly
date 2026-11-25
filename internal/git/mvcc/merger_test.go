package mvcc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMerger_patchReftable_idempotentRunCleansTemp covers the branch where
// the patched bytes hash to a path that already exists -- a merge replaying
// work a previous attempt completed. The temp file was never renamed on that
// path, so it has to be removed; leaving it behind drops a rebase-*.ref.tmp
// into the cache's refs directory on every retry.
func TestMerger_patchReftable_idempotentRunCleansTemp(t *testing.T) {
	t.Parallel()

	s, cacheDir := newTestMerger(t)
	refsDir := filepath.Join(cacheDir, "refs")
	require.NoError(t, os.MkdirAll(refsDir, 0o755))

	srcRefPath := "refs/source.ref"
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, srcRefPath), []byte("original"), 0o644))

	// A deterministic patch, so both runs content-address to the same
	// path and the second takes the already-exists branch.
	patchRange := func(path string, min, max uint64) error {
		return os.WriteFile(path, []byte("patched"), 0o644)
	}

	first, err := s.patchReftable(srcRefPath, 4, 5, patchRange)
	require.NoError(t, err)

	second, err := s.patchReftable(srcRefPath, 4, 5, patchRange)
	require.NoError(t, err)
	require.Equal(t, first, second, "identical input must content-address to the same path")

	entries, err := os.ReadDir(refsDir)
	require.NoError(t, err)
	var leaked []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			leaked = append(leaked, e.Name())
		}
	}
	require.Empty(t, leaked, "no temp file may be left behind in the cache")
}
