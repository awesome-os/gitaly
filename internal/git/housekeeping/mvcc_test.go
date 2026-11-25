package housekeeping

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	gitmvcc "gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage/mode"
)

func TestCleanMVCCMigrationLeftovers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		setup        func(t *testing.T, repoPath string)
		expectedGone []string
	}{
		{
			desc:  "nothing to remove",
			setup: func(t *testing.T, repoPath string) {},
		},
		{
			desc: "stale packed-refs",
			setup: func(t *testing.T, repoPath string) {
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "packed-refs"), []byte("# pack-refs with: peeled\n"), mode.File))
			},
			expectedGone: []string{"packed-refs"},
		},
		{
			desc: "stale reftable/",
			setup: func(t *testing.T, repoPath string) {
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "reftable"), mode.Directory))
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "reftable", "tables.list"), []byte(""), mode.File))
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "reftable", "0x1.ref"), []byte("x"), mode.File))
			},
			expectedGone: []string{"reftable"},
		},
		{
			desc: "pre-migration refs/ moved aside",
			setup: func(t *testing.T, repoPath string) {
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, gitmvcc.MigrationOldRefsDir, "heads"), mode.Directory))
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, gitmvcc.MigrationOldRefsDir, "heads", "main"), []byte("abc1234\n"), mode.File))
			},
			expectedGone: []string{gitmvcc.MigrationOldRefsDir},
		},
		{
			desc: "staging directories",
			setup: func(t *testing.T, repoPath string) {
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "ref_migration.abc123", "refs", "heads"), mode.Directory))
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "ref_migration.abc123", "refs", "heads", "main"), []byte("abc1234\n"), mode.File))
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "ref_migration.xyz789", "refs", "heads"), mode.Directory))
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "ref_migration.xyz789", "refs", "heads", "main"), []byte("abc1234\n"), mode.File))
			},
			expectedGone: []string{"ref_migration.abc123", "ref_migration.xyz789"},
		},
		{
			desc: "files-backend content in refs/",
			setup: func(t *testing.T, repoPath string) {
				require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "refs", "heads")))
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "refs", "heads"), mode.Directory))
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "refs", "heads", "main"), []byte("abc1234\n"), mode.File))
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "refs", "tags"), mode.Directory))
			},
			expectedGone: []string{"refs/heads", "refs/tags"},
		},
		{
			desc: "counts nested loose refs rather than top-level entries",
			setup: func(t *testing.T, repoPath string) {
				require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "refs", "heads")))
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "refs", "heads", "feature"), mode.Directory))
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "refs", "heads", "main"), []byte("abc1234\n"), mode.File))
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "refs", "heads", "feature", "a"), []byte("abc1234\n"), mode.File))
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "refs", "heads", "feature", "b"), []byte("abc1234\n"), mode.File))
				require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "refs", "tags"), mode.Directory))
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "refs", "tags", "v1"), []byte("abc1234\n"), mode.File))
			},
			expectedGone: []string{"refs/heads", "refs/tags"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			// The layout Git leaves behind for an MVCC repository: refs/ with a
			// refs/heads stub file.
			repoPath := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(repoPath, "refs"), mode.Directory))
			require.NoError(t, os.WriteFile(filepath.Join(repoPath, "refs", "heads"), []byte("this repository uses the mvcc format\n"), mode.File))
			tc.setup(t, repoPath)

			err := CleanMVCCMigrationLeftovers(repoPath)
			require.NoError(t, err)

			for _, path := range tc.expectedGone {
				require.NoFileExists(t, filepath.Join(repoPath, path))
				require.NoDirExists(t, filepath.Join(repoPath, path))
			}
			require.DirExists(t, filepath.Join(repoPath, "refs"), "refs/ must never be removed")
		})
	}
}
