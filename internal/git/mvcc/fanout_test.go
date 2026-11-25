package mvcc

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage/mode"
)

func TestFanoutRelPath(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc        string
		kind        string
		name        string
		expected    string
		expectedErr string
	}{
		{
			desc:     "manifest hash",
			kind:     manifestDirectory,
			name:     "abcdef0123456789",
			expected: "manifests/ab/cdef0123456789",
		},
		{
			desc:     "pack artifact",
			kind:     "pack",
			name:     "cec0880cf4323043aeb2268a3eaefdbd0e006c60.pack",
			expected: "pack/ce/c0880cf4323043aeb2268a3eaefdbd0e006c60.pack",
		},
		{
			desc:     "reftable artifact",
			kind:     refsDirectory,
			name:     "e546fa0090bfdaa4.ref",
			expected: "refs/e5/46fa0090bfdaa4.ref",
		},
		{
			desc:        "name shorter than the fanout prefix",
			kind:        "pack",
			name:        "a",
			expectedErr: `pack name "a" is shorter than the 2-character fanout prefix`,
		},
		{
			desc:        "empty name",
			kind:        "pack",
			expectedErr: `pack name "" is shorter than the 2-character fanout prefix`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			actual, err := fanoutRelPath(tc.kind, tc.name)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}

	actual, err := ManifestRelPath("abcdef")
	require.NoError(t, err)
	require.Equal(t, "manifests/ab/cdef", actual)

	_, err = ManifestRelPath("a")
	require.Error(t, err)
}

func TestScanManifestDir(t *testing.T) {
	t.Parallel()

	t.Run("reconstructs hashes across the fanout", func(t *testing.T) {
		t.Parallel()

		repo := newMVCCRepo(t)

		first := repo.writeManifest("first", time.Minute)
		second := repo.writeManifest("second", time.Minute)

		manifestsDir := filepath.Join(repo.mvccPath, manifestDirectory)

		// Neither a plain file nor a directory that is not a fanout prefix holds manifests.
		require.NoError(t, os.WriteFile(filepath.Join(manifestsDir, "stray"), nil, mode.File))
		require.NoError(t, os.MkdirAll(filepath.Join(manifestsDir, "notafanout"), mode.Directory))
		require.NoError(t, os.WriteFile(filepath.Join(manifestsDir, "notafanout", "x"), nil, mode.File))

		manifests, err := scanManifestDir(manifestsDir)
		require.NoError(t, err)

		hashes := make([]string, 0, len(manifests))
		for _, m := range manifests {
			hashes = append(hashes, m.hash)
		}
		require.ElementsMatch(t, []string{first, second}, hashes)
	})

	t.Run("errors when the directory is missing", func(t *testing.T) {
		t.Parallel()

		_, err := scanManifestDir(filepath.Join(t.TempDir(), "missing"))
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}
