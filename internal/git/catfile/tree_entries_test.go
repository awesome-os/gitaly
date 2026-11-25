package catfile

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

func TestTreeEntryFinder_FindByRevisionAndPath(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, objectReader, _, repoPath := setupObjectReader(t, ctx)

	blobID := gittest.WriteBlob(t, cfg, repoPath, []byte("data"))
	submoduleID := gittest.DefaultObjectHash.HashData([]byte("submodule"))
	subtreeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "file", Mode: "100644", OID: blobID},
		{Path: "file-other", Mode: "100755", OID: blobID},
	})
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "dir", Mode: "040000", OID: subtreeID},
		gittest.TreeEntry{Path: "file", Mode: "100644", OID: blobID},
		gittest.TreeEntry{Path: "submodule", Mode: "160000", OID: submoduleID},
	))
	revision := commitID.String()

	finder := NewTreeEntryFinder(objectReader)

	for _, tc := range []struct {
		desc          string
		revision      string
		path          string
		expectedEntry *gitalypb.TreeEntry
	}{
		{
			desc:     "file in root tree",
			revision: revision,
			path:     "file",
			expectedEntry: &gitalypb.TreeEntry{
				CommitOid: revision,
				Oid:       blobID.String(),
				Path:      []byte("file"),
				Type:      gitalypb.TreeEntry_BLOB,
				Mode:      0o100644,
			},
		},
		{
			desc:     "subtree",
			revision: revision,
			path:     "dir",
			expectedEntry: &gitalypb.TreeEntry{
				CommitOid: revision,
				Oid:       subtreeID.String(),
				Path:      []byte("dir"),
				Type:      gitalypb.TreeEntry_TREE,
				Mode:      0o40000,
			},
		},
		{
			desc:     "submodule",
			revision: revision,
			path:     "submodule",
			expectedEntry: &gitalypb.TreeEntry{
				CommitOid: revision,
				Oid:       submoduleID.String(),
				Path:      []byte("submodule"),
				Type:      gitalypb.TreeEntry_COMMIT,
				Mode:      0o160000,
			},
		},
		{
			desc:     "file in subtree",
			revision: revision,
			path:     "dir/file",
			expectedEntry: &gitalypb.TreeEntry{
				CommitOid: revision,
				Oid:       blobID.String(),
				Path:      []byte("dir/file"),
				Type:      gitalypb.TreeEntry_BLOB,
				Mode:      0o100644,
			},
		},
		{
			desc:     "second file in cached subtree",
			revision: revision,
			path:     "dir/file-other",
			expectedEntry: &gitalypb.TreeEntry{
				CommitOid: revision,
				Oid:       blobID.String(),
				Path:      []byte("dir/file-other"),
				Type:      gitalypb.TreeEntry_BLOB,
				Mode:      0o100755,
			},
		},
		{
			desc:     "missing file in subtree",
			revision: revision,
			path:     "dir/missing",
		},
		{
			desc:     "missing directory",
			revision: revision,
			path:     "missing/file",
		},
		{
			desc:     "path below a blob",
			revision: revision,
			path:     "file/file",
		},
		{
			desc:     "trailing slash",
			revision: revision,
			path:     "dir/",
		},
		{
			desc:     "unclean path",
			revision: revision,
			path:     "dir//file",
		},
		{
			desc:     "path outside of repository",
			revision: revision,
			path:     "../file",
		},
		{
			desc:     "empty path",
			revision: revision,
			path:     "",
		},
		{
			desc:     "missing revision",
			revision: gittest.DefaultObjectHash.HashData([]byte("missing")).String(),
			path:     "file",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			entry, err := finder.FindByRevisionAndPath(ctx, tc.revision, tc.path)
			require.NoError(t, err)
			testhelper.ProtoEqual(t, tc.expectedEntry, entry)
		})
	}
}
