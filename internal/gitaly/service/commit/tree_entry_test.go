package commit

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestTreeEntry(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupCommitService(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	blobContent := []byte("1234567890")
	blobID := gittest.WriteBlob(t, cfg, repoPath, blobContent)

	blobWithSpacesContent := []byte("space")
	blobWithSpacesID := gittest.WriteBlob(t, cfg, repoPath, blobWithSpacesContent)

	multilineContent := []byte("line 1\nline 2\nline 3\nline 4")
	multilineID := gittest.WriteBlob(t, cfg, repoPath, multilineContent)

	submoduleCommit := gittest.DefaultObjectHash.HashData([]byte("submodule commit"))

	subfileContent := []byte("subfile content")
	subfileID := gittest.WriteBlob(t, cfg, repoPath, subfileContent)
	subtreeID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "subfile", Mode: "100644", OID: subfileID},
	})
	subtreeSize := gittest.ObjectSize(t, cfg, repoPath, subtreeID)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"), gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "blob", Mode: "100644", OID: blobID},
		gittest.TreeEntry{Path: "blob with spaces", Mode: "100644", OID: blobWithSpacesID},
		gittest.TreeEntry{Path: "multiline", Mode: "100644", OID: multilineID},
		gittest.TreeEntry{Path: "submodule", Mode: "160000", OID: submoduleCommit},
		gittest.TreeEntry{Path: "subtree", Mode: "040000", OID: subtreeID},
	))

	for _, tc := range []struct {
		desc              string
		request           *gitalypb.TreeEntryRequest
		expectedErr       error
		expectedResponses []*gitalypb.TreeEntryResponse
	}{
		{
			desc: "missing repository",
			request: &gitalypb.TreeEntryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "fake",
					RelativePath: "path",
				},
				Revision: []byte(commitID),
				Path:     []byte("blob"),
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
		{
			desc: "unset repository",
			request: &gitalypb.TreeEntryRequest{
				Repository: nil,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "empty revision",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   nil,
				Path:       []byte("blob"),
			},
			expectedErr: structerr.NewInvalidArgument("empty revision"),
		},
		{
			desc: "empty path",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
			},
			expectedErr: structerr.NewInvalidArgument("empty Path"),
		},
		{
			desc: "invalid revision",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte("--output=/meow"),
				Path:       []byte("blob"),
			},
			expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
		},
		{
			desc: "negative limit",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				Limit:      -1,
			},
			expectedErr: structerr.NewInvalidArgument("negative Limit"),
		},
		{
			desc: "negative maximum size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				MaxSize:    -1,
			},
			expectedErr: structerr.NewInvalidArgument("negative MaxSize"),
		},
		{
			desc: "object size exceeds maximum size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				MaxSize:    5,
			},
			expectedErr: structerr.NewFailedPrecondition("object size (10) is bigger than the maximum allowed size (5)"),
		},
		{
			desc: "path escapes repository",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				// Git blows up on paths like this
				Path: []byte("../bar/.gitkeep"),
			},
			expectedErr: testhelper.WithInterceptedMetadata(structerr.NewNotFound("tree entry not found"), "path", "../bar/.gitkeep"),
		},
		{
			desc: "missing file with space in path",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("with space/README.md"),
			},
			expectedErr: testhelper.WithInterceptedMetadata(structerr.NewNotFound("tree entry not found"), "path", "with space/README.md"),
		},
		{
			desc: "missing file",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("missing.rb"),
			},
			expectedErr: testhelper.WithInterceptedMetadata(structerr.NewNotFound("tree entry not found"), "path", "missing.rb"),
		},
		{
			desc: "without limits",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  blobID.String(),
					Size: int64(len(blobContent)),
					Mode: 0o100644,
					Data: blobContent,
				},
			},
		},
		{
			desc: "with limit exceeding entry size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				Limit:      20,
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  blobID.String(),
					Size: int64(len(blobContent)),
					Mode: 0o100644,
					Data: blobContent,
				},
			},
		},
		{
			desc: "with maximum size exceeding entry size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				MaxSize:    20,
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  blobID.String(),
					Size: int64(len(blobContent)),
					Mode: 0o100644,
					Data: blobContent,
				},
			},
		},
		{
			desc: "path with space",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob with spaces"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  blobWithSpacesID.String(),
					Size: int64(len(blobWithSpacesContent)),
					Mode: 0o100644,
					Data: blobWithSpacesContent,
				},
			},
		},
		{
			desc: "with limit that truncates the response",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("blob"),
				Limit:      5,
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  blobID.String(),
					Size: int64(len(blobContent)),
					Mode: 0o100644,
					Data: blobContent[:5],
				},
			},
		},
		{
			desc: "with submodule",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("submodule"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_COMMIT,
					Oid:  submoduleCommit.String(),
					Mode: 0o160000,
				},
			},
		},
		{
			desc: "subdirectory without trailing slash",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("subtree"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_TREE,
					Oid:  subtreeID.String(),
					Size: subtreeSize,
					Mode: 0o40000,
				},
			},
		},
		{
			desc: "subdirectory with trailing slash",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("subtree/"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_TREE,
					Oid:  subtreeID.String(),
					Size: subtreeSize,
					Mode: 0o40000,
				},
			},
		},
		{
			desc: "blob in subdirectory",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("subtree/subfile"),
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  subfileID.String(),
					Size: int64(len(subfileContent)),
					Mode: 0o100644,
					Data: subfileContent,
				},
			},
		},
		{
			desc: "line range starting at zero",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("multiline"),
				LineRange:  &gitalypb.LineRange{Start: 0, End: 1},
			},
			expectedErr: structerr.NewInvalidArgument("line range start must be at least 1"),
		},
		{
			desc: "line range ending before start",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("multiline"),
				LineRange:  &gitalypb.LineRange{Start: 2, End: 1},
			},
			expectedErr: structerr.NewInvalidArgument("line range end must not be smaller than start"),
		},
		{
			desc: "line range with object size exceeding maximum size",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("multiline"),
				MaxSize:    10,
				LineRange:  &gitalypb.LineRange{Start: 1, End: 1},
			},
			expectedErr: structerr.NewFailedPrecondition("object size (27) is bigger than the maximum allowed size (10)"),
		},
		{
			desc: "with line range",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("multiline"),
				LineRange:  &gitalypb.LineRange{Start: 2, End: 3},
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  multilineID.String(),
					Size: int64(len(multilineContent)),
					Mode: 0o100644,
					Data: []byte("line 2\nline 3\n"),
				},
			},
		},
		{
			desc: "with line range including last line without trailing newline",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("multiline"),
				LineRange:  &gitalypb.LineRange{Start: 3, End: 10},
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  multilineID.String(),
					Size: int64(len(multilineContent)),
					Mode: 0o100644,
					Data: []byte("line 3\nline 4"),
				},
			},
		},
		{
			desc: "with line range beyond last line",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("multiline"),
				LineRange:  &gitalypb.LineRange{Start: 5, End: 10},
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  multilineID.String(),
					Size: int64(len(multilineContent)),
					Mode: 0o100644,
				},
			},
		},
		{
			desc: "with line range and limit",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("multiline"),
				Limit:      4,
				LineRange:  &gitalypb.LineRange{Start: 2, End: 3},
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_BLOB,
					Oid:  multilineID.String(),
					Size: int64(len(multilineContent)),
					Mode: 0o100644,
					Data: []byte("line"),
				},
			},
		},
		{
			desc: "line range is ignored for subdirectories",
			request: &gitalypb.TreeEntryRequest{
				Repository: repo,
				Revision:   []byte(commitID),
				Path:       []byte("subtree"),
				LineRange:  &gitalypb.LineRange{Start: 2, End: 3},
			},
			expectedResponses: []*gitalypb.TreeEntryResponse{
				{
					Type: gitalypb.TreeEntryResponse_TREE,
					Oid:  subtreeID.String(),
					Size: subtreeSize,
					Mode: 0o40000,
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			stream, err := client.TreeEntry(ctx, tc.request)
			require.NoError(t, err)

			responses, err := testhelper.Receive(stream.Recv)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
			testhelper.ProtoEqual(t, tc.expectedResponses, responses)
		})
	}
}

func TestTreeEntry_lineRangeDrainsSmallBlobs(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	var catfileCache catfile.Cache
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		catfileCache = deps.CatfileCache
		gitalypb.RegisterCommitServiceServer(srv, NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
	})
	commitClient := newCommitServiceClient(t, cfg.SocketPath)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	line := []byte("line\n")
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{Path: "small", Mode: "100644", Content: string(bytes.Repeat(line, 10))},
		gittest.TreeEntry{Path: "large", Mode: "100644", Content: string(bytes.Repeat(line, catfile.MaxDrainSize/len(line)+1))},
	))

	for _, tc := range []struct {
		path                   string
		expectedDirtyProcesses float64
	}{
		{path: "small", expectedDirtyProcesses: 0},
		{path: "large", expectedDirtyProcesses: 1},
	} {
		stream, err := commitClient.TreeEntry(ctx, &gitalypb.TreeEntryRequest{
			Repository: repo,
			Revision:   []byte(commitID),
			Path:       []byte(tc.path),
			LineRange:  &gitalypb.LineRange{Start: 1, End: 1},
		})
		require.NoError(t, err)

		responses, err := testhelper.Receive(stream.Recv)
		require.NoError(t, err)
		require.Len(t, responses, 1)
		require.Equal(t, line, responses[0].GetData())

		require.Equal(t, tc.expectedDirtyProcesses, testhelper.PromCounterValue(t, catfileCache.(prometheus.Collector), "gitaly_catfile_cache_total", "type", "dirty"))
	}
}
