package blob

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

func TestGetBlobs(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setup(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	blobASize := int64(13)
	blobAData := strings.Repeat("a", int(blobASize))
	blobAOID := gittest.WriteBlob(t, cfg, repoPath, []byte(blobAData))

	blobBSize := int64(50)
	blobBData := strings.Repeat("b", int(blobBSize))
	blobBOID := gittest.WriteBlob(t, cfg, repoPath, []byte(blobBData))

	submoduleOID := gittest.DefaultObjectHash.HashData([]byte("arbitrary data"))

	subtreeOID := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{OID: blobAOID, Path: "subfile", Mode: "100644"},
	})
	subtreeSize := gittest.ObjectSize(t, cfg, repoPath, subtreeOID)

	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{OID: blobAOID, Path: "a", Mode: "100644"},
		gittest.TreeEntry{OID: blobBOID, Path: "b", Mode: "100644"},
		gittest.TreeEntry{OID: submoduleOID, Path: "submodule", Mode: "160000"},
		gittest.TreeEntry{OID: subtreeOID, Path: "dir", Mode: "040000"},
	))

	missingBlob := func(path string, revision git.ObjectID) *gitalypb.GetBlobsResponse {
		return &gitalypb.GetBlobsResponse{
			Path:     []byte(path),
			Revision: revision.String(),
		}
	}

	existingBlob := func(path string, revision, blobID git.ObjectID, data string, size int64) *gitalypb.GetBlobsResponse {
		return &gitalypb.GetBlobsResponse{
			Path:     []byte(path),
			Revision: revision.String(),
			Oid:      blobID.String(),
			Data:     []byte(data),
			Size:     size,
			Mode:     0o100644,
			Type:     gitalypb.ObjectType_BLOB,
		}
	}

	type setupData struct {
		request           *gitalypb.GetBlobsRequest
		expectedErr       error
		expectedResponses []*gitalypb.GetBlobsResponse
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "unset repository",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: "does-not-exist", Path: []byte("file")},
						},
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "empty RevisionPaths",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
					},
					expectedErr: structerr.NewInvalidArgument("empty RevisionPaths"),
				}
			},
		},
		{
			desc: "invalid Revision",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: "--output=/meow", Path: []byte("CHANGELOG")},
						},
					},
					expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
				}
			},
		},
		{
			desc: "zero-limit skips blob data",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
						Limit:      0,
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: commitID.String(), Path: []byte("a")},
							{Revision: commitID.String(), Path: []byte("b")},
						},
					},
					expectedResponses: []*gitalypb.GetBlobsResponse{
						existingBlob("a", commitID, blobAOID, "", blobASize),
						existingBlob("b", commitID, blobBOID, "", blobBSize),
					},
				}
			},
		},
		{
			desc: "negative limit sends all blob data",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
						Limit:      -1,
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: commitID.String(), Path: []byte("a")},
							{Revision: commitID.String(), Path: []byte("b")},
						},
					},
					expectedResponses: []*gitalypb.GetBlobsResponse{
						existingBlob("a", commitID, blobAOID, blobAData, blobASize),
						existingBlob("b", commitID, blobBOID, blobBData, blobBSize),
					},
				}
			},
		},
		{
			desc: "positive limit truncates blob data",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
						Limit:      5,
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: commitID.String(), Path: []byte("a")},
							{Revision: commitID.String(), Path: []byte("b")},
						},
					},
					expectedResponses: []*gitalypb.GetBlobsResponse{
						existingBlob("a", commitID, blobAOID, blobAData[:5], blobASize),
						existingBlob("b", commitID, blobBOID, blobBData[:5], blobBSize),
					},
				}
			},
		},
		{
			desc: "with limit exceeding blob sizes",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
						Limit:      9000,
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: commitID.String(), Path: []byte("a")},
							{Revision: commitID.String(), Path: []byte("b")},
						},
					},
					expectedResponses: []*gitalypb.GetBlobsResponse{
						existingBlob("a", commitID, blobAOID, blobAData, blobASize),
						existingBlob("b", commitID, blobBOID, blobBData, blobBSize),
					},
				}
			},
		},
		{
			desc: "nonexisting path",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: commitID.String(), Path: []byte("does-not-exist")},
						},
					},
					expectedResponses: []*gitalypb.GetBlobsResponse{
						missingBlob("does-not-exist", commitID),
					},
				}
			},
		},
		{
			desc: "mixed existing and nonexisting path",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: commitID.String(), Path: []byte("a")},
							{Revision: commitID.String(), Path: []byte("does-not-exist")},
						},
					},
					expectedResponses: []*gitalypb.GetBlobsResponse{
						existingBlob("a", commitID, blobAOID, "", blobASize),
						missingBlob("does-not-exist", commitID),
					},
				}
			},
		},
		{
			desc: "empty path",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: commitID.String(), Path: []byte("")},
						},
					},
					expectedResponses: []*gitalypb.GetBlobsResponse{
						missingBlob("", commitID),
					},
				}
			},
		},
		{
			desc: "path refers to root",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: commitID.String(), Path: []byte("/")},
						},
					},
					expectedResponses: []*gitalypb.GetBlobsResponse{
						missingBlob("/", commitID),
					},
				}
			},
		},
		{
			desc: "path refers to subdirectory",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: commitID.String(), Path: []byte("dir")},
						},
					},
					expectedResponses: []*gitalypb.GetBlobsResponse{
						// It's somewhat weird that this RPC also returns tree
						// objects, but that's simply the current behaviour.
						{
							Path:     []byte("dir"),
							Revision: commitID.String(),
							Oid:      subtreeOID.String(),
							Size:     subtreeSize,
							Mode:     0o040000,
							Type:     gitalypb.ObjectType_TREE,
						},
					},
				}
			},
		},
		{
			desc: "submodule",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.GetBlobsRequest{
						Repository: repo,
						RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
							{Revision: commitID.String(), Path: []byte("submodule")},
						},
					},
					expectedResponses: []*gitalypb.GetBlobsResponse{
						{
							Path:        []byte("submodule"),
							Revision:    commitID.String(),
							Oid:         submoduleOID.String(),
							Mode:        0o160000,
							Type:        gitalypb.ObjectType_COMMIT,
							IsSubmodule: true,
						},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			stream, err := client.GetBlobs(ctx, setup.request)
			require.NoError(t, err)

			var responses []*gitalypb.GetBlobsResponse
			for {
				response, err := stream.Recv()
				if err != nil {
					if setup.expectedErr == nil {
						setup.expectedErr = io.EOF
					}

					testhelper.RequireGrpcError(t, setup.expectedErr, err)
					break
				}
				require.NoError(t, err)

				responses = append(responses, response)
			}

			testhelper.ProtoEqual(t, setup.expectedResponses, responses)
		})
	}
}

func BenchmarkGetBlobs(b *testing.B) {
	ctx := testhelper.Context(b)
	cfg, client := setup(b, ctx)

	repo, repoPath := gittest.CreateRepository(b, ctx, cfg)

	blobID := gittest.WriteBlob(b, cfg, repoPath, []byte(strings.Repeat("a", 1024)))

	entries := make([]gittest.TreeEntry, 10000)
	for i := range entries {
		entries[i] = gittest.TreeEntry{OID: blobID, Path: fmt.Sprintf("file-%05d", i), Mode: "100644"}
	}

	commitID := gittest.WriteCommit(b, cfg, repoPath, gittest.WithTreeEntries(gittest.TreeEntry{
		OID:  gittest.WriteTree(b, cfg, repoPath, entries),
		Path: "dir",
		Mode: "040000",
	}))

	revisionPaths := func(count int) []*gitalypb.GetBlobsRequest_RevisionPath {
		paths := make([]*gitalypb.GetBlobsRequest_RevisionPath, count)
		for i := range paths {
			paths[i] = &gitalypb.GetBlobsRequest_RevisionPath{
				Revision: commitID.String(),
				Path:     []byte("dir/" + entries[i*len(entries)/count].Path),
			}
		}
		return paths
	}

	for _, tc := range []struct {
		desc  string
		paths int
	}{
		{desc: "single path", paths: 1},
		{desc: "100 paths in the same directory", paths: 100},
	} {
		b.Run(tc.desc, func(b *testing.B) {
			request := &gitalypb.GetBlobsRequest{
				Repository:    repo,
				RevisionPaths: revisionPaths(tc.paths),
				Limit:         -1,
			}

			b.ReportAllocs()

			for b.Loop() {
				stream, err := client.GetBlobs(ctx, request)
				require.NoError(b, err)

				for {
					_, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(b, err)
				}
			}
		})
	}
}

// TestGetBlobs_catfileProcessReuse verifies that a single GetBlobs call acquires exactly one
// catfile process. GetBlobs reads both object info and object content from the same
// catfile.ObjectReader, so a call against a cold cache must bump gitaly_catfile_cache_total exactly
// once. Acquiring separate info and content readers would make it two and fail this test.
func TestGetBlobs_catfileProcessReuse(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client, catfileCache := setupWithCatfileCache(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	blobData := strings.Repeat("a", 13)
	blobOID := gittest.WriteBlob(t, cfg, repoPath, []byte(blobData))
	commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithTreeEntries(
		gittest.TreeEntry{OID: blobOID, Path: "a", Mode: "100644"},
	))

	stream, err := client.GetBlobs(ctx, &gitalypb.GetBlobsRequest{
		Repository: repo,
		Limit:      -1,
		RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
			{Revision: commitID.String(), Path: []byte("a")},
		},
	})
	require.NoError(t, err)

	for {
		_, err := stream.Recv()
		if err != nil {
			testhelper.RequireGrpcError(t, io.EOF, err)
			break
		}
	}

	collector, ok := catfileCache.(prometheus.Collector)
	require.True(t, ok, "catfile cache must expose Prometheus metrics")

	testhelper.RequirePromMetrics(t, collector, `
# HELP gitaly_catfile_cache_total Counter of catfile cache hit/miss
# TYPE gitaly_catfile_cache_total counter
gitaly_catfile_cache_total{type="miss"} 1
`, "gitaly_catfile_cache_total")
}
