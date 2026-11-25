package blob

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v19/streamio"
	"google.golang.org/grpc"
)

func TestGetBlob_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setup(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	smallBlobContents := []byte("small blob")
	smallBlobLen := int64(len(smallBlobContents))
	smallBlobID := gittest.WriteBlob(t, cfg, repoPath, smallBlobContents)

	largeBlobContents := bytes.Repeat([]byte{1}, 1024*1024)
	largeBlobID := gittest.WriteBlob(t, cfg, repoPath, largeBlobContents)

	multilineBlobContents := []byte("line 1\nline 2\nline 3\nline 4")
	multilineBlobLen := int64(len(multilineBlobContents))
	multilineBlobID := gittest.WriteBlob(t, cfg, repoPath, multilineBlobContents)

	var largeMultilineBlobContents []byte
	for i := 1; i <= 100000; i++ {
		largeMultilineBlobContents = fmt.Appendf(largeMultilineBlobContents, "line %d\n", i)
	}
	largeMultilineBlobID := gittest.WriteBlob(t, cfg, repoPath, largeMultilineBlobContents)

	for _, tc := range []struct {
		desc            string
		oid             string
		limit           int64
		lineRange       *gitalypb.LineRange
		expectedContent []byte
		expectedSize    int64
	}{
		{
			desc:            "unlimited fetch",
			oid:             smallBlobID.String(),
			limit:           -1,
			expectedContent: smallBlobContents,
			expectedSize:    smallBlobLen,
		},
		{
			desc:            "limit larger than blob size",
			oid:             smallBlobID.String(),
			limit:           smallBlobLen + 1,
			expectedContent: smallBlobContents,
			expectedSize:    smallBlobLen,
		},
		{
			desc:         "limit zero",
			oid:          smallBlobID.String(),
			limit:        0,
			expectedSize: smallBlobLen,
		},
		{
			desc:            "limit greater than zero, less than blob size",
			oid:             smallBlobID.String(),
			limit:           10,
			expectedContent: smallBlobContents[:10],
			expectedSize:    smallBlobLen,
		},
		{
			desc:            "large blob",
			oid:             largeBlobID.String(),
			limit:           10,
			expectedContent: largeBlobContents[:10],
			expectedSize:    int64(len(largeBlobContents)),
		},
		{
			desc:            "line range",
			oid:             multilineBlobID.String(),
			limit:           -1,
			lineRange:       &gitalypb.LineRange{Start: 2, End: 3},
			expectedContent: []byte("line 2\nline 3\n"),
			expectedSize:    multilineBlobLen,
		},
		{
			desc:            "line range including last line without trailing newline",
			oid:             multilineBlobID.String(),
			limit:           -1,
			lineRange:       &gitalypb.LineRange{Start: 3, End: 10},
			expectedContent: []byte("line 3\nline 4"),
			expectedSize:    multilineBlobLen,
		},
		{
			desc:         "line range beyond last line",
			oid:          multilineBlobID.String(),
			limit:        -1,
			lineRange:    &gitalypb.LineRange{Start: 5, End: 10},
			expectedSize: multilineBlobLen,
		},
		{
			desc:            "line range with limit",
			oid:             multilineBlobID.String(),
			limit:           4,
			lineRange:       &gitalypb.LineRange{Start: 2, End: 3},
			expectedContent: []byte("line"),
			expectedSize:    multilineBlobLen,
		},
		{
			desc:         "line range with zero limit",
			oid:          multilineBlobID.String(),
			limit:        0,
			lineRange:    &gitalypb.LineRange{Start: 2, End: 3},
			expectedSize: multilineBlobLen,
		},
		{
			desc:            "line range in large blob",
			oid:             largeMultilineBlobID.String(),
			limit:           -1,
			lineRange:       &gitalypb.LineRange{Start: 50000, End: 50002},
			expectedContent: []byte("line 50000\nline 50001\nline 50002\n"),
			expectedSize:    int64(len(largeMultilineBlobContents)),
		},
		{
			desc:            "line range spanning multiple response messages",
			oid:             largeMultilineBlobID.String(),
			limit:           -1,
			lineRange:       &gitalypb.LineRange{Start: 1, End: 100000},
			expectedContent: largeMultilineBlobContents,
			expectedSize:    int64(len(largeMultilineBlobContents)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.GetBlob(ctx, &gitalypb.GetBlobRequest{
				Repository: repo,
				Oid:        tc.oid,
				Limit:      tc.limit,
				LineRange:  tc.lineRange,
			})
			require.NoError(t, err, "initiate RPC")

			reportedSize, reportedOid, data, err := getBlob(stream)
			require.NoError(t, err, "consume response")

			require.Equal(t, tc.expectedSize, reportedSize, "real blob size")

			require.NotEmpty(t, reportedOid)
			require.True(t, bytes.Equal(tc.expectedContent, data), "returned data exactly as expected")
		})
	}
}

func TestGetBlob_notFound(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setup(t, ctx)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	stream, err := client.GetBlob(ctx, &gitalypb.GetBlobRequest{
		Repository: repo,
		Oid:        "doesnotexist",
	})
	require.NoError(t, err)

	reportedSize, reportedOid, data, err := getBlob(stream)
	require.NoError(t, err)
	require.Zero(t, reportedSize)
	require.Empty(t, reportedOid)
	require.Zero(t, len(data))
}

func getBlob(stream gitalypb.BlobService_GetBlobClient) (int64, string, []byte, error) {
	firstResponse, err := stream.Recv()
	if err != nil {
		return 0, "", nil, err
	}

	data := &bytes.Buffer{}
	_, err = data.Write(firstResponse.GetData())
	if err != nil {
		return 0, "", nil, err
	}

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		if response.GetSize() != 0 {
			return nil, fmt.Errorf("size may only be set in the first response message")
		}
		if len(response.GetOid()) != 0 {
			return nil, fmt.Errorf("oid may only be set in the first response message")
		}
		return response.GetData(), err
	})

	_, err = io.Copy(data, reader)
	return firstResponse.GetSize(), firstResponse.GetOid(), data.Bytes(), err
}

func TestGetBlob_invalidRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setup(t, ctx)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
	oid := gittest.WriteBlob(t, cfg, repoPath, []byte("something")).String()

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.GetBlobRequest
		expectedErr error
	}{
		{
			desc: "missing repository",
			request: &gitalypb.GetBlobRequest{
				Repository: nil,
				Oid:        oid,
			},
			expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
		},
		{
			desc: "invalid storage name",
			request: &gitalypb.GetBlobRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "fake",
					RelativePath: "path",
				},
				Oid: oid,
			},
			expectedErr: testhelper.ToInterceptedMetadata(structerr.NewInvalidArgument(
				"%w", storage.NewStorageNotFoundError("fake"),
			)),
		},
		{
			desc: "invalid relative path",
			request: &gitalypb.GetBlobRequest{
				Repository: &gitalypb.Repository{
					StorageName:  cfg.Storages[0].Name,
					RelativePath: "path",
				},
				Oid: oid,
			},
			expectedErr: testhelper.ToInterceptedMetadata(
				structerr.New("%w", storage.NewRepositoryNotFoundError(cfg.Storages[0].Name, "path")),
			),
		},
		{
			desc: "missing object ID",
			request: &gitalypb.GetBlobRequest{
				Repository: repo,
			},
			expectedErr: structerr.NewInvalidArgument("empty Oid"),
		},
		{
			desc: "line range starting at zero",
			request: &gitalypb.GetBlobRequest{
				Repository: repo,
				Oid:        oid,
				LineRange:  &gitalypb.LineRange{Start: 0, End: 1},
			},
			expectedErr: structerr.NewInvalidArgument("line range start must be at least 1"),
		},
		{
			desc: "line range ending before start",
			request: &gitalypb.GetBlobRequest{
				Repository: repo,
				Oid:        oid,
				LineRange:  &gitalypb.LineRange{Start: 2, End: 1},
			},
			expectedErr: structerr.NewInvalidArgument("line range end must not be smaller than start"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.GetBlob(ctx, tc.request)
			require.NoError(t, err)

			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func TestGetBlob_lineRangeDrainsSmallBlobs(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	var catfileCache catfile.Cache
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		catfileCache = deps.CatfileCache
		gitalypb.RegisterBlobServiceServer(srv, NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
	})

	conn, err := client.New(ctx, cfg.SocketPath)
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })
	blobClient := gitalypb.NewBlobServiceClient(conn)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

	line := []byte("line\n")
	smallBlobID := gittest.WriteBlob(t, cfg, repoPath, bytes.Repeat(line, 10))
	largeBlobID := gittest.WriteBlob(t, cfg, repoPath, bytes.Repeat(line, catfile.MaxDrainSize/len(line)+1))

	for _, tc := range []struct {
		oid                    string
		expectedDirtyProcesses float64
	}{
		{oid: smallBlobID.String(), expectedDirtyProcesses: 0},
		{oid: largeBlobID.String(), expectedDirtyProcesses: 1},
	} {
		stream, err := blobClient.GetBlob(ctx, &gitalypb.GetBlobRequest{
			Repository: repo,
			Oid:        tc.oid,
			Limit:      -1,
			LineRange:  &gitalypb.LineRange{Start: 1, End: 1},
		})
		require.NoError(t, err)

		_, _, data, err := getBlob(stream)
		require.NoError(t, err)
		require.Equal(t, line, data)

		require.Equal(t, tc.expectedDirtyProcesses, testhelper.PromCounterValue(t, catfileCache.(prometheus.Collector), "gitaly_catfile_cache_total", "type", "dirty"))
	}
}
