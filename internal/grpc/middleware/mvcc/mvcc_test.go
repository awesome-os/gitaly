package mvcc

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v19/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type hookTestSetup struct {
	cfg         config.Cfg
	repo        *gitalypb.Repository
	interceptor *CacheInterceptor
	// gitBinary returns the Git binary that commands run with in ctx.
	gitBinary  func(ctx context.Context) string
	mvccBinary string
}

func setupHookTest(t *testing.T, ctx context.Context) hookTestSetup {
	t.Helper()

	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)

	repo, _ := gittest.CreateRepository(t, gitcmd.ContextWithMVCC(ctx), cfg, gittest.CreateRepositoryConfig{
		ReferenceBackend:       git.ReferenceBackendMVCC,
		SkipCreationViaService: true,
	})

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)
	gitBinary := func(ctx context.Context) string {
		return gitCmdFactory.GetExecutionEnvironment(ctx).BinaryPath
	}
	mvccBinary := gitBinary(gitcmd.ContextWithMVCC(ctx))
	require.NotEqual(t, gitBinary(ctx), mvccBinary)

	return hookTestSetup{
		cfg:         cfg,
		repo:        repo,
		interceptor: NewMvccInterceptor(mvcc.NewCacheFactory(cfg.RuntimeDir, locator), testhelper.SharedLogger(t), protoregistry.GitalyProtoPreregistered, locator),
		gitBinary:   gitBinary,
		mvccBinary:  mvccBinary,
	}
}

// runRPC runs fn within an RPC on the repository, with the RPC's context.
func (s hookTestSetup) runRPC(t *testing.T, ctx context.Context, fn func(rpcCtx context.Context)) {
	t.Helper()

	_, err := s.interceptor.UnaryInterceptor()(ctx,
		&gitalypb.RepositorySizeRequest{Repository: s.repo},
		&grpc.UnaryServerInfo{FullMethod: gitalypb.RepositoryService_RepositorySize_FullMethodName},
		func(ctx context.Context, _ any) (any, error) {
			require.NotNil(t, mvcc.CacheFromContext(ctx))
			fn(ctx)
			return nil, nil
		},
	)
	require.NoError(t, err)
}

// hooksEnv returns the environment of hooks of Git commands spawned with rpcCtx.
func (s hookTestSetup) hooksEnv(t *testing.T, rpcCtx context.Context, repo *gitalypb.Repository) []string {
	t.Helper()

	env, err := gitcmd.NewHooksPayload(rpcCtx, s.cfg, repo, gittest.DefaultObjectHash, nil, nil, gitcmd.ReceivePackHooks, nil).Env()
	require.NoError(t, err)

	return []string{env}
}

func TestCacheInterceptor_InternalUnaryInterceptor(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	s := setupHookTest(t, ctx)

	// callHook calls a hook RPC with the given environment. The call gets a new context, like any
	// hook RPC.
	callHook := func(t *testing.T, repo *gitalypb.Repository, env []string) (context.Context, error) {
		var hookCtx context.Context
		_, err := s.interceptor.InternalUnaryInterceptor()(ctx,
			&gitalypb.PackObjectsHookWithSidechannelRequest{Repository: repo, EnvironmentVariables: env},
			&grpc.UnaryServerInfo{FullMethod: gitalypb.HookService_PackObjectsHookWithSidechannel_FullMethodName},
			func(ctx context.Context, _ any) (any, error) {
				hookCtx = ctx
				return nil, nil
			},
		)

		return hookCtx, err
	}

	// The error for a hook whose payload names a cache that isn't registered for the repository.
	notFoundErr := func(repo *gitalypb.Repository, id mvcc.CacheID) error {
		return structerr.NewFailedPrecondition("mvcc: cache of the spawning RPC not found").WithMetadataItems(
			structerr.MetadataItem{Key: "mvcc_cache_id", Value: id},
			structerr.MetadataItem{Key: "storage_name", Value: repo.GetStorageName()},
			structerr.MetadataItem{Key: "relative_path", Value: repo.GetRelativePath()},
		)
	}

	var id mvcc.CacheID

	var env []string
	s.runRPC(t, ctx, func(rpcCtx context.Context) {
		env = s.hooksEnv(t, rpcCtx, s.repo)

		id = mvcc.CacheIDFromContext(rpcCtx)

		hookCtx, err := callHook(t, s.repo, env)
		require.NoError(t, err)
		require.Same(t, mvcc.CacheFromContext(rpcCtx), mvcc.CacheFromContext(hookCtx))
		require.Equal(t, s.mvccBinary, s.gitBinary(hookCtx))
		// Hooks spawned by the hook RPC find the cache too.
		require.Equal(t, mvcc.CacheIDFromContext(rpcCtx), mvcc.CacheIDFromContext(hookCtx))

		t.Run("other repository", func(t *testing.T) {
			otherRepo := proto.CloneOf(s.repo)
			otherRepo.RelativePath = gittest.NewRepositoryName(t)

			_, err := callHook(t, otherRepo, s.hooksEnv(t, rpcCtx, otherRepo))
			testhelper.RequireGrpcError(t, notFoundErr(otherRepo, id), err)
		})
	})

	t.Run("spawning RPC done", func(t *testing.T) {
		// The cache is unregistered, so the hook RPC would read a different manifest.
		_, err := callHook(t, s.repo, env)
		testhelper.RequireGrpcError(t, notFoundErr(s.repo, id), err)
	})

	t.Run("no cache ID", func(t *testing.T) {
		// No RPC with a cache spawned the hook. The hook RPC still needs the binary that can read
		// the repository.
		hookCtx, err := callHook(t, s.repo, s.hooksEnv(t, ctx, s.repo))
		require.NoError(t, err)
		require.Nil(t, mvcc.CacheFromContext(hookCtx))
		require.Equal(t, s.mvccBinary, s.gitBinary(hookCtx))
	})

	t.Run("other RPC", func(t *testing.T) {
		// RPCs other than hook RPCs get their own cache, as on the external server.
		_, err := s.interceptor.InternalUnaryInterceptor()(ctx,
			&gitalypb.RepositorySizeRequest{Repository: s.repo},
			&grpc.UnaryServerInfo{FullMethod: gitalypb.RepositoryService_RepositorySize_FullMethodName},
			func(ctx context.Context, _ any) (any, error) {
				require.NotNil(t, mvcc.CacheFromContext(ctx))
				require.NotZero(t, mvcc.CacheIDFromContext(ctx))
				require.Equal(t, s.mvccBinary, s.gitBinary(ctx))
				return nil, nil
			},
		)
		require.NoError(t, err)
	})
}

type firstMessageStream struct {
	grpc.ServerStream
	ctx          context.Context
	firstMessage proto.Message
}

func (s *firstMessageStream) Context() context.Context { return s.ctx }

func (s *firstMessageStream) RecvMsg(m any) error {
	proto.Merge(m.(proto.Message), s.firstMessage)
	return nil
}

func TestCacheInterceptor_InternalStreamInterceptor(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	s := setupHookTest(t, ctx)

	s.runRPC(t, ctx, func(rpcCtx context.Context) {
		request := &gitalypb.PreReceiveHookRequest{Repository: s.repo, EnvironmentVariables: s.hooksEnv(t, rpcCtx, s.repo)}

		err := s.interceptor.InternalStreamInterceptor()(nil,
			&firstMessageStream{ctx: ctx, firstMessage: request},
			&grpc.StreamServerInfo{FullMethod: gitalypb.HookService_PreReceiveHook_FullMethodName},
			func(_ any, stream grpc.ServerStream) error {
				hookCtx := stream.Context()
				require.Same(t, mvcc.CacheFromContext(rpcCtx), mvcc.CacheFromContext(hookCtx))
				require.Equal(t, s.mvccBinary, s.gitBinary(hookCtx))

				// The handler still receives the message the interceptor peeked at.
				var received gitalypb.PreReceiveHookRequest
				require.NoError(t, stream.RecvMsg(&received))
				testhelper.ProtoEqual(t, request, &received)

				return nil
			},
		)
		require.NoError(t, err)
	})
}

func TestMapCommitError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc         string
		err          error
		expectedCode codes.Code
		expectedKind string
	}{
		{
			desc:         "nil error",
			err:          nil,
			expectedCode: codes.OK,
		},
		{
			desc:         "pointer conflict",
			err:          mvcc.ErrManifestPointerConflict,
			expectedCode: codes.Aborted,
			expectedKind: "cas_exhausted",
		},
		{
			desc:         "wrapped pointer conflict",
			err:          fmt.Errorf("commit: %w", mvcc.ErrManifestPointerConflict),
			expectedCode: codes.Aborted,
			expectedKind: "cas_exhausted",
		},
		{
			desc:         "merge overlap",
			err:          mvcc.ErrOverlap,
			expectedCode: codes.Aborted,
			expectedKind: "overlap",
		},
		{
			desc: "merge overlap wrapping a pointer conflict",
			// Should an error ever carry both, the more specific
			// kind must win.
			err:          fmt.Errorf("%w: %w", mvcc.ErrOverlap, mvcc.ErrManifestPointerConflict),
			expectedCode: codes.Aborted,
			expectedKind: "overlap",
		},
		{
			desc: "unrelated error is passed through",
			err:  io.ErrUnexpectedEOF,
			// Not a conflict, so it is returned untouched rather than
			// being dressed up as a gRPC status.
			expectedCode: codes.Unknown,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			mapped := mapCommitError(tc.err)

			if tc.expectedCode == codes.OK {
				require.NoError(t, mapped)
				return
			}

			require.Error(t, mapped)
			require.ErrorIs(t, mapped, tc.err)
			require.Equal(t, tc.expectedCode, status.Code(mapped))

			kind, ok := structerr.ExtractMetadata(mapped)["mvcc_conflict_kind"]
			if tc.expectedKind == "" {
				require.False(t, ok, "non-conflict errors must not be classified")
				return
			}
			require.True(t, ok, "conflicts must carry mvcc_conflict_kind")
			require.Equal(t, tc.expectedKind, kind)
		})
	}
}
