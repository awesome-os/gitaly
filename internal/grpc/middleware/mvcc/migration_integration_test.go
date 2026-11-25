package mvcc_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/localrepo"
	gitmvcc "gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/mvcc/migration"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/service/operations"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v19/internal/grpc/middleware/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// These tests run the interceptors on a live Gitaly server. They live in an
// external test package because testserver itself imports this package.

// countingRegister counts the migrations the interceptors register and cancel.
type countingRegister struct {
	registerCount int
	cancelCount   int
}

func (c *countingRegister) RegisterMigration(context.Context, *localrepo.Repo, func(context.Context) error, func() error) {
	c.registerCount++
}

func (c *countingRegister) CancelMigration(*localrepo.Repo) {
	c.cancelCount++
}

// noParent stands in for waiting on the parent of a graceful upgrade when
// there is none.
func noParent(context.Context) error { return nil }

type noopHousekeeping struct{}

func (noopHousekeeping) TryLockRepository(storage.Repository) (func(), bool) { return func() {}, true }

// requireMigrated waits until the repository has been migrated to the MVCC
// backend. Removing the staging directory is the migration's last step, and
// the config already names MVCC before then.
func requireMigrated(t *testing.T, repoPath string) {
	t.Helper()

	require.Eventually(t, func() bool {
		config, err := os.ReadFile(filepath.Join(repoPath, "config"))
		require.NoError(t, err)

		stages, err := filepath.Glob(filepath.Join(repoPath, gitmvcc.MigrationStageGlob))
		require.NoError(t, err)

		return strings.Contains(string(config), "refstorage = mvcc") && len(stages) == 0
	}, time.Minute, 10*time.Millisecond, "repository was not migrated")
}

func TestMigrationInterceptor(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.MVCCMigration).Run(t, testMigrationInterceptor)
}

func testMigrationInterceptor(t *testing.T, ctx context.Context) {
	testhelper.SkipWithPraefect(t, "Praefect-managed repositories are skipped")

	cfg := testcfg.Build(t)

	counter := &countingRegister{}
	callback := func(_ log.Logger, factory localrepo.Factory) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
		unary, stream := mvcc.NewMigrationInterceptors(factory, protoregistry.GitalyProtoPreregistered, counter)
		return []grpc.UnaryServerInterceptor{unary}, []grpc.StreamServerInterceptor{stream}
	}

	serverSocketPath := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(deps))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps))
	}, testserver.WithInterceptors(callback))
	cfg.SocketPath = serverSocketPath

	conn, err := client.New(ctx, serverSocketPath)
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	md := testcfg.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	for _, tc := range []struct {
		desc                  string
		setup                 func(repoProto *gitalypb.Repository, commitID git.ObjectID)
		expectedRegistrations int
		expectedCancellations int
	}{
		{
			desc: "unary accessor",
			setup: func(repoProto *gitalypb.Repository, commitID git.ObjectID) {
				_, err := gitalypb.NewCommitServiceClient(conn).FindCommit(ctx, &gitalypb.FindCommitRequest{
					Repository: repoProto,
					Revision:   []byte("main"),
				})
				require.NoError(t, err)
			},
			expectedRegistrations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.MVCCMigration, 1, 0),
			expectedCancellations: 0,
		},
		{
			desc: "unary mutator",
			setup: func(repoProto *gitalypb.Repository, commitID git.ObjectID) {
				_, err := gitalypb.NewRefServiceClient(conn).DeleteRefs(ctx, &gitalypb.DeleteRefsRequest{
					Repository: repoProto,
					Refs:       [][]byte{[]byte("refs/heads/main")},
				})
				require.NoError(t, err)
			},
			// A mutator cancels any in-flight migration and registers a fresh
			// one once it has succeeded.
			expectedRegistrations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.MVCCMigration, 1, 0),
			expectedCancellations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.MVCCMigration, 1, 0),
		},
		{
			desc: "stream accessor",
			setup: func(repoProto *gitalypb.Repository, commitID git.ObjectID) {
				stream, err := gitalypb.NewRefServiceClient(conn).FindAllBranches(ctx, &gitalypb.FindAllBranchesRequest{
					Repository: repoProto,
				})
				require.NoError(t, err)

				_, err = testhelper.ReceiveAndFold(stream.Recv, func(
					result []*gitalypb.FindAllBranchesResponse_Branch,
					response *gitalypb.FindAllBranchesResponse,
				) []*gitalypb.FindAllBranchesResponse_Branch {
					if response == nil {
						return result
					}
					return append(result, response.GetBranches()...)
				})
				require.NoError(t, err)
			},
			expectedRegistrations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.MVCCMigration, 1, 0),
			expectedCancellations: 0,
		},
		{
			desc: "stream mutator",
			setup: func(repoProto *gitalypb.Repository, commitID git.ObjectID) {
				updater, err := gitalypb.NewRefServiceClient(conn).UpdateReferences(ctx)
				require.NoError(t, err)

				require.NoError(t, updater.Send(&gitalypb.UpdateReferencesRequest{
					Repository: repoProto,
					Updates: []*gitalypb.UpdateReferencesRequest_Update{
						{
							Reference:   []byte("refs/heads/fun"),
							OldObjectId: []byte(gittest.DefaultObjectHash.ZeroOID),
							NewObjectId: []byte(commitID),
						},
					},
				}))

				_, err = updater.CloseAndRecv()
				require.NoError(t, err)
			},
			expectedRegistrations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.MVCCMigration, 1, 0),
			expectedCancellations: testhelper.EnabledOrDisabledFlag(ctx, featureflag.MVCCMigration, 1, 0),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			*counter = countingRegister{}

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

			tc.setup(repoProto, commitID)

			require.Equal(t, tc.expectedRegistrations, counter.registerCount)
			require.Equal(t, tc.expectedCancellations, counter.cancelCount)
		})
	}
}

// TestMigration_endToEnd exercises the full migration through a live Gitaly
// server: a files-backend repository is served over gRPC, the interceptor
// schedules a migration, and afterwards the repository must be on the MVCC
// backend, expose exactly the same references, and still accept reads and
// writes over the API.
func TestMigration_endToEnd(t *testing.T) {
	gittest.SkipWithoutMVCCGit(t)
	testhelper.SkipWithPraefect(t, "we shouldn't migrate with Praefect")
	testhelper.SkipWithReftable(t, "checks migration from the files backend")
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	t.Parallel()

	ctx := featureflag.ContextWithFeatureFlag(
		testhelper.Context(t),
		featureflag.MVCCMigration,
		true,
	)
	cfg := testcfg.Build(t)

	var stopMigrations func()
	callback := func(logger log.Logger, factory localrepo.Factory) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
		handler := migration.NewHandler(logger, migration.NewMetrics(), noopHousekeeping{})
		handler.Run(testhelper.Context(t), noParent)
		stopMigrations = handler.Close
		t.Cleanup(stopMigrations)

		unary, stream := mvcc.NewMigrationInterceptors(factory, protoregistry.GitalyProtoPreregistered, handler)
		return []grpc.UnaryServerInterceptor{unary}, []grpc.StreamServerInterceptor{stream}
	}

	serverSocketPath := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps))
		gitalypb.RegisterOperationServiceServer(srv, operations.NewServer(deps))
	}, testserver.WithInterceptors(callback))
	cfg.SocketPath = serverSocketPath

	conn, err := client.New(ctx, serverSocketPath)
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	refClient := gitalypb.NewRefServiceClient(conn)
	repoClient := gitalypb.NewRepositoryServiceClient(conn)
	commitClient := gitalypb.NewCommitServiceClient(conn)
	opClient := gitalypb.NewOperationServiceClient(conn)

	md := testcfg.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	// Build a repository with a mixture of packed and loose refs, plus a tag
	// and a symbolic HEAD, so the migration has to carry all of them across.
	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	mainCommit := gittest.WriteCommit(
		t, cfg, repoPath, gittest.WithBranch("main"),
		gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "hello"}),
	)
	gittest.WriteCommit(
		t, cfg, repoPath, gittest.WithBranch("packed-branch"), gittest.WithParents(mainCommit),
		gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "packed"}),
	)
	gittest.WriteTag(t, cfg, repoPath, "v1.0.0", mainCommit.Revision())

	// Pack everything written so far.
	gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")

	// Now add a loose ref after packing; the migration must pick it up from
	// the refs/ hierarchy rather than packed-refs.
	looseCommit := gittest.WriteCommit(
		t, cfg, repoPath, gittest.WithBranch("loose-branch"),
		gittest.WithParents(mainCommit),
		gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "loose"}),
	)

	gittest.SetDefaultBranch(t, cfg, repoPath, "main")

	// Snapshot the reference set and the current backend by reading the
	// repository directly rather than over gRPC. Every repository-scoped RPC
	// goes through the interceptor and would itself schedule the migration, so
	// an RPC here would race with the migration this test means to trigger
	// explicitly below.
	refsBefore := gittest.GetReferences(t, cfg, repoPath, gittest.GetReferencesConfig{
		Patterns: []string{"refs/"},
	})
	require.NotEmpty(t, refsBefore)

	backendBefore, err := localrepo.NewTestRepo(t, cfg, repoProto).ReferenceBackend(ctx)
	require.NoError(t, err)
	require.Equal(t, git.ReferenceBackendFiles, backendBefore, "test must start on the files backend")

	// Issue an accessor RPC. The interceptor registers a migration for it.
	_, err = commitClient.FindCommit(ctx, &gitalypb.FindCommitRequest{
		Repository: repoProto,
		Revision:   []byte("main"),
	})
	require.NoError(t, err)

	requireMigrated(t, repoPath)

	// The repository must now report the MVCC backend.
	infoAfter, err := repoClient.RepositoryInfo(ctx, &gitalypb.RepositoryInfoRequest{Repository: repoProto})
	require.NoError(t, err)
	require.Equal(
		t,
		gitalypb.ReferenceBackend_REFERENCE_BACKEND_MVCC,
		infoAfter.GetReferences().GetReferenceBackend(),
	)

	// On-disk layout: the mvcc directory is in place and the files-backend
	// artifacts are gone.
	require.DirExists(t, filepath.Join(repoPath, "mvcc"))
	require.NoFileExists(t, filepath.Join(repoPath, "packed-refs"))

	cfgBytes, err := os.ReadFile(filepath.Join(repoPath, "config"))
	require.NoError(t, err)
	require.Contains(t, string(cfgBytes), "refstorage = mvcc")
	require.Contains(t, string(cfgBytes), "repositoryformatversion = 1")

	// The reference set must be byte-for-byte identical, including the tag and
	// the loose branch written after packing.
	refsAfter := gittest.GetReferencesAPI(t, ctx, refClient, repoProto, [][]byte{[]byte("refs/")})
	require.Equal(t, refsBefore, refsAfter, "reference set must survive the migration")

	// HEAD must still resolve to main.
	head, err := refClient.FindDefaultBranchName(ctx, &gitalypb.FindDefaultBranchNameRequest{
		Repository: repoProto,
	})
	require.NoError(t, err)
	require.Equal(t, "refs/heads/main", string(head.GetName()))

	// Reads still work and return the right objects.
	foundLoose, err := commitClient.FindCommit(ctx, &gitalypb.FindCommitRequest{
		Repository: repoProto,
		Revision:   []byte("loose-branch"),
	})
	require.NoError(t, err)
	require.Equal(t, looseCommit.String(), foundLoose.GetCommit().GetId())

	// Writes still work: create a branch through the API on the migrated repo.
	createResp, err := opClient.UserCreateBranch(ctx, &gitalypb.UserCreateBranchRequest{
		Repository: repoProto,
		BranchName: []byte("post-migration"),
		StartPoint: []byte("main"),
		User:       gittest.TestUser,
	})
	require.NoError(t, err)
	require.Equal(t, mainCommit.String(), createResp.GetBranch().GetTargetCommit().GetId())

	// ... and the new branch is visible afterwards.
	refsFinal := gittest.GetReferencesAPI(t, ctx, refClient, repoProto, [][]byte{[]byte("refs/")})
	require.Contains(t, refNames(refsFinal), git.ReferenceName("refs/heads/post-migration"))

	// Deleting it again also works, proving the write path is fully functional.
	_, err = opClient.UserDeleteBranch(ctx, &gitalypb.UserDeleteBranchRequest{
		Repository: repoProto,
		BranchName: []byte("post-migration"),
		User:       gittest.TestUser,
	})
	require.NoError(t, err)

	require.NotContains(
		t,
		refNames(gittest.GetReferencesAPI(t, ctx, refClient, repoProto, [][]byte{[]byte("refs/")})),
		git.ReferenceName("refs/heads/post-migration"),
	)
}

// TestMigration_writesWhileMigrating runs writes against a repository while
// migrations keep being scheduled, and asserts that no reference is lost regardless of
// which side wins the race.
func TestMigration_writesWhileMigrating(t *testing.T) {
	gittest.SkipWithoutMVCCGit(t)
	testhelper.SkipWithPraefect(t, "we shouldn't migrate with Praefect")
	testhelper.SkipWithReftable(t, "checks migration from the files backend")
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	t.Parallel()

	ctx := featureflag.ContextWithFeatureFlag(
		testhelper.Context(t),
		featureflag.MVCCMigration,
		true,
	)
	cfg := testcfg.Build(t)

	var stopMigrations func()
	callback := func(logger log.Logger, factory localrepo.Factory) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
		handler := migration.NewHandler(logger, migration.NewMetrics(), noopHousekeeping{})
		handler.Run(testhelper.Context(t), noParent)
		stopMigrations = handler.Close
		t.Cleanup(stopMigrations)

		unary, stream := mvcc.NewMigrationInterceptors(factory, protoregistry.GitalyProtoPreregistered, handler)
		return []grpc.UnaryServerInterceptor{unary}, []grpc.StreamServerInterceptor{stream}
	}

	serverSocketPath := testserver.RunGitalyServer(t, cfg, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(deps))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps))
		gitalypb.RegisterOperationServiceServer(srv, operations.NewServer(deps))
	}, testserver.WithInterceptors(callback))
	cfg.SocketPath = serverSocketPath

	conn, err := client.New(ctx, serverSocketPath)
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	opClient := gitalypb.NewOperationServiceClient(conn)

	md := testcfg.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(
		t, cfg, repoPath, gittest.WithBranch("main"),
		gittest.WithTreeEntries(gittest.TreeEntry{Mode: "100644", Path: "README", Content: "hello"}),
	)
	gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
	gittest.SetDefaultBranch(t, cfg, repoPath, "main")

	// Interleave branch creations with migration attempts. Each mutator RPC
	// cancels any in-flight migration and schedules a fresh one afterwards, so
	// this drives the cancel/reschedule path repeatedly.
	const branches = 8
	for i := range branches {
		name := "concurrent-" + string(rune('a'+i))

		_, err := opClient.UserCreateBranch(ctx, &gitalypb.UserCreateBranchRequest{
			Repository: repoProto,
			BranchName: []byte(name),
			StartPoint: []byte("main"),
			User:       gittest.TestUser,
		})
		require.NoError(t, err, "branch %q must be created regardless of migration state", name)
	}

	// Stopping waits for a running migration to return, so nothing changes the
	// repository while it is read below.
	stopMigrations()

	// Every branch we created must be present, whether it was written to the
	// files backend before the swap or to MVCC afterwards.
	names := refNames(gittest.GetReferences(t, cfg, repoPath, gittest.GetReferencesConfig{Patterns: []string{"refs/"}}))
	for i := range branches {
		name := git.ReferenceName("refs/heads/concurrent-" + string(rune('a'+i)))
		require.Contains(t, names, name, "no reference may be lost across the migration")
	}
	require.Contains(t, names, git.ReferenceName("refs/heads/main"))
}

func refNames(refs []git.Reference) []git.ReferenceName {
	names := make([]git.ReferenceName, 0, len(refs))
	for _, r := range refs {
		names = append(names, r.Name)
	}
	return names
}
