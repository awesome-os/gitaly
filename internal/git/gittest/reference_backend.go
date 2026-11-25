package gittest

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

// DefaultReferenceBackend is the reference backend that test repositories use.
var DefaultReferenceBackend = func() git.ReferenceBackend {
	if testhelper.IsMVCCEnabled() {
		return git.ReferenceBackendMVCC
	}

	return git.ReferenceBackendFiles
}()

// FilesOrReftables returns the files or reftable value based on which reference
// backend is currently being used. The MVCC backend stores its references in
// reftables, so it gets the reftable value. That is right for what most callers
// assert, like having no loose references, but not for on-disk paths: MVCC keeps
// its tables in the manifest, not in reftable/tables.list.
func FilesOrReftables[T any](files, reftable T) T {
	if DefaultReferenceBackend == git.ReferenceBackendFiles {
		return files
	}
	return reftable
}

// SkipWithoutMVCCGit skips the test when the bundled MVCC Git binary is not
// available. This happens when GITALY_TESTING_GIT_BINARY overrides the Git
// binary, for example in the nightly tests.
func SkipWithoutMVCCGit(tb testing.TB) {
	if os.Getenv("GITALY_TESTING_GIT_BINARY") != "" {
		tb.Skip("the MVCC reference backend requires the bundled MVCC Git binary, which is disabled by the GITALY_TESTING_GIT_BINARY override")
	}
}

// MVCCCacheContext returns ctx carrying a prepared read-only MVCC cache when repo uses the MVCC
// reference backend, like the MVCC middleware sets up for an RPC. Code that reads the pinned
// manifest, such as repository statistics, needs it. Other repositories get ctx unchanged.
//
// repo may be the repository as a client of Gitaly sees it. With Praefect in front of Gitaly, it is
// then rewritten to the replica's path, like localrepo.NewTestRepo does.
func MVCCCacheContext(tb testing.TB, ctx context.Context, cfg config.Cfg, repo storage.Repository) context.Context {
	tb.Helper()

	locator := config.NewLocator(cfg)
	repoPath, err := locator.GetRepoPath(ctx, repo, storage.WithRepositoryVerificationSkipped())
	require.NoError(tb, err)

	if !isRepository(repoPath) && cfg.SocketPath != testcfg.UnconfiguredSocketPath {
		repo = RewrittenRepository(tb, ctx, cfg, &gitalypb.Repository{
			StorageName:  repo.GetStorageName(),
			RelativePath: repo.GetRelativePath(),
		})
		repoPath, err = locator.GetRepoPath(ctx, repo, storage.WithRepositoryVerificationSkipped())
		require.NoError(tb, err)
	}

	backend, err := gitcmd.DetectReferenceBackend(ctx, repoPath)
	require.NoError(tb, err)
	if backend.Name != git.ReferenceBackendMVCC.Name {
		return ctx
	}

	cache, err := mvcc.NewCacheFactory(cfg.RuntimeDir, locator).NewCache(ctx, repo.GetStorageName(), repo.GetRelativePath(), true)
	require.NoError(tb, err)
	tb.Cleanup(cache.Cleanup)

	cache.Prepare(ctx)
	require.NoError(tb, cache.WaitUntilReady(ctx))

	return gitcmd.ContextWithMVCC(mvcc.ContextWithCache(ctx, cache))
}

// ContextWithDefaultMVCC returns ctx carrying the MVCC marker when test repositories use the MVCC
// backends, like the MVCC middleware marks the context of an RPC on such a repository.
func ContextWithDefaultMVCC(ctx context.Context) context.Context {
	if DefaultReferenceBackend.Name != git.ReferenceBackendMVCC.Name {
		return ctx
	}

	return gitcmd.ContextWithMVCC(ctx)
}

// contextWithDetectedMVCC returns ctx carrying the MVCC marker if the repository at repoPath uses
// MVCC, like the MVCC middleware marks it in production. Other repositories get ctx unchanged.
func contextWithDetectedMVCC(ctx context.Context, repoPath string) context.Context {
	backend, err := gitcmd.DetectReferenceBackend(ctx, repoPath)
	if err != nil || backend.Name != git.ReferenceBackendMVCC.Name {
		return ctx
	}

	return gitcmd.ContextWithMVCC(ctx)
}
