package mvcc

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/mvcc/migration"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
)

// countingMigrator is a migrationRegister that counts calls and keeps the
// last registered migration.
type countingMigrator struct {
	registerCount int
	cancelCount   int
	block         func(context.Context) error
	unblock       func() error
}

func (c *countingMigrator) RegisterMigration(_ context.Context, _ *localrepo.Repo, block func(context.Context) error, unblock func() error) {
	c.registerCount++
	c.block, c.unblock = block, unblock
}

func (c *countingMigrator) CancelMigration(_ *localrepo.Repo) {
	c.cancelCount++
}

func TestMigrationInterceptor_praefectRepoSkipped(t *testing.T) {
	t.Parallel()

	ctx := featureflag.ContextWithFeatureFlag(testhelper.Context(t), featureflag.MVCCMigration, true)
	counter := &countingMigrator{}
	unaryInterceptor, _ := NewMigrationInterceptors(localrepo.Factory{}, protoregistry.GitalyProtoPreregistered, counter)

	handlerCalled := false
	_, err := unaryInterceptor(ctx,
		&gitalypb.FindCommitRequest{Repository: &gitalypb.Repository{
			StorageName:  "default",
			RelativePath: storage.PraefectRootPathPrefix + "/repositories/ab/cd/1",
		}},
		&grpc.UnaryServerInfo{FullMethod: gitalypb.CommitService_FindCommit_FullMethodName},
		func(context.Context, any) (any, error) {
			handlerCalled = true
			return nil, nil
		},
	)
	require.NoError(t, err)
	require.True(t, handlerCalled)
	require.Zero(t, counter.registerCount)
	require.Zero(t, counter.cancelCount)
}

// TestMigrationInterceptor_lockHeldWhateverTheFlag checks that an RPC holds the
// repository's read lock even when it doesn't carry the feature flag, so a
// migration triggered by another RPC can't swap the backend underneath it.
func TestMigrationInterceptor_lockHeldWhateverTheFlag(t *testing.T) {
	t.Parallel()

	repo := &gitalypb.Repository{StorageName: "default", RelativePath: "repo.git"}
	info := &grpc.UnaryServerInfo{FullMethod: gitalypb.CommitService_FindCommit_FullMethodName}
	req := &gitalypb.FindCommitRequest{Repository: repo}

	counter := &countingMigrator{}
	unaryInterceptor, _ := NewMigrationInterceptors(localrepo.Factory{}, protoregistry.GitalyProtoPreregistered, counter)

	// An accessor with the flag registers a migration.
	enabled := featureflag.ContextWithFeatureFlag(testhelper.Context(t), featureflag.MVCCMigration, true)
	_, err := unaryInterceptor(enabled, req, info, func(context.Context, any) (any, error) { return nil, nil })
	require.NoError(t, err)
	require.Equal(t, 1, counter.registerCount)

	// While an RPC without the flag runs, the migration can't take the lock.
	disabled := featureflag.ContextWithFeatureFlag(testhelper.Context(t), featureflag.MVCCMigration, false)
	_, err = unaryInterceptor(disabled, req, info, func(context.Context, any) (any, error) {
		ctx, cancel := context.WithTimeout(testhelper.Context(t), 50*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, counter.block(ctx), migration.ErrBlockTimeout)
		return nil, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, counter.registerCount, "RPCs without the flag must not register migrations")

	// Once it's done, the migration takes the lock.
	require.NoError(t, counter.block(testhelper.Context(t)))
	require.NoError(t, counter.unblock())
}

func TestMigrationInterceptor_removeRepositoryDoesNotReregister(t *testing.T) {
	t.Parallel()

	ctx := featureflag.ContextWithFeatureFlag(testhelper.Context(t), featureflag.MVCCMigration, true)
	counter := &countingMigrator{}
	unaryInterceptor, _ := NewMigrationInterceptors(localrepo.Factory{}, protoregistry.GitalyProtoPreregistered, counter)

	_, err := unaryInterceptor(ctx,
		&gitalypb.RemoveRepositoryRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: "repo.git"}},
		&grpc.UnaryServerInfo{FullMethod: gitalypb.RepositoryService_RemoveRepository_FullMethodName},
		func(context.Context, any) (any, error) { return nil, nil },
	)
	require.NoError(t, err)
	require.Equal(t, 1, counter.cancelCount)
	require.Zero(t, counter.registerCount)
}

// TestMigrationInterceptor_maintenanceReregisters checks that RPCs other than
// accessors and mutators also register a migration again once they succeed.
func TestMigrationInterceptor_maintenanceReregisters(t *testing.T) {
	t.Parallel()

	ctx := featureflag.ContextWithFeatureFlag(testhelper.Context(t), featureflag.MVCCMigration, true)
	req := &gitalypb.OptimizeRepositoryRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: "repo.git"}}
	info := &grpc.UnaryServerInfo{FullMethod: gitalypb.RepositoryService_OptimizeRepository_FullMethodName}

	for _, tc := range []struct {
		desc                  string
		handlerErr            error
		expectedRegistrations int
	}{
		{desc: "successful", expectedRegistrations: 1},
		{desc: "failed", handlerErr: errors.New("failed"), expectedRegistrations: 0},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			counter := &countingMigrator{}
			unaryInterceptor, _ := NewMigrationInterceptors(localrepo.Factory{}, protoregistry.GitalyProtoPreregistered, counter)

			_, err := unaryInterceptor(ctx, req, info, func(context.Context, any) (any, error) {
				require.Zero(t, counter.registerCount, "the migration must only be registered once the RPC is done")
				return nil, tc.handlerErr
			})
			require.Equal(t, tc.handlerErr, err)
			require.Equal(t, 1, counter.cancelCount)
			require.Equal(t, tc.expectedRegistrations, counter.registerCount)
		})
	}
}

func TestLockForMigration(t *testing.T) {
	t.Parallel()

	t.Run("acquires when unused", func(t *testing.T) {
		t.Parallel()

		sem := semaphore.NewWeighted(math.MaxInt64)
		require.NoError(t, lockForMigration(testhelper.Context(t), sem, time.Minute))
		require.False(t, sem.TryAcquire(1), "the lock must be held in full")
	})

	t.Run("doesn't block readers while it waits", func(t *testing.T) {
		t.Parallel()

		sem := semaphore.NewWeighted(math.MaxInt64)
		require.True(t, sem.TryAcquire(1))

		waiting := make(chan error)
		go func() { waiting <- lockForMigration(testhelper.Context(t), sem, 200*time.Millisecond) }()

		// A queued full acquisition would make this fail until it gave up.
		time.Sleep(20 * time.Millisecond)
		require.True(t, sem.TryAcquire(1), "a reader must not wait behind a pending migration")
		sem.Release(1)

		require.ErrorIs(t, <-waiting, migration.ErrBlockTimeout)
		sem.Release(1)
	})

	t.Run("acquires once readers are done", func(t *testing.T) {
		t.Parallel()

		sem := semaphore.NewWeighted(math.MaxInt64)
		require.True(t, sem.TryAcquire(1))
		time.AfterFunc(20*time.Millisecond, func() { sem.Release(1) })

		require.NoError(t, lockForMigration(testhelper.Context(t), sem, time.Minute))
	})

	t.Run("stops once the context is canceled", func(t *testing.T) {
		t.Parallel()

		sem := semaphore.NewWeighted(math.MaxInt64)
		require.True(t, sem.TryAcquire(1))

		ctx, cancel := context.WithCancel(testhelper.Context(t))
		cancel()
		require.ErrorIs(t, lockForMigration(ctx, sem, time.Minute), context.Canceled)
	})
}

func TestRepositoryLocks(t *testing.T) {
	t.Parallel()

	lockCache := &repositoryLocks{locks: map[repoKey]*repositoryLock{}}
	key := repoKey{storage: "default", relativePath: "repo.git"}

	first, dropFirst := lockCache.fetch(key)
	second, dropSecond := lockCache.fetch(key)
	require.Same(t, first, second, "callers for the same repository share a lock")

	dropFirst()
	require.Len(t, lockCache.locks, 1, "the lock is kept while it is referenced")
	dropSecond()
	require.Empty(t, lockCache.locks, "an unused lock is dropped")
}

// TestMigrationInterceptor_noTargetRepository verifies that requests which carry no
// target repository are relayed to the handler instead of being rejected.
// Storage-scoped RPCs have no target_repository annotation at all, and
// repository-scoped ones may arrive with the field unset; neither is this
// opportunistic middleware's to fail.
func TestMigrationInterceptor_noTargetRepository(t *testing.T) {
	t.Parallel()

	ctx := featureflag.ContextWithFeatureFlag(
		testhelper.Context(t),
		featureflag.MVCCMigration,
		true,
	)

	for _, tc := range []struct {
		desc       string
		fullMethod string
		req        any
	}{
		{
			desc: "storage-scoped RPC has no target repository",
			// WalkRepos is storage-scoped: the request type has no
			// target_repository field for TargetRepo to find.
			fullMethod: "/gitaly.InternalGitaly/WalkRepos",
			req:        &gitalypb.WalkReposRequest{StorageName: "default"},
		},
		{
			desc: "repository-scoped RPC with the repository unset",
			// The handler must be the one to reject this with a proper
			// InvalidArgument, not the middleware with a generic error.
			fullMethod: "/gitaly.CommitService/FindCommit",
			req:        &gitalypb.FindCommitRequest{},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			counter := &countingMigrator{}
			unaryInterceptor, _ := NewMigrationInterceptors(localrepo.Factory{}, protoregistry.GitalyProtoPreregistered, counter)

			handlerCalled := false
			_, err := unaryInterceptor(
				ctx, tc.req, &grpc.UnaryServerInfo{FullMethod: tc.fullMethod},
				func(_ context.Context, _ any) (any, error) {
					handlerCalled = true
					return nil, nil
				},
			)
			require.NoError(t, err)
			require.True(t, handlerCalled, "handler must be called")
			require.Equal(t, 0, counter.registerCount)
			require.Equal(t, 0, counter.cancelCount)
		})
	}
}

func TestMigrationInterceptor_quarantineNotCarriedIntoMigration(t *testing.T) {
	t.Parallel()

	ctx := featureflag.ContextWithFeatureFlag(testhelper.Context(t), featureflag.MVCCMigration, true)

	register := &captureRepo{}
	unaryInterceptor, _ := NewMigrationInterceptors(localrepo.Factory{}, protoregistry.GitalyProtoPreregistered, register)

	req := &gitalypb.FindCommitRequest{Repository: &gitalypb.Repository{
		StorageName:        "default",
		RelativePath:       "repo.git",
		GitObjectDirectory: "objects/incoming-quarantine",
	}}
	_, _ = unaryInterceptor(ctx, req,
		&grpc.UnaryServerInfo{FullMethod: gitalypb.CommitService_FindCommit_FullMethodName},
		func(context.Context, any) (any, error) { return nil, nil })

	require.Equal(t, 1, register.registerCount, "a migration must have been registered")
	require.Empty(t, register.repo.GetGitObjectDirectory(), "quarantine directory must be stripped")
}

type captureRepo struct {
	countingMigrator
	repo *gitalypb.Repository
}

func (c *captureRepo) RegisterMigration(ctx context.Context, repo *localrepo.Repo, block func(context.Context) error, unblock func() error) {
	c.countingMigrator.RegisterMigration(ctx, repo, block, unblock)
	c.repo = repo.Repository.(*gitalypb.Repository)
}
