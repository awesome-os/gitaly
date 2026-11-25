package mvcc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/v19/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/mvcc/migration"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v19/middleware"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// repoKey identifies a repository's lock. Relative path alone is not unique
// across storages, so both are part of the key.
type repoKey struct {
	storage, relativePath string
}

type migrationRegister interface {
	RegisterMigration(ctx context.Context, repo *localrepo.Repo, block func(context.Context) error, unblock func() error)
	CancelMigration(repo *localrepo.Repo)
}

// blockTimeout bounds how long block() tries to take the write lock, so a
// long-running reader can't stall the migration goroutine indefinitely.
const blockTimeout = 5 * time.Second

// blockPollInterval is how often block() tries to take the write lock.
const blockPollInterval = 10 * time.Millisecond

// repositoryLock is held with weight one by every RPC on the repository and in
// full by a migration, which makes the two mutually exclusive. A semaphore
// rather than a sync.RWMutex, as RPCs waiting for it must respect their
// context.
type repositoryLock struct {
	sem *semaphore.Weighted
	// references counts the callers using this lock. It is only touched while
	// holding the registry's lock, which lets the registry drop unused locks.
	references int
}

// repositoryLocks hands out one lock per repository and drops locks once they
// are unused, so it doesn't grow with every repository ever served.
type repositoryLocks struct {
	mu    sync.Mutex
	locks map[repoKey]*repositoryLock
}

// fetch returns the lock for the repository and a function that gives up the
// reference to it. The reference must only be given up after the lock has been
// released, or two callers could end up holding different locks.
func (r *repositoryLocks) fetch(key repoKey) (*repositoryLock, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()

	lock, ok := r.locks[key]
	if !ok {
		lock = &repositoryLock{sem: semaphore.NewWeighted(math.MaxInt64)}
		r.locks[key] = lock
	}
	lock.references++

	return lock, sync.OnceFunc(func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		lock.references--
		if lock.references == 0 {
			delete(r.locks, key)
		}
	})
}

// lockForMigration takes the repository's write lock, giving up after timeout
// or once ctx is done. It never waits in the semaphore's queue: a queued full
// acquisition makes every RPC arriving behind it wait too, which would let one
// long-running RPC stall all others on the repository. That includes calls
// back into Gitaly made from within an RPC, such as a push's access checks,
// which would then deadlock against the push.
func lockForMigration(ctx context.Context, sem *semaphore.Weighted, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(blockPollInterval)
	defer ticker.Stop()

	for !sem.TryAcquire(math.MaxInt64) {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return migration.ErrBlockTimeout
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}

	return nil
}

// NewMigrationInterceptors provides opportunistic middlewares to aid in MVCC
// migration. Every RPC on a repository holds its read lock, so a migration can
// swap the backend between RPCs. With the feature flag enabled, accessors also register
// a migration, while other RPCs cancel any ongoing one and register one again
// once they succeed. Praefect-managed repositories are skipped, since Praefect
// coordinates its own replication.
//
// For streaming RPCs the first request is consumed and re-wrapped so the next
// handler doesn't miss it.
func NewMigrationInterceptors(
	factory localrepo.Factory,
	registry *protoregistry.Registry,
	register migrationRegister,
) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	lockCache := &repositoryLocks{locks: map[repoKey]*repositoryLock{}}

	// intercept takes the repository's read lock and schedules or cancels a
	// migration, and returns a function to call once the RPC is done.
	intercept := func(ctx context.Context, methodInfo protoregistry.MethodInfo, req proto.Message, fullMethod string) (func(error), error) {
		targetRepo, err := methodInfo.TargetRepo(req)
		if err != nil || storage.IsPraefectRepository(targetRepo) {
			// Requests without a target repository are left for the handler
			// to validate, and Praefect-managed repositories aren't migrated.
			return func(error) {}, nil
		}

		key := repoKey{storage: targetRepo.GetStorageName(), relativePath: targetRepo.GetRelativePath()}

		// Without the flag, an RPC still takes the lock, as a migration
		// triggered by another RPC must not swap the backend underneath it.
		registerMigration := func() {}
		reregister := false
		if featureflag.MVCCMigration.IsEnabled(ctx) {
			// Build from storage and path only, not from the full request
			// proto: a push carries a quarantine object directory that disappears
			// once the push ends, and git commands run after that would fail.
			repo := factory.Build(&gitalypb.Repository{
				StorageName:  targetRepo.GetStorageName(),
				RelativePath: targetRepo.GetRelativePath(),
			})
			registerMigration = func() {
				// The reference is only taken once the migration runs: a
				// registration can be dropped or skipped, and would then
				// never give it back.
				var migrationLock *repositoryLock
				var dropMigrationReference func()
				block := func(ctx context.Context) error {
					migrationLock, dropMigrationReference = lockCache.fetch(key)
					if err := lockForMigration(ctx, migrationLock.sem, blockTimeout); err != nil {
						dropMigrationReference()
						return err
					}
					return nil
				}
				unblock := func() error {
					migrationLock.sem.Release(math.MaxInt64)
					dropMigrationReference()
					return nil
				}
				register.RegisterMigration(ctx, repo, block, unblock)
			}

			// Both happen before waiting for the lock, so that a running
			// migration is canceled rather than waited for.
			switch methodInfo.Operation {
			case protoregistry.OpAccessor:
				registerMigration()
			default:
				// Registering up front would only have this RPC cancel it, so
				// it registers once it's done instead. A failed RPC changed
				// nothing, and there's nothing left to migrate after a
				// repository was removed.
				register.CancelMigration(repo)
				reregister = fullMethod != gitalypb.RepositoryService_RemoveRepository_FullMethodName
			}
		}

		lock, dropReference := lockCache.fetch(key)
		if err := lock.sem.Acquire(ctx, 1); err != nil {
			dropReference()
			return nil, fmt.Errorf("acquiring repository lock: %w", err)
		}

		return func(rpcErr error) {
			lock.sem.Release(1)
			dropReference()
			if reregister && rpcErr == nil {
				registerMigration()
			}
		}, nil
	}

	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ any, returnedErr error) {
		methodInfo, err := registry.LookupMethod(info.FullMethod)
		if err != nil {
			return handler(ctx, req)
		}

		done, err := intercept(ctx, methodInfo, req.(proto.Message), info.FullMethod)
		if err != nil {
			return nil, err
		}
		defer func() { done(returnedErr) }()

		return handler(ctx, req)
	}

	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (returnedErr error) {
		methodInfo, err := registry.LookupMethod(info.FullMethod)
		if err != nil {
			return handler(srv, ss)
		}

		req := methodInfo.NewRequest()
		if err := ss.RecvMsg(req); err != nil {
			return handler(srv, middleware.NewPeekedStream(ss.Context(), nil, err, ss))
		}

		done, err := intercept(ss.Context(), methodInfo, req, info.FullMethod)
		if err != nil {
			return err
		}
		defer func() { done(returnedErr) }()

		return handler(srv, middleware.NewPeekedStream(ss.Context(), req, nil, ss))
	}

	return unary, stream
}
