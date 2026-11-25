package migration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/v19/internal/dontpanic"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/localrepo"
	gitmvcc "gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
)

// repoKey identifies a repository for migration state and locking. Relative
// path alone is not unique across storages, so both must be part of the key.
type repoKey struct {
	storage, relativePath string
}

func repoKeyFor(repo *localrepo.Repo) repoKey {
	return repoKey{storage: repo.GetStorageName(), relativePath: repo.GetRelativePath()}
}

// ErrBlockTimeout is returned by a block callback that couldn't take the write
// lock in time. The repository is busy, so it backs off like any failure.
var ErrBlockTimeout = errors.New("timed out acquiring write lock")

// errHousekeepingRunning reports that the migration didn't start because
// housekeeping was running on the repository.
var errHousekeepingRunning = errors.New("housekeeping is running")

// HousekeepingLocker keeps housekeeping off a repository while it is migrated:
// housekeeping reads every reference, and must not do so while the swap
// changes where they are stored.
type HousekeepingLocker interface {
	TryLockRepository(storage.Repository) (func(), bool)
}

type migratorState struct {
	completed bool
	attempts  uint
	coolDown  time.Time
	cancelCtx context.CancelFunc
}

type migrationData struct {
	ctx     context.Context
	repo    *localrepo.Repo
	block   func(context.Context) error
	unblock func() error
}

// Handler migrates repositories to the MVCC backend one at a time, in
// the background.
type Handler struct {
	wg sync.WaitGroup

	ctx       context.Context
	ctxCancel context.CancelFunc

	logger       log.Logger
	metrics      Metrics
	housekeeping HousekeepingLocker

	migrateCh chan migrationData
	// state holds one migratorState per repository offered for migration.
	// Entries for repositories that turn out not to exist are dropped.
	state sync.Map
}

// NewHandler provides a new migration handler.
func NewHandler(logger log.Logger, metrics Metrics, housekeeping HousekeepingLocker) *Handler {
	ctx, cancel := context.WithCancel(context.Background())

	return &Handler{
		ctx:       ctx,
		ctxCancel: cancel,

		logger:       logger,
		metrics:      metrics,
		housekeeping: housekeeping,

		migrateCh: make(chan migrationData),
		state:     sync.Map{},
	}
}

// Run creates a long running goroutine which handles all migration requests.
// Must be accompanied with a Close(), which will safely stop the goroutine.
//
// The repository locks are per process, so migrations must not overlap with
// another Gitaly process serving RPCs during a graceful upgrade. They only start
// once waitForParent returns, and the caller must Close the handler before this
// process spawns a child.
func (m *Handler) Run(ctx context.Context, waitForParent func(context.Context) error) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		// Close cancels m.ctx, which stops the wait.
		if err := waitForParent(m.ctx); err != nil && m.ctx.Err() == nil {
			// The parent is gone, it just didn't shut down cleanly.
			m.logger.WithError(err).WarnContext(ctx, "parent process exited with an error")
		} else if m.ctx.Err() != nil {
			return
		}

		for {
			select {
			case <-m.ctx.Done():
				return
			case data := <-m.migrateCh:
				// A panic must not take down this long-lived goroutine.
				dontpanic.Try(m.logger, func() { m.handle(data) })
			}
		}
	}()
}

func (m *Handler) handle(data migrationData) {
	if data.repo == nil || data.block == nil || data.unblock == nil {
		return
	}

	// The migration outlives the RPC that asked for it, but keeps its values,
	// such as feature flags and the correlation ID. The RPC's MVCC cache is
	// dropped, as it must not be used past the RPC.
	ctx := gitmvcc.ContextWithoutCache(context.WithoutCancel(data.ctx))

	// Since we only run a single migration at a time, add a safety deadline.
	ctx, deadlineCancel := context.WithDeadline(ctx, time.Now().Add(time.Hour))
	defer deadlineCancel()

	// We want the middleware to cancel any ongoing migration when a mutator RPC
	// comes along
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(m.ctx, cancel)()

	key := repoKeyFor(data.repo)

	// Store the new cancel before any other work so CancelMigration always
	// sees it, even if it races the LoadOrStore-to-Store window.
	var state migratorState
	if val, ok := m.state.Load(key); ok {
		state = val.(migratorState)
	}
	state.cancelCtx = cancel
	m.state.Store(key, state)

	// forget removes the entry instead of storing it back, for repositories that
	// turned out not to exist, so the map stays proportional to existing repos.
	forget := false
	defer func() {
		if forget {
			m.state.Delete(key)
			return
		}
		m.state.Store(key, state)
	}()

	if state.completed || state.coolDown.After(time.Now()) {
		state.cancelCtx = nil
		cancel()
		return
	}

	before := time.Now()
	var stats migrationStats
	var err error
	if unlockHousekeeping, ok := m.housekeeping.TryLockRepository(data.repo); ok {
		func() {
			defer unlockHousekeeping()
			stats, err = migrate(ctx, m.logger, data.repo, func() error { return data.block(ctx) }, data.unblock)
		}()
	} else {
		err = errHousekeepingRunning
	}
	// A canceled attempt fails with whatever error its killed git process
	// produced, so record the cancellation itself.
	if err != nil && ctx.Err() != nil {
		err = fmt.Errorf("%w: %w", ctx.Err(), err)
	}
	cancel()

	if errors.Is(err, storage.ErrRepositoryNotFound) {
		state.cancelCtx = nil
		forget = true
		return
	}

	// A concurrent pack-refs resolves itself: skip without counting an attempt
	// or applying backoff.
	if errors.Is(err, errPackedRefsModified) {
		state.cancelCtx = nil
		return
	}

	// Already on MVCC: nothing ran, so record no latency or success. This is
	// also how an already-migrated repository converges to completed.
	if errors.Is(err, errAlreadyMigrated) {
		state.cancelCtx = nil
		state.completed = true
		return
	}

	// Object pools and their members are permanently skipped: the MVCC backend
	// does not yet support the alternates relationship, and pool membership is
	// long-lived, so there is no point in retrying.
	if errors.Is(err, errPooledRepository) {
		state.cancelCtx = nil
		state.completed = true
		m.logger.WithFields(log.Fields{
			"relative_path": key.relativePath,
			"storage_name":  key.storage,
		}).WarnContext(ctx, "MVCC migration skipped: repository is part of an object pool")
		return
	}

	latency := time.Since(before)
	state.attempts = state.attempts + 1
	state.cancelCtx = nil

	logFields := log.Fields{
		"relative_path":             key.relativePath,
		"storage_name":              key.storage,
		"mvcc.migration_latency_ms": latency.Milliseconds(),
		"mvcc.migration_attempts":   state.attempts,
		"mvcc.packed_refs_bytes":    stats.packedRefsSize,
		"mvcc.loose_refs_count":     stats.numLooseRefs,
		"mvcc.write_blocked_ms":     stats.writeBlocked.Milliseconds(),
	}

	switch {
	case errors.Is(err, errPostSwapCleanup):
		// The migration succeeded; only the cleanup step didn't. Don't count it
		// as a failure and apply backoff to a repository that is already done.
		// A failed swap can leave refs/ missing though, which fails every RPC
		// until the next attempt repairs it, so the repository is only done
		// once refs/ exists.
		_, pathErr := data.repo.Path(context.WithoutCancel(ctx))
		state.completed = pathErr == nil
		m.logger.WithError(err).WithFields(logFields).
			WarnContext(ctx, "MVCC migration succeeded but post-swap cleanup failed")
		m.metrics.observeSuccess(latency, stats)
	case err != nil:
		logger := m.logger.WithError(err).WithFields(logFields)
		// A busy repository is expected rather than an error, though it still
		// backs off.
		switch {
		case errors.Is(err, context.Canceled):
			logger.WarnContext(ctx, "MVCC migration canceled for repository")
		case errors.Is(err, ErrBlockTimeout), errors.Is(err, errHousekeepingRunning), errors.Is(err, errReftableModified):
			logger.WarnContext(ctx, "MVCC migration postponed for busy repository")
		default:
			logger.ErrorContext(ctx, "MVCC migration failed for repository")
		}
		m.metrics.failsMetric.WithLabelValues(failMetricReason(err)).Add(1)

		state.coolDown = time.Now().Add(coolDown(state.attempts))
	default:
		m.logger.WithFields(logFields).
			InfoContext(ctx, "MVCC migration successful for repository")
		m.metrics.observeSuccess(latency, stats)

		state.completed = true
	}
}

// coolDown is how long to wait after the given number of failed attempts. A
// failure is often a busy repository whose writes keep canceling the
// migration, so back off from 15 minutes, doubling up to 6 hours, rather than
// keep interrupting it.
func coolDown(attempts uint) time.Duration {
	return min(15*time.Minute<<min(attempts-1, 5), 6*time.Hour)
}

// Close is used to stop the handler.
func (m *Handler) Close() {
	defer m.wg.Wait()
	m.ctxCancel()
}

// RegisterMigration is used to register a new migration. This function
// is non-blocking and doesn't return an error. The migration keeps the values
// of ctx, but not its cancellation.
func (m *Handler) RegisterMigration(
	ctx context.Context,
	repo *localrepo.Repo,
	block func(context.Context) error,
	unblock func() error,
) {
	select {
	case m.migrateCh <- migrationData{ctx: ctx, repo: repo, block: block, unblock: unblock}:
	default:
		return
	}
}

// CancelMigration cancels the ongoing migration for the given repository. There
// is a brief window between a RegisterMigration send and handle() writing the
// cancel into the state map; a CancelMigration arriving in that window finds
// either no entry or a nil cancelCtx and is therefore a no-op. The migration
// will proceed, complete or time out, and the caller's write will eventually
// unblock.
func (m *Handler) CancelMigration(repo *localrepo.Repo) {
	val, ok := m.state.Load(repoKeyFor(repo))
	if !ok {
		return
	}

	if cancel := val.(migratorState).cancelCtx; cancel != nil {
		cancel()
	}
}
