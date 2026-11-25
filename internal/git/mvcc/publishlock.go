package mvcc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
)

// PublishSerialized durably publishes a mutator's write while serializing only
// the head advance against other publishes to the same repository. It is the
// entry point the mvcc middleware uses to commit a write, so all writers to a
// repository queue on the same per-repository lock instead of racing the
// generation log.
//
// It runs in two phases. First it uploads the staged artifacts to object
// storage WITHOUT the lock: uploads are content-addressed and de-duplicated, do
// not advance the durable pointer, and so proceed in parallel even for the same
// repository. Serializing this heavy, parallelizable phase would be a throughput
// regression. Then it takes the per-repository publish lock and advances the
// durable head under it via publishWithRetry.
//
// The durable head is a single per-repository linear generation log, so its CAS
// is a serialization point by construction: concurrent commits must be ordered
// regardless. Holding the lock only around the commit turns a thundering herd of
// CAS-conflict retries into an orderly queue where each publish CASes
// uncontended. The CAS-retry loop inside publishWithRetry remains the safety net
// for the rare cross-node case (owner flip during memberlist flux). See
// https://gitlab.com/gitlab-org/gitaly/-/issues/7341.
//
// Returns the new manifest hash (or the base hash when Git produced no new
// state). Errors are returned unmapped; the mvcc middleware maps them to gRPC
// status codes (overlap vs cas_exhausted) before they reach the client.
func PublishSerialized(ctx context.Context, cache *LocalCache, opts RetryOpts) (newSha string, returnedErr error) {
	span, ctx := tracing.StartSpanIfHasParent(ctx, "mvcc.PublishSerialized", nil)
	defer span.End()

	if cache.ReadOnly() {
		return "", nil
	}

	// Count the publish outcome, mirroring LocalCache.Commit so the conflict-rate
	// signal keeps working when writes publish through this path. Read-only RPCs
	// return above and are not counted. conflict is flagged explicitly because a
	// caller-registered conflict error no longer matches ErrManifestPointerConflict.
	var conflict bool
	defer func() { cache.recordCommit(returnedErr, conflict) }()

	// Wait for warming to finish before reading the base state the publish builds
	// on. A background context is used so the wait ignores request cancellation.
	if err := cache.WaitUntilReady(context.Background()); err != nil {
		return "", err
	}
	if cache.ManifestPath() == "" {
		return "", fmt.Errorf("empty GIT_MVCC_MANIFEST_PATH")
	}

	// Assert the at-most-once property: advancing the canonical pointer more than
	// once for a single RPC would result in a torn write.
	if cache.committed {
		return "", fmt.Errorf("mvcc: publish called more than once")
	}
	cache.committed = true

	staged, err := cache.stagedManifestHash()
	if err != nil {
		return "", err
	}
	if staged == cache.base.Hash {
		return cache.base.Hash, nil // no changes, nothing to publish
	}

	// Upload phase, deliberately outside the lock. casOnce under the lock does not
	// re-upload; the only artifact the retry loop uploads under the lock is the
	// merged manifest body produced by a 3-way merge.
	if err := cache.uploadStaged(ctx); err != nil {
		return "", err
	}

	// Under the lock a first conflict can only be a same-process stale base (the
	// previous holder just committed); it merges and re-CASes uncontended, so the
	// first retry skips its backoff. Later conflicts can only be cross-node, which
	// must still back off to avoid lockstep.
	opts.Serialized = true

	// Because publishes now serialize per repository, the lock's wait and hold
	// times are the queue's throughput signal: on a hot repository they grow with
	// concurrency before it becomes client-visible latency, so log both.
	lockWaitStart := time.Now()
	release, err := LockPublish(ctx, cache.repositoryID)
	if err != nil {
		return "", fmt.Errorf("acquire publish lock: %w", err)
	}
	lockWaited := time.Since(lockWaitStart)
	publishLockWait.Observe(lockWaited.Seconds())
	span.SetAttributes(attribute.Int64("mvcc.lock_wait_ms", lockWaited.Milliseconds()))

	// Track only the time the lock is actually held: the backoff hook below drops
	// it during inter-attempt sleeps, so the hold metric must exclude those gaps.
	var heldTotal time.Duration
	heldSince := time.Now()
	locked := true
	releaseLock := func() {
		if locked {
			heldTotal += time.Since(heldSince)
			release()
			locked = false
		}
	}
	defer releaseLock()

	// Between CAS attempts, drop the lock for the backoff and reacquire it after,
	// so a writer that must back off does not stall the queue the lock exists to
	// order. The merged manifest is already staged durably, so another writer
	// winning the lock meanwhile just means the next attempt sees a newer head and
	// merges again.
	opts.Backoff = func(ctx context.Context, d time.Duration) error {
		releaseLock()
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return ctx.Err()
		}
		r, err := LockPublish(ctx, cache.repositoryID)
		if err != nil {
			return fmt.Errorf("reacquire publish lock: %w", err)
		}
		release = r
		heldSince = time.Now()
		locked = true
		return nil
	}

	newSha, err = cache.publishWithRetry(ctx, opts)
	releaseLock() // stop the hold clock and release; the deferred call is then a no-op
	publishLockHold.Observe(heldTotal.Seconds())
	span.SetAttributes(attribute.Int64("mvcc.lock_hold_ms", heldTotal.Milliseconds()))
	if err != nil {
		if errors.Is(err, ErrManifestPointerConflict) {
			conflict = true
		}
		// Honor an error registered via SetCommitConflictError (e.g.
		// UserMergeBranch's referenceUpdateConflictError) when the terminal
		// failure is a pointer conflict, matching the single-shot Commit path so
		// Rails sees the backend-agnostic conflict error.
		if cache.commitConflictErr != nil && errors.Is(err, ErrManifestPointerConflict) {
			// Carry the conflict kind buildConflictError classified (overlap or
			// cas_exhausted) onto the registered error. Flattening err to text drops
			// its metadata, so the kind would otherwise be lost and the conflict
			// could not be told apart in logs.
			wrapped := structerr.New("%w: %s", cache.commitConflictErr, err.Error())
			if kind, ok := structerr.ExtractMetadata(err)[ConflictKindMetadataKey]; ok {
				wrapped = wrapped.WithMetadata(ConflictKindMetadataKey, kind)
			}
			return "", wrapped
		}
		return "", err
	}

	log.FromContext(ctx).WithFields(log.Fields{
		"new_manifest_sha":     newSha,
		"manifest_path":        cache.ManifestPath(),
		"publish_lock_wait_ms": lockWaited.Milliseconds(),
		"publish_lock_hold_ms": heldTotal.Milliseconds(),
	}).InfoContext(ctx, "MVCC: serialized publish complete")

	return newSha, nil
}

// publishLocks serializes concurrent publishes to the same repository within
// this process.
//
// A repository is served by a single Gitaly process, so its concurrent
// publishes run here. The durable head is a linear per-repository generation
// log, so N concurrent writers need N sequential CAS wins. Without
// serialization they form a thundering herd: each loses the CAS, 3-way-merges,
// and re-CASes, and under enough concurrency the head advances faster than a
// writer can retry within its budget, so many are rejected with cas_exhausted.
//
// Holding a per-repository lock around the publish critical section turns that
// herd into an orderly queue: each publish merges onto the previous winner's
// committed head and CASes uncontended, succeeding without exhausting retries.
// The CAS-retry loop remains the safety net for a writer this lock cannot see,
// such as another process racing the same durable head. See
// https://gitlab.com/gitlab-org/gitaly/-/issues/7341.
var publishLocks = &keyedMutex{entries: make(map[string]*keyedMutexEntry)}

type keyedMutexEntry struct {
	// sem is a capacity-one semaphore used as a cancellable mutex: sending
	// acquires it, receiving releases it. A channel rather than a sync.Mutex so a
	// caller can abandon the wait when its context is cancelled.
	sem  chan struct{}
	refs int
}

type keyedMutex struct {
	guard   sync.Mutex
	entries map[string]*keyedMutexEntry
}

// LockPublish acquires the per-repository publish lock for the given key (the
// repository ID) and returns a release function. Each repository has its own
// durable generation log, so the key must be the repository's own identity, not
// its object-storage prefix: object pools (gitaly#7250) let distinct forks share
// an artifact prefix while keeping independent logs, and they must not share a
// lock. Concurrent callers for the same key serialize; different keys proceed in
// parallel. The entry is reclaimed once its last holder releases, so the map
// does not grow without bound.
//
// It blocks until the lock is acquired or ctx is done. On cancellation it
// acquires nothing and returns ctx.Err(); the release function is nil, so
// callers must check the error before deferring it.
func LockPublish(ctx context.Context, key string) (func(), error) {
	publishLocks.guard.Lock()
	e := publishLocks.entries[key]
	if e == nil {
		e = &keyedMutexEntry{sem: make(chan struct{}, 1)}
		publishLocks.entries[key] = e
	}
	// Count this holder before releasing the guard so the entry is not reclaimed
	// out from under a waiter blocked on the semaphore below.
	e.refs++
	publishLocks.guard.Unlock()

	// unref drops this holder's reference and reclaims the entry when it was the
	// last one. Shared by the release function and the cancel path.
	unref := func() {
		publishLocks.guard.Lock()
		e.refs--
		if e.refs == 0 {
			delete(publishLocks.entries, key)
		}
		publishLocks.guard.Unlock()
	}

	release := func() {
		<-e.sem
		unref()
	}

	// Uncontended fast path, so the gauge only counts callers that block.
	select {
	case e.sem <- struct{}{}:
		return release, nil
	default:
	}

	publishLockWaiters.Inc()
	defer publishLockWaiters.Dec()

	select {
	case e.sem <- struct{}{}:
		return release, nil
	case <-ctx.Done():
		unref()
		return nil, ctx.Err()
	}
}
