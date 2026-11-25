package mvcc

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
)

// DefaultMaxAttempts is the retry-budget ceiling used when RetryOpts leaves
// MaxAttempts unset. Three attempts gives one initial publish plus two retries,
// enough headroom to absorb a brief contention burst (the most common case is
// one concurrent writer winning the CAS race) while staying bounded under
// pathological concurrency. Operators who need to disable retries entirely can
// set MaxAttempts=1 explicitly.
const DefaultMaxAttempts = 3

const (
	retryBackoffBase = 350 * time.Millisecond
	retryBackoffCap  = 2 * time.Second
)

// backoffDelay returns the wait before the given retry (1-based): a uniformly
// random duration in [0, min(cap, base·2^(retry−1))). Full jitter keeps
// contending writers from retrying in lockstep and colliding on the same
// generation again. The doubling loop stops at the cap, so large retry counts
// cannot overflow.
func backoffDelay(retry int) time.Duration {
	d := retryBackoffBase
	for i := 1; i < retry && d < retryBackoffCap; i++ {
		d *= 2
	}
	return rand.N(min(d, retryBackoffCap))
}

// RetryOpts controls the publishWithRetry loop.
type RetryOpts struct {
	// MaxAttempts is the hard cap on publish attempts. Zero means use
	// DefaultMaxAttempts. Set to 1 to disable retries entirely: the first CAS
	// conflict surfaces as Aborted with kind=cas_exhausted and no rebase is
	// attempted. This is the recommended operator-level knob for sites whose
	// custom hooks may read arbitrary ref values at prepare time and therefore
	// can't tolerate a rebase.
	MaxAttempts int

	// MergeOpts supplies the reftable callbacks for the 3-way merge. See
	// MergeOpts for details on each callback.
	MergeOpts MergeOpts

	// Serialized is set when the loop runs under the per-repository publish lock
	// (PublishSerialized). The first retry then skips its backoff, because a first
	// conflict under the lock is a same-process stale base that re-CASes
	// uncontended; later conflicts can only be cross-node and still back off.
	Serialized bool

	// Backoff, if set, performs the inter-attempt wait of the given duration.
	// PublishSerialized uses it to release the publish lock during the sleep so
	// queued writers proceed, then reacquire before the next attempt. When nil the
	// loop sleeps directly. It must return ctx.Err() if ctx is cancelled.
	Backoff func(ctx context.Context, d time.Duration) error

	// Logger, if set, receives diagnostic events from the loop.
	Logger log.Logger
}

// publishWithRetry advances the durable manifest pointer, retrying on CAS
// conflict. On success it returns the new manifest hash (or the base hash when
// Git produced no new state). On a CAS conflict it performs a 3-way manifest
// merge (base/ours/theirs) via threeWayMerge:
//
//   - Disjoint ref sets: the merge succeeds, the cache is reset to the merged
//     manifest, and the loop re-publishes. Counts as one attempt.
//   - Overlapping ref sets: the conflict surfaces as Aborted with kind=overlap
//     so the client redrives the whole RPC.
//
// On any non-conflict error the underlying error is returned unchanged so
// callers can keep their existing error handling.
//
// Budget enforcement:
//
//   - Each iteration starts with a context check. A cancelled or
//     deadline-exceeded context returns ctx.Err() immediately so the caller can
//     convert it to the right gRPC code.
//   - Retries are spaced with capped full-jitter exponential backoff (see
//     backoffDelay); the wait aborts early when the context expires. The first
//     attempt is never delayed, and under the publish lock (opts.Serialized) the
//     first retry skips its backoff too (a same-process stale base re-CASes
//     uncontended).
//   - The final attempt does not merge: a conflict there has no CAS to follow,
//     so the loop returns kind=cas_exhausted instead of doing a wasted rebase. A
//     genesis conflict (empty base) likewise exhausts, having no common ancestor
//     to merge from.
//   - When MaxAttempts is reached without success, the loop returns Aborted with
//     the last observed *ConflictError as the underlying cause and
//     kind=cas_exhausted.
func (m *LocalCache) publishWithRetry(ctx context.Context, opts RetryOpts) (string, error) {
	// Attempts and accumulated backoff are the CAS-contention signal. Without
	// them a write that lost several races and slept is indistinguishable from a
	// write that was simply slow, because the retries are invisible from outside
	// the loop.
	span, ctx := tracing.StartSpanIfHasParent(ctx, "mvcc.LocalCache.publishWithRetry", nil)
	defer span.End()

	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}

	var lastConflict *ConflictError
	var lastErr error
	var totalBackoff time.Duration
	var attempts int
	// mergedLastAttempt records whether the previous attempt ended in a 3-way
	// merge. Only then may the first retry under the publish lock skip its backoff.
	var mergedLastAttempt bool
	// Recorded in a defer so the exhausted and overlap returns carry the counts
	// too. Those are the returns the contention signal is for; the success return
	// is the uninteresting one.
	defer func() {
		span.SetAttributes(
			attribute.Int("mvcc.attempts", attempts),
			attribute.Int64("mvcc.total_backoff_ms", totalBackoff.Milliseconds()),
		)
	}()

	// attempt is 1-based: it is the try number surfaced in the logs, the span
	// attributes, and the conflict error's metadata.
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attempts = attempt
		if err := ctx.Err(); err != nil {
			return "", err
		}
		// The first retry under the publish lock skips its backoff only when the
		// previous attempt merged: a stale-base conflict re-CASes uncontended, so
		// the wait is wasted. A transient conflict (a locked pointer, or Winner ==
		// Base from an in-flight PUT) made no progress, so it must still back off to
		// let the contending write land before retrying.
		var backoff time.Duration
		if attempt > 1 && !(opts.Serialized && attempt == 2 && mergedLastAttempt) {
			backoff = backoffDelay(attempt - 1)
			totalBackoff += backoff
			if opts.Backoff != nil {
				if err := opts.Backoff(ctx, backoff); err != nil {
					return "", err
				}
			} else {
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return "", ctx.Err()
				}
			}
		}
		mergedLastAttempt = false

		newSha, err := m.casOnce(ctx)
		if err == nil {
			return newSha, nil
		}
		if !errors.Is(err, ErrManifestPointerConflict) {
			return "", err
		}
		lastErr = err

		var conflict *ConflictError
		if !errors.As(err, &conflict) {
			// A conflict with no observable winner to merge onto: the durable
			// pointer was locked by another publisher, or the head read-back
			// failed (the EE backend joins the sentinel without a winner). It is
			// transient, so retry the CAS after a backoff; the next read sees the
			// winner as a *ConflictError if one landed. There is nothing to merge.
			if attempt == maxAttempts {
				break
			}
			continue
		}
		lastConflict = conflict

		// Winner == Base means the head read-back saw the losing write's own base
		// rather than a newer manifest: an object store can report a conditional-PUT
		// conflict while the winning PUT to the same slot is still in flight (S3's
		// 409 ConditionalRequestConflict), so the read-back has not observed it yet
		// (see ConflictError.Winner). There is nothing newer to merge onto, so retry
		// the CAS after a backoff rather than 3-way merging ours against itself.
		if conflict.Winner.Hash == conflict.Base.Hash {
			if attempt == maxAttempts {
				break
			}
			continue
		}

		// Stop before the merge when no further CAS can follow: the budget is
		// exhausted, or a genesis conflict (empty base) has no base manifest to
		// merge from. Both redrive as cas_exhausted.
		if attempt == maxAttempts || conflict.Base.Hash == "" {
			break
		}

		if opts.Logger != nil {
			opts.Logger.WithFields(log.Fields{
				"attempt":           attempt,
				"backoff_ms":        backoff.Milliseconds(),
				"total_backoff_ms":  totalBackoff.Milliseconds(),
				"base_sha":          conflict.Base.Hash,
				"base_generation":   conflict.Base.Generation,
				"theirs_sha":        conflict.Winner.Hash,
				"theirs_generation": conflict.Winner.Generation,
			}).InfoContext(ctx, "mvcc: CAS conflict; attempting 3-way merge")
		}

		// Read the base, theirs, and ours manifests for the 3-way merge.
		baseManifest, err := m.prepareManifestFile(ctx, conflict.Base.Hash)
		if err != nil {
			return "", fmt.Errorf("mvcc: read base manifest for retry: %w", err)
		}
		theirsManifest, err := m.prepareManifestFile(ctx, conflict.Winner.Hash)
		if err != nil {
			return "", fmt.Errorf("mvcc: read theirs manifest for retry: %w", err)
		}
		oursSha, err := m.stagedManifestHash()
		if err != nil {
			return "", fmt.Errorf("mvcc: read ours pointer for retry: %w", err)
		}
		oursManifest, err := m.prepareManifestFile(ctx, oursSha)
		if err != nil {
			return "", fmt.Errorf("mvcc: read ours manifest: %w", err)
		}

		// 3-way merge: computes exclusive sets, checks ref overlap, and if
		// disjoint, writes a merged manifest with rebased reftables and updates
		// the per-RPC pointer file. The next iteration's casOnce picks it up.
		merger := NewMerger(m.rs, m.repositoryID, m.cacheDir())
		if err := merger.ThreeWayMerge(ctx, m.ManifestPath(), oursManifest, baseManifest, theirsManifest, opts.MergeOpts); err != nil {
			if errors.Is(err, ErrOverlap) {
				return "", buildConflictError(ConflictKindOverlap, conflict, attempt, totalBackoff)
			}
			return "", fmt.Errorf("mvcc: 3-way merge: %w", err)
		}

		// The merge rebased and uploaded any ours-exclusive reftables, but wrote
		// the merged manifest body only to the local cache. Upload it so the next
		// CAS never advances the pointer to a manifest that is not yet durable.
		mergedSha, err := m.stagedManifestHash()
		if err != nil {
			return "", fmt.Errorf("mvcc: read merged pointer for retry: %w", err)
		}
		mergedRelPath, err := ManifestRelPath(mergedSha)
		if err != nil {
			return "", fmt.Errorf("mvcc: merged manifest path: %w", err)
		}
		if err := m.uploadFromCache(ctx, mergedRelPath); err != nil {
			return "", fmt.Errorf("mvcc: upload merged manifest: %w", err)
		}

		m.resetForRetry(theirsManifest, ManifestPointer{
			Hash:          conflict.Winner.Hash,
			Generation:    conflict.Winner.Generation,
			HasGeneration: m.base.HasGeneration,
		})
		mergedLastAttempt = true
	}

	if lastConflict != nil {
		return "", buildConflictError(ConflictKindCASExhausted, lastConflict, attempts, totalBackoff)
	}
	// Every failing attempt was a transient conflict with no observable winner
	// (a locked pointer, or a failed head read-back), so there is no winner to
	// name. Surface it as cas_exhausted with the retry history.
	return "", structerr.NewAborted("mvcc cas conflict (%s): %w", ConflictKindCASExhausted, lastErr).
		WithMetadata("mvcc_conflict_kind", string(ConflictKindCASExhausted)).
		WithMetadata("attempts", attempts).
		WithMetadata("total_backoff_ms", totalBackoff.Milliseconds())
}

// buildConflictError builds the terminal Aborted error returned to the caller
// when no further retry is possible. The metadata carries the conflict kind, the
// base/actual hashes, and the retry history (attempt count and total backoff).
func buildConflictError(kind ConflictKind, conflict *ConflictError, attempts int, totalBackoff time.Duration) error {
	return structerr.NewAborted("mvcc cas conflict (%s): %w", kind, conflict).
		WithMetadata("mvcc_conflict_kind", string(kind)).
		WithMetadata("base_sha", conflict.Base.Hash).
		WithMetadata("actual_sha", conflict.Winner.Hash).
		WithMetadata("attempts", attempts).
		WithMetadata("total_backoff_ms", totalBackoff.Milliseconds())
}
