package mvcc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
)

// lockThenSucceedStorage returns a locked-pointer error (which wraps the
// sentinel but is not a *ConflictError) for its first `failures` commits, then
// succeeds. It models the durable pointer being briefly held by another
// publisher.
type lockThenSucceedStorage struct {
	ObjectStorage
	commitCalls int
	failures    int
}

func (s *lockThenSucceedStorage) Commit(context.Context, string, ManifestPointer, string) error {
	s.commitCalls++
	if s.commitCalls <= s.failures {
		return fmt.Errorf("manifest pointer locked: %w", ErrManifestPointerConflict)
	}
	return nil
}

// winnerEqualsBaseStorage returns a *ConflictError whose Winner equals the base
// for its first `failures` commits, then succeeds. It models an object store
// reporting a conditional-PUT conflict while the winning write to the same slot
// is still in flight, so the head read-back has not observed a newer manifest.
type winnerEqualsBaseStorage struct {
	ObjectStorage
	commitCalls int
	failures    int
}

func (s *winnerEqualsBaseStorage) Commit(_ context.Context, _ string, base ManifestPointer, _ string) error {
	s.commitCalls++
	if s.commitCalls <= s.failures {
		return &ConflictError{Base: base, Winner: base}
	}
	return nil
}

func TestLocalCache_publishWithRetry_retriesLockedPointer(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	newCache := func(rs ObjectStorage) *LocalCache {
		staged := filepath.Join(t.TempDir(), "staged")
		require.NoError(t, os.WriteFile(staged, []byte("newhash"), 0o644))
		return &LocalCache{
			rs:           rs,
			repositoryID: "repo",
			base:         ManifestPointer{Hash: "basehash"},
			environments: map[string]string{"GIT_MVCC_MANIFEST_PATH": staged},
		}
	}

	t.Run("retries a locked pointer then succeeds", func(t *testing.T) {
		t.Parallel()
		rs := &lockThenSucceedStorage{failures: 1}
		cache := newCache(rs)
		// A locked pointer is transient, not a merge, so the first retry backs off;
		// the hook returns immediately to keep the test fast.
		newSha, err := cache.publishWithRetry(ctx, RetryOpts{
			Serialized: true,
			Backoff:    func(context.Context, time.Duration) error { return nil },
		})
		require.NoError(t, err)
		require.Equal(t, "newhash", newSha)
		require.Equal(t, 2, rs.commitCalls, "locked pointer must be retried, not returned on the first attempt")
	})

	t.Run("exhausts as cas_exhausted when the pointer stays locked", func(t *testing.T) {
		t.Parallel()
		rs := &lockThenSucceedStorage{failures: 100}
		cache := newCache(rs)
		_, err := cache.publishWithRetry(ctx, RetryOpts{
			MaxAttempts: 2,
			Serialized:  true,
			Backoff:     func(context.Context, time.Duration) error { return nil },
		})
		require.ErrorIs(t, err, ErrManifestPointerConflict)
		require.Equal(t, string(ConflictKindCASExhausted), structerr.ExtractMetadata(err)["mvcc_conflict_kind"])
		require.Equal(t, 2, rs.commitCalls)
	})

	t.Run("refuses to CAS an empty staged pointer", func(t *testing.T) {
		t.Parallel()
		empty := filepath.Join(t.TempDir(), "staged")
		require.NoError(t, os.WriteFile(empty, []byte(""), 0o644))
		cache := &LocalCache{
			rs:           &lockThenSucceedStorage{}, // Commit must not be reached
			repositoryID: "repo",
			base:         ManifestPointer{Hash: "basehash"},
			environments: map[string]string{"GIT_MVCC_MANIFEST_PATH": empty},
		}
		_, err := cache.casOnce(ctx)
		require.ErrorContains(t, err, "empty manifest pointer")
	})

	t.Run("retries a Winner==Base conflict without merging against itself", func(t *testing.T) {
		t.Parallel()
		rs := &winnerEqualsBaseStorage{failures: 1}
		cache := newCache(rs)
		// Without the guard the loop would try to 3-way merge, reading the base
		// manifest file (absent in this stub), and error. With it, it retries the CAS.
		var backoffCalls int
		newSha, err := cache.publishWithRetry(ctx, RetryOpts{
			Serialized: true,
			Backoff:    func(context.Context, time.Duration) error { backoffCalls++; return nil },
		})
		require.NoError(t, err)
		require.Equal(t, "newhash", newSha)
		require.Equal(t, 2, rs.commitCalls)
		// A transient Winner==Base conflict is not a stale-base merge, so the first
		// retry must still back off; the Serialized skip must not apply to it.
		require.Equal(t, 1, backoffCalls)
	})

	t.Run("backoff hook replaces the inline sleep", func(t *testing.T) {
		t.Parallel()
		rs := &lockThenSucceedStorage{failures: 2}
		cache := newCache(rs)

		var backoffCalls int
		_, err := cache.publishWithRetry(ctx, RetryOpts{
			MaxAttempts: 5,
			// A hook that returns immediately: PublishSerialized uses it to
			// release the publish lock during the sleep. Here it just proves the
			// loop waits through the hook rather than sleeping inline.
			Backoff: func(context.Context, time.Duration) error {
				backoffCalls++
				return nil
			},
		})
		require.NoError(t, err)
		// Attempts 1 and 2 hit the locked pointer; attempt 3 succeeds. The hook
		// runs before attempts 2 and 3 (attempt 1 has no backoff).
		require.Equal(t, 2, backoffCalls)
		require.Equal(t, 3, rs.commitCalls)
	})
}

func TestBackoffDelay_bounds(t *testing.T) {
	t.Parallel()

	// The first retry is bounded by base; later retries double up to the cap.
	for retry, wantCap := range map[int]time.Duration{
		1: retryBackoffBase,
		2: 2 * retryBackoffBase,
		3: 4 * retryBackoffBase,
		9: retryBackoffCap, // long since saturated at the cap
	} {
		for i := 0; i < 200; i++ {
			d := backoffDelay(retry)
			require.GreaterOrEqual(t, d, time.Duration(0))
			require.Less(t, d, wantCap+time.Nanosecond)
			require.LessOrEqual(t, d, retryBackoffCap)
		}
	}
}
