package mvcc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
)

func TestLockPublish_serializesSameKey(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	const key = "repo-a"
	var inCritical atomic.Int32
	var maxConcurrent atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := LockPublish(ctx, key)
			require.NoError(t, err)
			defer release()

			n := inCritical.Add(1)
			for {
				prev := maxConcurrent.Load()
				if n <= prev || maxConcurrent.CompareAndSwap(prev, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			inCritical.Add(-1)
		}()
	}
	wg.Wait()

	require.Equal(t, int32(1), maxConcurrent.Load(),
		"the per-key lock must admit at most one holder at a time")
}

func TestLockPublish_differentKeysProceedInParallel(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	// Hold key-a, then confirm key-b is acquirable without waiting.
	releaseA, err := LockPublish(ctx, "key-a")
	require.NoError(t, err)
	defer releaseA()

	done := make(chan struct{})
	go func() {
		releaseB, err := LockPublish(ctx, "key-b")
		require.NoError(t, err)
		releaseB()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a different key should not block on an unrelated held lock")
	}
}

func TestLockPublish_contextCancel(t *testing.T) {
	t.Parallel()

	releaseHeld, err := LockPublish(testhelper.Context(t), "busy")
	require.NoError(t, err)
	defer releaseHeld()

	ctx, cancel := context.WithCancel(testhelper.Context(t))
	cancel()

	release, err := LockPublish(ctx, "busy")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, release, "release must be nil when acquisition fails so callers can guard before deferring")
}

func TestLockPublish_entryReclaimed(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	const key = "ephemeral"
	release, err := LockPublish(ctx, key)
	require.NoError(t, err)
	release()

	publishLocks.guard.Lock()
	_, present := publishLocks.entries[key]
	publishLocks.guard.Unlock()
	require.False(t, present, "the entry must be reclaimed once its last holder releases")
}
