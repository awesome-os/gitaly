package migration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

func TestMigrationHandler_RegisterMigration(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	m := &Handler{migrateCh: make(chan migrationData)}

	// The send is non-blocking, so retry until the receive below is ready.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				m.RegisterMigration(ctx, repo, ignoreCtx(func() error { return nil }), func() error { return nil })
				runtime.Gosched()
			}
		}
	}()

	data := <-m.migrateCh
	close(stop)
	<-done
	require.Equal(t, repo, data.repo)
}

func TestMigrationHandler_CancelMigration(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	for _, tc := range []struct {
		desc        string
		repo        *localrepo.Repo
		expectedErr error
	}{
		{
			desc: "unknown repo is a no-op",
			repo: func() *localrepo.Repo {
				other, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				return localrepo.NewTestRepo(t, cfg, other)
			}(),
		},
		{
			desc:        "known repo cancels in-flight migration",
			repo:        repo,
			expectedErr: context.Canceled,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			cancelCtx, cancel := context.WithCancel(context.Background())

			parentCtx, parentCancel := context.WithCancel(context.Background())
			m := &Handler{
				state:     sync.Map{},
				ctx:       parentCtx,
				ctxCancel: parentCancel,
			}
			m.state.Store(repoKeyFor(repo), migratorState{cancelCtx: cancel})

			m.CancelMigration(tc.repo)
			require.ErrorIs(t, cancelCtx.Err(), tc.expectedErr)
		})
	}
}

func TestMigrationHandler(t *testing.T) {
	t.Parallel()

	gittest.SkipWithoutMVCCGit(t)
	testhelper.SkipWithPraefect(t, "we shouldn't migrate with Praefect")
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	logger := testhelper.NewLogger(t)
	hook := testhelper.AddLoggerHook(logger)
	metrics := NewMetrics()

	type setupData struct {
		block      func() error
		unblock    func() error
		run        func(m *Handler, repo *localrepo.Repo)
		repoProto  *gitalypb.Repository
		startState *migratorState
	}

	for _, tc := range []struct {
		desc             string
		setup            func() setupData
		completed        bool
		attempts         uint
		expectedLogMsg   string
		expectedLogLevel string
		// expectForgotten indicates the repository's state entry must be gone
		// entirely rather than merely unchanged: a repository that doesn't
		// exist has nothing worth remembering, and keeping the entry anyway
		// would grow the map without bound under repository create/delete
		// churn.
		expectForgotten bool
		// skipWithReftable skips the subtest when the default ref format is reftable.
		// Use for cases that exercise files-backend-specific behaviour (e.g. packed-refs mtime
		// detection) that has no equivalent on a reftable repository.
		skipWithReftable bool
	}{
		{
			desc: "cancelled migration",
			setup: func() setupData {
				ch := make(chan struct{})
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")

				return setupData{
					repoProto: repo,
					block: func() error {
						<-ch
						<-ch
						return nil
					},
					unblock: func() error { return nil },
					run: func(m *Handler, repo *localrepo.Repo) {
						ch <- struct{}{}
						m.CancelMigration(repo)
						ch <- struct{}{}
					},
				}
			},
			completed:        false,
			attempts:         1,
			expectedLogMsg:   "MVCC migration canceled for repository",
			expectedLogLevel: "warning",
		},
		{
			desc: "existing state accumulates attempts across cancel",
			setup: func() setupData {
				ch := make(chan struct{})
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")

				return setupData{
					repoProto: repo,
					block: func() error {
						<-ch
						<-ch
						return nil
					},
					unblock:    func() error { return nil },
					startState: &migratorState{attempts: 3},
					run: func(m *Handler, repo *localrepo.Repo) {
						ch <- struct{}{}
						m.CancelMigration(repo)
						ch <- struct{}{}
					},
				}
			},
			completed:        false,
			attempts:         4,
			expectedLogMsg:   "MVCC migration canceled for repository",
			expectedLogLevel: "warning",
		},
		{
			desc: "ErrRepositoryNotFound is silently skipped (no attempt counted)",
			setup: func() setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
				return setupData{
					repoProto: repo,
					block:     func() error { return storage.ErrRepositoryNotFound },
					unblock:   func() error { return nil },
					run:       func(m *Handler, repo *localrepo.Repo) {},
				}
			},
			expectForgotten: true,
		},
		{
			desc: "hard migration failure increments attempts and sets backoff",
			setup: func() setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
				return setupData{
					repoProto: repo,
					block:     func() error { return errors.New("forced failure") },
					unblock:   func() error { return nil },
					run:       func(m *Handler, repo *localrepo.Repo) {},
				}
			},
			completed:        false,
			attempts:         1,
			expectedLogMsg:   "MVCC migration failed for repository",
			expectedLogLevel: "error",
		},
		{
			desc:             "packed-refs modified is silently skipped (no attempt counted, no backoff)",
			skipWithReftable: true,
			setup: func() setupData {
				// packed-refs exists but the block() simulates a concurrent
				// packed-refs modification (e.g. housekeeping's own pack-refs
				// racing the migration). This must resolve itself without
				// intervention.
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
				return setupData{
					repoProto: repo,
					block: func() error {
						return os.Chtimes(filepath.Join(repoPath, "packed-refs"), time.Now(), time.Now())
					},
					unblock: func() error { return nil },
					run:     func(m *Handler, repo *localrepo.Repo) {},
				}
			},
			completed: false,
			attempts:  0,
		},
		{
			desc: "successful migration",
			setup: func() setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
				return setupData{
					repoProto: repo,
					block:     func() error { return nil },
					unblock:   func() error { return nil },
					run:       func(m *Handler, repo *localrepo.Repo) {},
				}
			},
			completed:        true,
			attempts:         1,
			expectedLogMsg:   "MVCC migration successful for repository",
			expectedLogLevel: "info",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.skipWithReftable {
				testhelper.SkipWithReftable(t, "packed-refs does not exist on reftable repositories")
			}
			data := tc.setup()

			parentCtx, parentCancel := context.WithCancel(context.Background())
			m := &Handler{
				wg:           sync.WaitGroup{},
				ctx:          parentCtx,
				ctxCancel:    parentCancel,
				logger:       logger,
				metrics:      metrics,
				migrateCh:    make(chan migrationData),
				housekeeping: noopHousekeeping{},
				state:        sync.Map{},
			}

			repo := localrepo.NewTestRepo(t, cfg, data.repoProto)
			key := repoKeyFor(repo)

			if data.startState != nil {
				m.state.Store(key, *data.startState)
			}

			m.Run(testhelper.Context(t), noParent)
			defer m.Close()

			for {
				if val, ok := m.state.Load(key); ok {
					if val.(migratorState).cancelCtx != nil {
						break
					}
				}
				m.RegisterMigration(context.Background(), repo, ignoreCtx(data.block), data.unblock)
			}

			data.run(m, repo)

			// Drain: send an empty item to ensure the handler has finished
			// processing the previous migration before we inspect state.
			m.migrateCh <- migrationData{}

			val, ok := m.state.Load(key)
			if tc.expectForgotten {
				require.False(t, ok, "state entry must have been forgotten")
				return
			}
			require.True(t, ok)
			state := val.(migratorState)

			require.Equal(t, tc.completed, state.completed)
			require.Equal(t, tc.attempts, state.attempts)

			if tc.expectedLogMsg != "" {
				entries := hook.AllEntries()
				entry := entries[len(entries)-1]
				require.Equal(t, tc.expectedLogMsg, entry.Message)
				require.Equal(t, tc.expectedLogLevel, entry.Level.String())
				require.GreaterOrEqual(t, entry.Data["mvcc.migration_latency_ms"].(int64), int64(0))
				require.Greater(t, entry.Data["mvcc.migration_attempts"].(uint), uint(0))
			}
		})
	}
}

func TestMigrationHandler_Metrics(t *testing.T) {
	gittest.SkipWithoutMVCCGit(t)
	testhelper.SkipWithPraefect(t, "we shouldn't migrate with Praefect")
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	logger := testhelper.NewLogger(t)

	for _, tc := range []struct {
		desc  string
		setup func() (*localrepo.Repo, func() error)
		// cancelInBlock cancels the attempt from within block(), before block()
		// fails with an error unrelated to the cancellation.
		cancelInBlock bool
		// skipWithReftable skips the subtest on reftable repositories; use for
		// cases that rely on packed-refs existing.
		skipWithReftable      bool
		expectedSuccesses     int
		expectedFailures      float64
		expectedCancellations float64
	}{
		{
			desc: "a canceled attempt is labeled as canceled whatever error it fails with",
			setup: func() (*localrepo.Repo, func() error) {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
				return localrepo.NewTestRepo(t, cfg, repoProto), nil
			},
			cancelInBlock:         true,
			expectedCancellations: 1,
		},
		{
			desc: "successful migration emits a latency observation and no failures",
			setup: func() (*localrepo.Repo, func() error) {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
				return localrepo.NewTestRepo(t, cfg, repoProto), func() error { return nil }
			},
			expectedSuccesses: 1,
			expectedFailures:  0,
		},
		{
			desc: "hard failure emits a failure and no latency observation",
			setup: func() (*localrepo.Repo, func() error) {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
				block := func() error { return errors.New("forced failure") }
				return localrepo.NewTestRepo(t, cfg, repoProto), block
			},
			expectedSuccesses: 0,
			expectedFailures:  1,
		},
		{
			desc:             "packed-refs modified emits no metrics (skip, not failure)",
			skipWithReftable: true,
			setup: func() (*localrepo.Repo, func() error) {
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
				gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
				block := func() error {
					return os.Chtimes(filepath.Join(repoPath, "packed-refs"), time.Now(), time.Now())
				}
				return localrepo.NewTestRepo(t, cfg, repoProto), block
			},
			expectedSuccesses: 0,
			expectedFailures:  0,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			if tc.skipWithReftable {
				testhelper.SkipWithReftable(t, "packed-refs does not exist on reftable repositories")
			}

			metrics := NewMetrics()
			parentCtx, parentCancel := context.WithCancel(context.Background())
			m := &Handler{
				wg:           sync.WaitGroup{},
				ctx:          parentCtx,
				ctxCancel:    parentCancel,
				logger:       logger,
				metrics:      metrics,
				migrateCh:    make(chan migrationData),
				housekeeping: noopHousekeeping{},
				state:        sync.Map{},
			}

			repo, block := tc.setup()
			if tc.cancelInBlock {
				block = func() error {
					m.CancelMigration(repo)
					return errors.New("read |0: file already closed")
				}
			}
			m.Run(testhelper.Context(t), noParent)
			defer m.Close()

			key := repoKeyFor(repo)
			for {
				if val, ok := m.state.Load(key); ok {
					if val.(migratorState).cancelCtx != nil {
						break
					}
				}
				m.RegisterMigration(context.Background(), repo, ignoreCtx(block), func() error { return nil })
			}
			m.migrateCh <- migrationData{}

			assert.Equal(t, tc.expectedSuccesses, histogramSampleCount(t, metrics.latencyMetric))
			assert.Equal(t, tc.expectedFailures,
				testutil.ToFloat64(metrics.failsMetric.WithLabelValues("migration_error")))
			assert.Equal(t, tc.expectedCancellations,
				testutil.ToFloat64(metrics.failsMetric.WithLabelValues("canceled")))
		})
	}
}

// TestMigrationHandler_logsRepositoryShape verifies that the packed-refs size
// and loose-ref count reported by migrate() reach the operator on the log line.
// These figures are magnitudes and are deliberately not exposed as metrics, so
// the log is the only place they surface and the values must be correct.
func TestMigrationHandler_logsRepositoryShape(t *testing.T) {
	gittest.SkipWithoutMVCCGit(t)
	testhelper.SkipWithPraefect(t, "we shouldn't migrate with Praefect")
	testhelper.SkipWithReftable(t, "checks migration from the files backend")
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	logger := testhelper.NewLogger(t)
	logHook := testhelper.AddLoggerHook(logger)
	metrics := NewMetrics()

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	// Pack a handful of refs, then leave a known number loose on top so both
	// figures are non-trivial and predictable.
	base := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	for _, name := range []string{"packed-a", "packed-b", "packed-c"} {
		gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(name), gittest.WithParents(base))
	}
	gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")

	packedRefsInfo, err := os.Stat(filepath.Join(repoPath, "packed-refs"))
	require.NoError(t, err)
	require.Positive(t, packedRefsInfo.Size())

	const looseCount = 2
	for i := range looseCount {
		gittest.WriteCommit(
			t, cfg, repoPath,
			gittest.WithBranch("loose-"+string(rune('a'+i))),
			gittest.WithParents(base),
		)
	}

	parentCtx, parentCancel := context.WithCancel(context.Background())
	m := &Handler{
		wg:           sync.WaitGroup{},
		ctx:          parentCtx,
		ctxCancel:    parentCancel,
		logger:       logger,
		metrics:      metrics,
		migrateCh:    make(chan migrationData),
		housekeeping: noopHousekeeping{},
		state:        sync.Map{},
	}

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	key := repoKeyFor(repo)

	m.Run(testhelper.Context(t), noParent)
	defer m.Close()

	for {
		if val, ok := m.state.Load(key); ok {
			if val.(migratorState).cancelCtx != nil {
				break
			}
		}
		m.RegisterMigration(context.Background(), repo, ignoreCtx(func() error { return nil }), func() error { return nil })
	}
	m.migrateCh <- migrationData{}

	val, ok := m.state.Load(key)
	require.True(t, ok)
	require.True(t, val.(migratorState).completed, "migration must have succeeded")

	// The log carries both figures, and they match what is on disk.
	entries := logHook.AllEntries()
	entry := entries[len(entries)-1]
	require.Equal(t, "MVCC migration successful for repository", entry.Message)
	require.Equal(t, packedRefsInfo.Size(), entry.Data["mvcc.packed_refs_bytes"].(int64))
	require.Equal(t, looseCount, entry.Data["mvcc.loose_refs_count"].(int))
	require.Contains(t, entry.Data, "mvcc.write_blocked_ms")

	// The histograms record the same figures.
	require.Equal(t, float64(packedRefsInfo.Size()), histogramSampleSum(t, metrics.packedRefsSizeMetric))
	require.Equal(t, float64(looseCount), histogramSampleSum(t, metrics.looseRefsCountMetric))
	require.Equal(t, 1, histogramSampleCount(t, metrics.writeBlockLatencyMetric))
}

func TestMigrationHandler_coolDown(t *testing.T) {
	gittest.SkipWithoutMVCCGit(t)
	testhelper.SkipWithPraefect(t, "we shouldn't migrate with Praefect")
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	logger := testhelper.NewLogger(t)
	metrics := NewMetrics()

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
	gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")

	parentCtx, parentCancel := context.WithCancel(context.Background())
	m := &Handler{
		wg:           sync.WaitGroup{},
		ctx:          parentCtx,
		ctxCancel:    parentCancel,
		logger:       logger,
		metrics:      metrics,
		migrateCh:    make(chan migrationData),
		housekeeping: noopHousekeeping{},
		state:        sync.Map{},
	}

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	key := repoKeyFor(repo)
	m.Run(testhelper.Context(t), noParent)
	defer m.Close()

	// A failing block() is a hard failure, which sets a cooldown.
	block := func() error { return errors.New("forced failure") }

	for {
		if val, ok := m.state.Load(key); ok {
			if val.(migratorState).cancelCtx != nil {
				break
			}
		}
		m.RegisterMigration(context.Background(), repo, ignoreCtx(block), func() error { return nil })
	}
	m.migrateCh <- migrationData{}

	val, _ := m.state.Load(key)
	state := val.(migratorState)
	require.Equal(t, uint(1), state.attempts)
	require.True(t, state.coolDown.After(time.Now()), "cooldown must be set after first failure")

	// Second attempt — cooldown is active, so the handler skips it.
	m.RegisterMigration(context.Background(), repo, ignoreCtx(block), func() error { return nil })
	m.migrateCh <- migrationData{}

	val, _ = m.state.Load(key)
	state = val.(migratorState)
	require.Equal(t, uint(1), state.attempts, "cooldown must prevent a second attempt")
}

func TestMigrationHandler_alreadyCompleted(t *testing.T) {
	gittest.SkipWithoutMVCCGit(t)
	testhelper.SkipWithPraefect(t, "we shouldn't migrate with Praefect")
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	logger := testhelper.NewLogger(t)
	metrics := NewMetrics()

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
	gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")

	parentCtx, parentCancel := context.WithCancel(context.Background())
	m := &Handler{
		wg:           sync.WaitGroup{},
		ctx:          parentCtx,
		ctxCancel:    parentCancel,
		logger:       logger,
		metrics:      metrics,
		migrateCh:    make(chan migrationData),
		housekeeping: noopHousekeeping{},
		state:        sync.Map{},
	}

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	key := repoKeyFor(repo)
	m.Run(testhelper.Context(t), noParent)
	defer m.Close()

	block := func() error { return nil }

	// First migration — succeeds.
	for {
		if val, ok := m.state.Load(key); ok {
			if val.(migratorState).cancelCtx != nil {
				break
			}
		}
		m.RegisterMigration(context.Background(), repo, ignoreCtx(block), func() error { return nil })
	}
	m.migrateCh <- migrationData{}

	val, _ := m.state.Load(key)
	require.True(t, val.(migratorState).completed)
	require.Equal(t, uint(1), val.(migratorState).attempts)

	// Second registration after completion — must be a no-op.
	m.RegisterMigration(context.Background(), repo, ignoreCtx(block), func() error { return nil })
	m.migrateCh <- migrationData{}

	val, _ = m.state.Load(key)
	require.True(t, val.(migratorState).completed)
	require.Equal(t, uint(1), val.(migratorState).attempts, "completed repo must not be re-attempted")
}

// TestMigrationHandler_pooledRepositorySkipped verifies that a repository with an
// objects/info/alternates file is permanently skipped: no attempt is counted, no
// backoff is applied, and the state is marked completed so subsequent registrations
// are no-ops.
func TestMigrationHandler_pooledRepositorySkipped(t *testing.T) {
	gittest.SkipWithoutMVCCGit(t)
	testhelper.SkipWithPraefect(t, "we shouldn't migrate with Praefect")
	testhelper.SkipWithReftable(t, "checks migration from the files backend")
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")

	// Simulate pool membership.
	alternatesPath := filepath.Join(repoPath, "objects", "info", "alternates")
	require.NoError(t, os.WriteFile(alternatesPath, []byte("/some/pool/objects\n"), 0o644))

	logger := testhelper.NewLogger(t)
	hook := testhelper.AddLoggerHook(logger)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	m := NewHandler(logger, NewMetrics(), noopHousekeeping{})
	m.handle(migrationData{ctx: ctx, repo: repo, block: ignoreCtx(func() error { return nil }), unblock: func() error { return nil }})

	val, ok := m.state.Load(repoKeyFor(repo))
	require.True(t, ok)
	state := val.(migratorState)
	require.True(t, state.completed, "pooled repo must be permanently skipped")
	require.Equal(t, uint(0), state.attempts, "no attempt must be counted for a pooled repo")
	require.True(t, state.coolDown.IsZero(), "no cooldown must be applied for a pooled repo")

	entries := hook.AllEntries()
	require.Equal(t, "MVCC migration skipped: repository is part of an object pool", entries[len(entries)-1].Message)
	require.Equal(t, "warning", entries[len(entries)-1].Level.String())
}

// histogramSampleCount returns the number of observations recorded in the
// histogram. testutil.CollectAndCount is not usable here: it counts series, and
// a plain Histogram always exposes exactly one series whether or not anything
// has been observed.
func histogramSampleCount(t *testing.T, h prometheus.Histogram) int {
	t.Helper()

	var m dto.Metric
	require.NoError(t, h.(prometheus.Metric).Write(&m))

	return int(m.GetHistogram().GetSampleCount())
}

// histogramSampleSum returns the sum of observations recorded in the histogram.
func histogramSampleSum(t *testing.T, h prometheus.Histogram) float64 {
	t.Helper()

	var m dto.Metric
	require.NoError(t, h.(prometheus.Metric).Write(&m))

	return m.GetHistogram().GetSampleSum()
}

func TestCoolDown(t *testing.T) {
	t.Parallel()

	for attempts, expected := range map[uint]time.Duration{
		// attempts=0 is unreachable in production (attempts is always incremented
		// before coolDown is called), but the uint underflow wraps to MaxUint,
		// which clamps to the 6-hour cap via min(MaxUint,5).
		0:   6 * time.Hour,
		1:   15 * time.Minute,
		2:   30 * time.Minute,
		3:   time.Hour,
		4:   2 * time.Hour,
		5:   4 * time.Hour,
		6:   6 * time.Hour,
		7:   6 * time.Hour,
		100: 6 * time.Hour,
	} {
		require.Equal(t, expected, coolDown(attempts), "attempts=%d", attempts)
	}
}

// noParent stands in for waiting on the parent of a graceful upgrade when there
// is none.
func noParent(context.Context) error { return nil }

// waitForChannel returns a wait for the parent process, which exits once
// exited is closed.
func waitForChannel(exited <-chan struct{}) func(context.Context) error {
	return func(ctx context.Context) error {
		select {
		case <-exited:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func TestHandler_Run(t *testing.T) {
	t.Parallel()

	t.Run("migrations wait for the parent to exit", func(t *testing.T) {
		t.Parallel()

		parentExited := make(chan struct{})
		m := NewHandler(testhelper.NewLogger(t), NewMetrics(), noopHousekeeping{})
		m.Run(testhelper.Context(t), waitForChannel(parentExited))
		defer m.Close()

		// Nothing receives registrations while the parent is alive.
		select {
		case m.migrateCh <- migrationData{}:
			require.FailNow(t, "handler accepted a migration before the parent exited")
		default:
		}

		close(parentExited)
		m.migrateCh <- migrationData{}
	})

	t.Run("closing returns while waiting for the parent", func(t *testing.T) {
		t.Parallel()

		m := NewHandler(testhelper.NewLogger(t), NewMetrics(), noopHousekeeping{})
		m.Run(testhelper.Context(t), waitForChannel(make(chan struct{})))

		// The upgrade hook and the deferred Close both run in the parent of an
		// upgrade, so closing must be safe to repeat.
		m.Close()
		m.Close()
	})
}

type noopHousekeeping struct{}

func (noopHousekeeping) TryLockRepository(storage.Repository) (func(), bool) { return func() {}, true }

type busyHousekeeping struct{}

func (busyHousekeeping) TryLockRepository(storage.Repository) (func(), bool) { return nil, false }

func ignoreCtx(f func() error) func(context.Context) error {
	return func(context.Context) error { return f() }
}

func TestMigrationHandler_busyRepositoryBacksOff(t *testing.T) {
	gittest.SkipWithoutMVCCGit(t)
	testhelper.SkipWithReftable(t, "checks migration from the files backend")
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc           string
		housekeeping   HousekeepingLocker
		block          func(context.Context) error
		expectedReason string
	}{
		{
			desc:           "write lock timeout",
			housekeeping:   noopHousekeeping{},
			block:          func(context.Context) error { return ErrBlockTimeout },
			expectedReason: "lock_timeout",
		},
		{
			desc:           "housekeeping running",
			housekeeping:   busyHousekeeping{},
			block:          func(context.Context) error { return nil },
			expectedReason: "housekeeping_running",
		},
		{
			desc:           "reftable modified during the dry-run",
			housekeeping:   noopHousekeeping{},
			block:          func(context.Context) error { return errReftableModified },
			expectedReason: "reftable_modified",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			logger := testhelper.NewLogger(t)
			hook := testhelper.AddLoggerHook(logger)
			metrics := NewMetrics()
			m := NewHandler(logger, metrics, tc.housekeeping)

			m.handle(migrationData{ctx: ctx, repo: repo, block: tc.block, unblock: func() error { return nil }})

			val, ok := m.state.Load(repoKeyFor(repo))
			require.True(t, ok)
			state := val.(migratorState)
			require.False(t, state.completed)
			require.Equal(t, uint(1), state.attempts)
			require.True(t, state.coolDown.After(time.Now()), "a busy repository must back off")

			require.Equal(t, float64(1), testutil.ToFloat64(metrics.failsMetric.WithLabelValues(tc.expectedReason)))
			entries := hook.AllEntries()
			require.Equal(t, "MVCC migration postponed for busy repository", entries[len(entries)-1].Message)
			require.Equal(t, "warning", entries[len(entries)-1].Level.String())

			backend, err := gitcmd.DetectReferenceBackend(ctx, repoPath)
			require.NoError(t, err)
			require.NotEqual(t, git.ReferenceBackendMVCC.Name, backend.Name, "a postponed migration must not change the backend")
		})
	}
}

// TestMigrationHandler_missingRefsAfterSwap checks that a repository whose
// swap failed after the config commit and left refs/ missing isn't marked as
// done, so that the next attempt repairs refs/.
func TestMigrationHandler_missingRefsAfterSwap(t *testing.T) {
	gittest.SkipWithoutMVCCGit(t)
	testhelper.SkipWithReftable(t, "checks migration from the files backend")
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	m := NewHandler(testhelper.NewLogger(t), NewMetrics(), noopHousekeeping{})
	noop := func(context.Context) error { return nil }

	// Once the swap is done, lose refs/ and make the cleanup that follows
	// fail, as a crash in the middle of swapRefs would leave things.
	m.handle(migrationData{ctx: ctx, repo: repo, block: noop, unblock: func() error {
		require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "refs")))
		require.NoError(t, os.Chmod(repoPath, 0o500))
		return nil
	}})
	require.NoError(t, os.Chmod(repoPath, mode.Directory))

	val, ok := m.state.Load(repoKeyFor(repo))
	require.True(t, ok)
	require.False(t, val.(migratorState).completed, "a repository without refs/ isn't done")
	require.True(t, val.(migratorState).coolDown.IsZero(), "the repair must not wait for a cooldown")

	// The next RPC registers the migration with a new repository, as a Repo
	// caches the backend it detected.
	m.handle(migrationData{ctx: ctx, repo: localrepo.NewTestRepo(t, cfg, repoProto), block: noop, unblock: func() error { return nil }})

	require.DirExists(t, filepath.Join(repoPath, "refs"), "the next attempt must repair refs/")
	val, ok = m.state.Load(repoKeyFor(repo))
	require.True(t, ok)
	require.True(t, val.(migratorState).completed)
}

// TestMigrationHandler_busyPathsViaRunLoop verifies that the errHousekeepingRunning
// and errReftableModified error paths correctly persist state (attempts,
// cooldown, nil cancelCtx) when going through the full goroutine run loop,
// not just a direct handle() call. This exercises the deferred state-store.
func TestMigrationHandler_busyPathsViaRunLoop(t *testing.T) {
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	for _, tc := range []struct {
		desc           string
		housekeeping   HousekeepingLocker
		block          func() error
		expectedReason string
		needsMVCC      bool
	}{
		{
			// errHousekeepingRunning requires no git operations: TryLockRepository
			// returns false immediately, so migrate() is never entered.
			desc:           "housekeeping running",
			housekeeping:   busyHousekeeping{},
			block:          func() error { return nil },
			expectedReason: "housekeeping_running",
		},
		{
			// errReftableModified is returned by block() after migratePacked()
			// succeeds, which requires MVCC git.
			desc:           "reftable modified",
			housekeeping:   noopHousekeeping{},
			block:          func() error { return errReftableModified },
			expectedReason: "reftable_modified",
			needsMVCC:      true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			if tc.needsMVCC {
				gittest.SkipWithoutMVCCGit(t)
			}

			repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})
			gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
			gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			metrics := NewMetrics()
			parentCtx, parentCancel := context.WithCancel(context.Background())
			m := &Handler{
				ctx:          parentCtx,
				ctxCancel:    parentCancel,
				logger:       testhelper.NewLogger(t),
				metrics:      metrics,
				migrateCh:    make(chan migrationData),
				housekeeping: tc.housekeeping,
				state:        sync.Map{},
			}
			m.Run(testhelper.Context(t), noParent)
			defer m.Close()

			key := repoKeyFor(repo)
			for {
				if val, ok := m.state.Load(key); ok {
					if val.(migratorState).cancelCtx != nil {
						break
					}
				}
				m.RegisterMigration(ctx, repo, ignoreCtx(tc.block), func() error { return nil })
			}

			// Drain: block until the goroutine has finished handle() and stored state.
			m.migrateCh <- migrationData{}

			val, ok := m.state.Load(key)
			require.True(t, ok)
			state := val.(migratorState)
			require.False(t, state.completed)
			require.Equal(t, uint(1), state.attempts)
			require.Nil(t, state.cancelCtx)
			require.True(t, state.coolDown.After(time.Now()))
			require.Equal(t, float64(1), testutil.ToFloat64(metrics.failsMetric.WithLabelValues(tc.expectedReason)))
		})
	}
}

// TestMigrationHandler_housekeepingUnlockOnPanic verifies that a panic inside
// migrate() still releases the housekeeping guard. The guard is held via a
// deferred unlock inside an anonymous closure, so the closure's defer fires
// before the panic propagates to the dontpanic.Try recovery boundary.
func TestMigrationHandler_housekeepingUnlockOnPanic(t *testing.T) {
	gittest.SkipWithoutMVCCGit(t)
	testhelper.SkipWithMVCC(t, "checks migration from the files backend")

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	var unlockCalled sync.WaitGroup
	unlockCalled.Add(1)
	h := &trackingHousekeeping{onUnlock: unlockCalled.Done}
	m := NewHandler(testhelper.NewLogger(t), NewMetrics(), h)

	require.Panics(t, func() {
		m.handle(migrationData{
			ctx:  ctx,
			repo: repo,
			block: func(context.Context) error {
				panic("injected test panic")
			},
			unblock: func() error { return nil },
		})
	})

	// unlockCalled.Wait() would block forever if the guard was not released,
	// making the test hang rather than pass; the require.Panics above already
	// asserts the panic propagated correctly.
	unlockCalled.Wait()
}

// trackingHousekeeping calls onUnlock each time the guard is released.
type trackingHousekeeping struct {
	onUnlock func()
}

func (h *trackingHousekeeping) TryLockRepository(storage.Repository) (func(), bool) {
	return h.onUnlock, true
}
