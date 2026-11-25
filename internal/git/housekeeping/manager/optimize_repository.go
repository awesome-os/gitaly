package manager

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/housekeeping/config"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/reftable"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/tracing"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/codes"
)

// OptimizeRepositoryConfig is the configuration used by OptimizeRepository that is computed by
// applying all the OptimizeRepositoryOption modifiers.
type OptimizeRepositoryConfig struct {
	StrategyConstructor OptimizationStrategyConstructor
}

// OptimizeRepositoryOption is an option that can be passed to OptimizeRepository.
type OptimizeRepositoryOption func(cfg *OptimizeRepositoryConfig)

// OptimizationStrategyConstructor is a constructor for an OptimizationStrategy that is being
// informed by the passed-in RepositoryInfo.
type OptimizationStrategyConstructor func(stats.RepositoryInfo) housekeeping.OptimizationStrategy

// WithOptimizationStrategyConstructor changes the constructor for the optimization strategy.that is
// used to determine which parts of the repository will be optimized. By default the
// HeuristicalOptimizationStrategy is used.
func WithOptimizationStrategyConstructor(strategyConstructor OptimizationStrategyConstructor) OptimizeRepositoryOption {
	return func(cfg *OptimizeRepositoryConfig) {
		cfg.StrategyConstructor = strategyConstructor
	}
}

// OptimizeRepository performs optimizations on the repository. Whether optimizations are performed
// or not depends on a set of heuristics.
func (m *RepositoryManager) OptimizeRepository(
	ctx context.Context,
	repo *localrepo.Repo,
	opts ...OptimizeRepositoryOption,
) error {
	var cfg OptimizeRepositoryConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	span, ctx := tracing.StartSpanIfHasParent(ctx, "housekeeping.OptimizeRepository", nil)
	defer span.End()

	// tryRunningHousekeeping acquires a lock on the repository to prevent other concurrent housekeeping calls on the repository.
	ok, cleanup := m.repositoryStates.tryRunningHousekeeping(repo)
	// If we didn't succeed to set the state to "running" because of a concurrent housekeeping run
	// we exit early.
	if !ok {
		return nil
	}
	defer cleanup()

	if m.optimizeFunc != nil {
		return m.withMVCCCache(ctx, repo, func(ctx context.Context) error {
			strategy, err := m.validate(ctx, repo, cfg)
			if err != nil {
				return err
			}
			return m.optimizeFunc(ctx, repo, strategy)
		})
	}

	return m.withMVCCCache(ctx, repo, func(ctx context.Context) error {
		return m.optimizeRepository(ctx, repo, cfg)
	})
}

// withMVCCCache runs fn within an MVCC cache lifecycle for MVCC-backed
// repositories, so background housekeeping (which does not pass through the mvcc
// gRPC middleware) operates on the current manifest snapshot and publishes the
// repacked result as a new manifest. This wrapper hydrates the base snapshot
// beforehand and commits the pointer afterward.
//
// It is a passthrough (just runs fn) when:
//   - a cache is already active on the context. This happens for the
//     OptimizeRepository RPC, whose handler runs inside the mvcc middleware: the
//     middleware owns that lifecycle and its Commit, so we must not wrap again.
//     Housekeeping triggered by an arbitrary RPC does not end up here, because the
//     housekeeping middleware is registered as the outer interceptor and its
//     detached goroutine therefore never sees the mvcc cache;
//   - MVCC cache support was not configured (see WithMVCCCacheSupport); or
//   - the repository does not use the MVCC reference backend.
func (m *RepositoryManager) withMVCCCache(ctx context.Context, repo *localrepo.Repo, fn func(context.Context) error) error {
	if mvcc.CacheFromContext(ctx) != nil || m.cacheFactory == nil {
		return fn(ctx)
	}

	backend, err := repo.ReferenceBackend(ctx)
	if err != nil {
		return fmt.Errorf("detect reference backend: %w", err)
	}
	if backend.Name != git.ReferenceBackendMVCC.Name {
		return fn(ctx)
	}

	// read-write cache: housekeeping repacks and publishes a new manifest.
	cache, err := m.cacheFactory.NewCache(ctx, repo.GetStorageName(), repo.GetRelativePath(), false)
	if err != nil {
		return fmt.Errorf("new mvcc cache: %w", err)
	}
	cache.Prepare(ctx)
	defer cache.Cleanup()

	// Wait for the cache to be hydrated before running the optimization. On the
	// RPC path readiness is awaited lazily by the first Git command, but an
	// optimization run may spawn no Git command at all (the strategy can decide
	// nothing needs to be done), in which case Commit would race with the
	// asynchronous preparation. Housekeeping gains nothing from the asynchronous
	// warm-up anyway, so wait here and fail early with a clear error.
	if err := cache.WaitUntilReady(ctx); err != nil {
		return fmt.Errorf("prepare mvcc cache: %w", err)
	}

	ctx = mvcc.ContextWithCache(ctx, cache)
	ctx = gitcmd.ContextWithMVCC(ctx)

	if err := fn(ctx); err != nil {
		return err
	}

	// Publish through the serialized path so housekeeping takes the same
	// per-repository publish lock as RPC writes and merges on a CAS conflict,
	// rather than racing an RPC publish on the durable pointer and failing. The
	// reftable callbacks the 3-way merge needs are wired here, where importing
	// internal/git/reftable is safe.
	_, err = mvcc.PublishSerialized(ctx, cache, mvcc.RetryOpts{
		MergeOpts: mvcc.MergeOpts{
			ReadReftableRefs:         reftable.ReadRefsAt,
			ReadReftableUpdateRange:  reftable.ReadUpdateRangeAt,
			PatchReftableUpdateRange: reftable.PatchUpdateRangeAt,
		},
		Logger: m.logger,
	})
	return err
}

func (m *RepositoryManager) validate(
	ctx context.Context,
	repo *localrepo.Repo,
	cfg OptimizeRepositoryConfig,
) (housekeeping.OptimizationStrategy, error) {
	repositoryInfo, err := stats.RepositoryInfoForRepository(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("deriving repository info: %w", err)
	}

	repositoryInfo.Log(ctx, m.logger)
	m.metrics.ReportRepositoryInfo(repositoryInfo)

	var strategy housekeeping.OptimizationStrategy
	if cfg.StrategyConstructor == nil {
		strategy = housekeeping.NewHeuristicalOptimizationStrategy(repositoryInfo)
	} else {
		strategy = cfg.StrategyConstructor(repositoryInfo)
	}

	return strategy, nil
}

func (m *RepositoryManager) optimizeRepository(
	ctx context.Context,
	repo *localrepo.Repo,
	cfg OptimizeRepositoryConfig,
) error {
	strategy, err := m.validate(ctx, repo, cfg)
	if err != nil {
		return err
	}

	var cleanStaleDataTime, cleanWorktreesTime, repackTime, packRefsTime,
		pruneTime, commitGraphTime time.Duration

	finishTotalTimer := m.metrics.ReportTaskLatency("total", "apply")
	totalStatus := "failure"

	optimizations := make(map[string]string)
	defer func() {
		totalTime := finishTotalTimer()

		m.logger.WithFields(logrus.Fields{
			"correlation_id":        correlation.ExtractFromContext(ctx),
			"optimizations":         optimizations,
			"total_time":            totalTime,
			"clean_stale_data_time": cleanStaleDataTime,
			"clean_work_trees_time": cleanWorktreesTime,
			"repack_time":           repackTime,
			"pack_refs_time":        packRefsTime,
			"prune_time":            pruneTime,
			"commit_graph_time":     commitGraphTime,
			"storage_name":          repo.Repository.GetStorageName(),
			"relative_path":         repo.Repository.GetRelativePath(),
			"gl_project_path":       repo.Repository.GetGlProjectPath(),
			"gl_repo":               repo.Repository.GetGlRepository(),
		}).Info("optimized repository")

		for task, status := range optimizations {
			m.metrics.TasksTotal.WithLabelValues(task, status).Inc()
		}

		m.metrics.TasksTotal.WithLabelValues("total", totalStatus).Add(1)
	}()

	cleanupCfg := housekeeping.DefaultStaleDataCleanup()

	finishTimer := m.metrics.ReportTaskLatency("clean-stale-data", "apply")
	if err := m.CleanStaleData(ctx, repo, cleanupCfg); err != nil {
		return fmt.Errorf("could not execute housekeeping: %w", err)
	}
	cleanStaleDataTime = finishTimer()

	finishTimer = m.metrics.ReportTaskLatency("clean-worktrees", "apply")
	if err := housekeeping.CleanupWorktrees(ctx, repo); err != nil {
		return fmt.Errorf("could not clean up worktrees: %w", err)
	}
	cleanWorktreesTime = finishTimer()

	finishTimer = m.metrics.ReportTaskLatency("repack", "apply")
	didRepack, repackCfg, err := repackIfNeeded(ctx, repo, strategy)
	if err != nil {
		optimizations["packed_objects_"+string(repackCfg.Strategy)] = "failure"
		if repackCfg.WriteBitmap {
			optimizations["written_bitmap"] = "failure"
		}
		if repackCfg.WriteMultiPackIndex {
			optimizations["written_multi_pack_index"] = "failure"
		}

		return fmt.Errorf("could not repack: %w", err)
	}
	if didRepack {
		optimizations["packed_objects_"+string(repackCfg.Strategy)] = "success"

		if repackCfg.WriteBitmap {
			optimizations["written_bitmap"] = "success"
		}
		if repackCfg.WriteMultiPackIndex {
			optimizations["written_multi_pack_index"] = "success"
		}
	}
	repackTime = finishTimer()

	finishTimer = m.metrics.ReportTaskLatency("prune", "apply")
	didPrune, err := pruneIfNeeded(ctx, repo, strategy)
	if err != nil {
		optimizations["pruned_objects"] = "failure"
		return fmt.Errorf("could not prune: %w", err)
	} else if didPrune {
		optimizations["pruned_objects"] = "success"
	}
	pruneTime = finishTimer()

	finishTimer = m.metrics.ReportTaskLatency("pack-refs", "apply")
	didPackRefs, err := m.packRefsIfNeeded(ctx, repo, strategy)
	if err != nil {
		optimizations["packed_refs"] = "failure"
		return fmt.Errorf("could not pack refs: %w", err)
	} else if didPackRefs {
		optimizations["packed_refs"] = "success"
	}
	packRefsTime = finishTimer()

	finishTimer = m.metrics.ReportTaskLatency("commit-graph", "apply")
	if didWriteCommitGraph, writeCommitGraphCfg, err := writeCommitGraphIfNeeded(ctx, repo, strategy); err != nil {
		optimizations["written_commit_graph_full"] = "failure"
		optimizations["written_commit_graph_incremental"] = "failure"
		return fmt.Errorf("could not write commit-graph: %w", err)
	} else if didWriteCommitGraph {
		if writeCommitGraphCfg.ReplaceChain {
			optimizations["written_commit_graph_full"] = "success"
		} else {
			optimizations["written_commit_graph_incremental"] = "success"
		}
	}
	commitGraphTime = finishTimer()

	totalStatus = "success"

	return nil
}

// repackIfNeeded repacks the repository according to the strategy.
func repackIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy housekeeping.OptimizationStrategy) (bool, config.RepackObjectsConfig, error) {
	repackNeeded, cfg := strategy.ShouldRepackObjects(ctx)
	if !repackNeeded {
		return false, config.RepackObjectsConfig{}, nil
	}

	if err := housekeeping.RepackObjects(ctx, repo, cfg); err != nil {
		return false, cfg, err
	}

	return true, cfg, nil
}

// writeCommitGraphIfNeeded writes the commit-graph if required.
func writeCommitGraphIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy housekeeping.OptimizationStrategy) (bool, config.WriteCommitGraphConfig, error) {
	needed, cfg, err := strategy.ShouldWriteCommitGraph(ctx)
	if !needed || err != nil {
		return false, config.WriteCommitGraphConfig{}, err
	}

	if err := housekeeping.WriteCommitGraph(ctx, repo, cfg); err != nil {
		return true, cfg, fmt.Errorf("writing commit-graph: %w", err)
	}

	return true, cfg, nil
}

// pruneIfNeeded removes objects from the repository which are either unreachable or which are
// already part of a packfile. We use a grace period of two weeks.
func pruneIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy housekeeping.OptimizationStrategy) (bool, error) {
	needed, cfg := strategy.ShouldPruneObjects(ctx)
	if !needed {
		return false, nil
	}

	if err := housekeeping.PruneObjects(ctx, repo, cfg); err != nil {
		return true, fmt.Errorf("pruning objects: %w", err)
	}

	return true, nil
}

func (m *RepositoryManager) packRefsIfNeeded(ctx context.Context, repo *localrepo.Repo, strategy housekeeping.OptimizationStrategy) (bool, error) {
	backend, err := repo.ReferenceBackend(ctx)
	if err != nil {
		return false, fmt.Errorf("detect reference backend: %w", err)
	}

	// Never repack references of MVCC repositories as reftable compaction will automatically
	// take place during writes.
	if backend.Name == git.ReferenceBackendMVCC.Name {
		return false, nil
	}

	if !strategy.ShouldRepackReferences(ctx) {
		return false, nil
	}

	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx, gitcmd.Command{
		Name: "pack-refs",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "--auto"},
			gitcmd.Flag{Name: "--all"},
		},
	}, gitcmd.WithStderr(&stderr)); err != nil {
		return false, fmt.Errorf("packing refs: %w, stderr: %q", err, stderr.String())
	}

	return true, nil
}

// CleanStaleData removes any stale data in the repository as per the provided configuration.
func (m *RepositoryManager) CleanStaleData(ctx context.Context, repo *localrepo.Repo, cfg housekeeping.CleanStaleDataConfig) error {
	span, ctx := tracing.StartSpanIfHasParent(ctx, "housekeeping.CleanStaleData", nil)
	defer span.End()

	repoPath, err := repo.Path(ctx)
	if err != nil {
		m.logger.WithError(err).WarnContext(ctx, "housekeeping failed to get repo path")
		if structerr.GRPCCode(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("housekeeping failed to get repo path: %w", err)
	}

	staleDataByType := map[string]int{}
	defer func() {
		if len(staleDataByType) == 0 {
			return
		}

		logEntry := m.logger
		for staleDataType, count := range staleDataByType {
			logEntry = logEntry.WithField(fmt.Sprintf("stale_data.%s", staleDataType), count)
			m.metrics.PrunedFilesTotal.WithLabelValues(staleDataType).Add(float64(count))
		}
		logEntry.InfoContext(ctx, "removed files")
	}()

	var entriesToPrune []string
	for staleEntryType, staleFileFinder := range cfg.StaleFileFinders {
		staleEntries, err := staleFileFinder(ctx, repoPath)
		if err != nil {
			return fmt.Errorf("housekeeping failed to find %s: %w", staleEntryType, err)
		}

		entriesToPrune = append(entriesToPrune, staleEntries...)
		staleDataByType[staleEntryType] = len(staleEntries)
	}

	for _, path := range entriesToPrune {
		// Remove stale entry (file or directory) - os.RemoveAll handles both cases
		// by first calling Remove before going for recursive removal.
		if err := os.RemoveAll(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			staleDataByType["failures"]++
			m.logger.WithError(err).WithField("path", path).WarnContext(ctx, "unable to remove stale entry")
		}
	}

	for repoCleanupName, repoCleanupFn := range cfg.RepoCleanups {
		cleanupCount, err := repoCleanupFn(ctx, repo)
		staleDataByType[repoCleanupName] = cleanupCount
		if err != nil {
			return fmt.Errorf("housekeeping could not perform cleanup %s: %w", repoCleanupName, err)
		}
	}

	for repoCleanupName, repoCleanupFn := range cfg.RepoCleanupWithTxManagers {
		cleanupCount, err := repoCleanupFn(ctx, repo, m.txManager)
		staleDataByType[repoCleanupName] = cleanupCount
		if err != nil {
			return fmt.Errorf("housekeeping could not perform cleanup (with TxManager) %s: %w", repoCleanupName, err)
		}
	}

	return nil
}
