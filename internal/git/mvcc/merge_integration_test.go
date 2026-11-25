package mvcc

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
)

// The merge operates on reftables only through the injected MergeOpts callbacks,
// so these tests model a reftable as a tiny text file:
//
//	range <min> <max>
//	<refname> <target>
//	...
//
// That lets the merge's rebase (range rewrite + content-addressing) and overlap
// detection run end-to-end without a real reftable or the bundled Git binary.

func writeFakeReftable(t *testing.T, path string, minIdx, maxIdx uint64, refs map[string]string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	var b strings.Builder
	fmt.Fprintf(&b, "range %d %d\n", minIdx, maxIdx)
	for name, target := range refs {
		fmt.Fprintf(&b, "%s %s\n", name, target)
	}
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
}

func fakeReadRange(path string) (uint64, uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	var minIdx, maxIdx uint64
	if _, err := fmt.Sscanf(strings.SplitN(string(data), "\n", 2)[0], "range %d %d", &minIdx, &maxIdx); err != nil {
		return 0, 0, err
	}
	return minIdx, maxIdx, nil
}

func fakePatchRange(path string, minIdx, maxIdx uint64) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	rest := ""
	if parts := strings.SplitN(string(data), "\n", 2); len(parts) == 2 {
		rest = parts[1]
	}
	return os.WriteFile(path, []byte(fmt.Sprintf("range %d %d\n%s", minIdx, maxIdx, rest)), 0o644)
}

func fakeReadRefs(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for i, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		if i == 0 || line == "" { // skip the range header
			continue
		}
		name, target, _ := strings.Cut(line, " ")
		out[name] = target
	}
	return out, nil
}

func fakeMergeOpts() MergeOpts {
	return MergeOpts{
		ReadReftableRefs:         fakeReadRefs,
		ReadReftableUpdateRange:  fakeReadRange,
		PatchReftableUpdateRange: fakePatchRange,
	}
}

func refManifest(paths ...string) *Manifest {
	return &Manifest{
		Paths:         append([]string(nil), paths...),
		refStackOrder: append([]string(nil), paths...),
		HashAlgo:      hashAlgoSHA1,
	}
}

func storeManifest(t *testing.T, cacheDir string, m *Manifest) string {
	t.Helper()
	body, sha, err := SerializeManifest(m)
	require.NoError(t, err)
	relPath, err := ManifestRelPath(sha)
	require.NoError(t, err)
	fullPath := filepath.Join(cacheDir, relPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o755))
	require.NoError(t, os.WriteFile(fullPath, body, 0o644))
	return sha
}

// newMergeFixture lays out base/ours/theirs reftables that each add one
// disjoint ref on top of a shared base, and returns a cache wired to the
// resulting on-disk cache dir. ours and theirs are both staged at update-index
// 2, so a rebase of ours past theirs is required.
func newMergeFixture(t *testing.T, oursRefs, theirsRefs map[string]string) (*LocalCache, *Manifest, *Manifest, *Manifest, string) {
	t.Helper()
	storageRoot := t.TempDir()
	const repoID = "repo"
	cacheDir := filepath.Join(storageRoot, repoID, mvccCacheDir)

	writeFakeReftable(t, filepath.Join(cacheDir, "refs/base1.ref"), 1, 1, map[string]string{"refs/heads/main": "a1"})
	writeFakeReftable(t, filepath.Join(cacheDir, "refs/theirs1.ref"), 2, 2, theirsRefs)
	writeFakeReftable(t, filepath.Join(cacheDir, "refs/ours1.ref"), 2, 2, oursRefs)

	base := refManifest("refs/base1.ref")
	theirs := refManifest("refs/base1.ref", "refs/theirs1.ref")
	ours := refManifest("refs/base1.ref", "refs/ours1.ref")

	cache := NewLocalCache(NewLocalStorage(storageRoot), t.TempDir(), storageRoot, repoID, false)
	return cache, base, ours, theirs, cacheDir
}

func TestLocalCache_publishWithRetry_mergesOnConflict(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cache, base, ours, theirs, cacheDir := newMergeFixture(t,
		map[string]string{"refs/heads/topic": "o1"},
		map[string]string{"refs/heads/feature": "t1"},
	)

	baseSha := storeManifest(t, cacheDir, base)
	theirsSha := storeManifest(t, cacheDir, theirs)
	oursSha := storeManifest(t, cacheDir, ours)

	// Seed the durable pointer at theirs: a concurrent writer already won the CAS
	// race against our base.
	pointerPath := filepath.Join(cacheDir, manifestPointer)
	require.NoError(t, os.WriteFile(pointerPath, []byte(theirsSha), 0o644))

	// The per-RPC staged pointer names ours.
	staged := filepath.Join(t.TempDir(), "staged")
	require.NoError(t, os.WriteFile(staged, []byte(oursSha), 0o644))
	cache.environments["GIT_MVCC_MANIFEST_PATH"] = staged
	cache.base = ManifestPointer{Hash: baseSha}
	cache.baseManifest = base

	newSha, err := cache.publishWithRetry(ctx, RetryOpts{MergeOpts: fakeMergeOpts()})
	require.NoError(t, err)

	// The durable pointer advanced to the merged manifest, past theirs.
	durable, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	require.Equal(t, newSha, string(durable))
	require.NotEqual(t, theirsSha, newSha)

	mergedRelPath, err := ManifestRelPath(newSha)
	require.NoError(t, err)
	body, err := os.ReadFile(filepath.Join(cacheDir, mergedRelPath))
	require.NoError(t, err)
	merged, err := ParseManifest(body)
	require.NoError(t, err)
	require.Len(t, merged.RefPaths(), 3, "merge keeps base, theirs, and the rebased ours reftable")
}

func TestLocalCache_publishWithRetry_overlapExhausts(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cache, base, ours, theirs, cacheDir := newMergeFixture(t,
		map[string]string{"refs/heads/shared": "o1"},
		map[string]string{"refs/heads/shared": "t1"},
	)
	baseSha := storeManifest(t, cacheDir, base)
	theirsSha := storeManifest(t, cacheDir, theirs)
	oursSha := storeManifest(t, cacheDir, ours)

	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, manifestPointer), []byte(theirsSha), 0o644))
	staged := filepath.Join(t.TempDir(), "staged")
	require.NoError(t, os.WriteFile(staged, []byte(oursSha), 0o644))
	cache.environments["GIT_MVCC_MANIFEST_PATH"] = staged
	cache.base = ManifestPointer{Hash: baseSha}
	cache.baseManifest = base

	_, err := cache.publishWithRetry(ctx, RetryOpts{MergeOpts: fakeMergeOpts()})
	require.Error(t, err)
	require.Equal(t, string(ConflictKindOverlap), structerr.ExtractMetadata(err)["mvcc_conflict_kind"])
}

func TestLocalCache_publishWithRetry_genesisConflictExhausts(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	// Two writers race the genesis commit: ours builds on an empty base while a
	// concurrent writer already created the repository. There is no common
	// ancestor to merge from, so this must exhaust without attempting a merge (and
	// without opening the empty base manifest).
	storageRoot := t.TempDir()
	const repoID = "repo"
	cacheDir := filepath.Join(storageRoot, repoID, mvccCacheDir)
	require.NoError(t, os.MkdirAll(cacheDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, manifestPointer), []byte("theirs-sha"), 0o644))

	staged := filepath.Join(t.TempDir(), "staged")
	require.NoError(t, os.WriteFile(staged, []byte("ours-sha"), 0o644))

	cache := NewLocalCache(NewLocalStorage(storageRoot), t.TempDir(), storageRoot, repoID, false)
	cache.environments["GIT_MVCC_MANIFEST_PATH"] = staged
	cache.base = ManifestPointer{} // genesis: empty hash
	cache.baseManifest = &Manifest{}

	_, err := cache.publishWithRetry(ctx, RetryOpts{MergeOpts: fakeMergeOpts()})
	require.Error(t, err)
	require.Equal(t, string(ConflictKindCASExhausted), structerr.ExtractMetadata(err)["mvcc_conflict_kind"])
}

func TestPublishSerialized_honorsCommitConflictError(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	// An overlap (both sides set refs/heads/shared differently) is a terminal
	// conflict. UserMergeBranch registers a backend-agnostic error via
	// SetCommitConflictError; PublishSerialized must surface it.
	cache, base, ours, theirs, cacheDir := newMergeFixture(t,
		map[string]string{"refs/heads/shared": "o1"},
		map[string]string{"refs/heads/shared": "t1"},
	)
	baseSha := storeManifest(t, cacheDir, base)
	theirsSha := storeManifest(t, cacheDir, theirs)
	oursSha := storeManifest(t, cacheDir, ours)

	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, manifestPointer), []byte(theirsSha), 0o644))
	staged := filepath.Join(t.TempDir(), "staged")
	require.NoError(t, os.WriteFile(staged, []byte(oursSha), 0o644))
	cache.environments["GIT_MVCC_MANIFEST_PATH"] = staged
	cache.base = ManifestPointer{Hash: baseSha}
	cache.baseManifest = base
	close(cache.prepareDone) // mark the cache ready without running prepare

	sentinel := structerr.NewFailedPrecondition("reference update conflict")
	cache.SetCommitConflictError(sentinel)

	_, err := PublishSerialized(ctx, cache, RetryOpts{MergeOpts: fakeMergeOpts()})
	require.ErrorIs(t, err, sentinel)
	// The registered error flattens the inner error to text, so the conflict kind
	// must be re-applied as metadata or the overlap could not be told apart in logs.
	require.Equal(t, string(ConflictKindOverlap), structerr.ExtractMetadata(err)["mvcc_conflict_kind"])
}

func TestPublishSerialized_recordsCommitMetric(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	// Disjoint ref sets: the retry loop 3-way merges and re-publishes, so the
	// publish succeeds and is counted as ok.
	cache, base, ours, theirs, cacheDir := newMergeFixture(t,
		map[string]string{"refs/heads/topic": "o1"},
		map[string]string{"refs/heads/feature": "t1"},
	)
	baseSha := storeManifest(t, cacheDir, base)
	theirsSha := storeManifest(t, cacheDir, theirs)
	oursSha := storeManifest(t, cacheDir, ours)

	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, manifestPointer), []byte(theirsSha), 0o644))
	staged := filepath.Join(t.TempDir(), "staged")
	require.NoError(t, os.WriteFile(staged, []byte(oursSha), 0o644))
	cache.environments["GIT_MVCC_MANIFEST_PATH"] = staged
	cache.base = ManifestPointer{Hash: baseSha}
	cache.baseManifest = base
	cache.metrics = newCacheMetrics()
	close(cache.prepareDone) // mark the cache ready without running prepare

	_, err := PublishSerialized(ctx, cache, RetryOpts{MergeOpts: fakeMergeOpts()})
	require.NoError(t, err)
	require.Equal(t, float64(1), testutil.ToFloat64(cache.metrics.commitTotal.WithLabelValues("ok")),
		"a serialized publish must count its outcome; otherwise gitaly_mvcc_commit_total goes dark once writes publish through this path")
}
