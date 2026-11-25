package mvcc

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
)

// testRepositoryID is the repository the merge tests operate on. Its exact
// value is irrelevant; it only has to be a valid relative path.
const testRepositoryID = "@hashed/aa/bb/repo.git"

// newTestMerger returns a Merger backed by a LocalStorage rooted in a fresh
// temporary directory, along with the local cache directory that the merge
// resolves manifest paths against. Tests seed artifacts by writing them
// straight into that directory.
func newTestMerger(t *testing.T) (*Merger, string) {
	t.Helper()

	storageRoot := t.TempDir()
	cacheDir := filepath.Join(storageRoot, testRepositoryID, mvccCacheDir)
	require.NoError(t, os.MkdirAll(cacheDir, mode.Directory))

	storage := NewLocalStorage(storageRoot)
	t.Cleanup(func() { require.NoError(t, storage.TearDown(testhelper.Context(t))) })

	return NewMerger(storage, testRepositoryID, cacheDir), cacheDir
}

// writeManifestBody serialises m, writes the body into the local cache and
// returns its sha. Used by the merge tests to seed the manifest bodies that
// ThreeWayMerge reads and rewrites, without going through a full Git
// invocation.
//
// The manifest is serialised via SerializeManifest (the binary chunk-based
// format) so the body that hits the cache is bit-identical to what a real Git
// write would produce.
func writeManifestBody(t *testing.T, s *Merger, m *Manifest) string {
	t.Helper()
	// Default to SHA-1 for tests that don't care about the algorithm.
	if m.HashAlgo == 0 {
		m.HashAlgo = hashAlgoSHA1
	}
	body, sha, err := SerializeManifest(m)
	require.NoError(t, err)

	manifestsDir := filepath.Join(s.cacheDir, manifestDirectory)
	require.NoError(t, os.MkdirAll(manifestsDir, mode.Directory))
	require.NoError(t, os.WriteFile(filepath.Join(manifestsDir, sha), body, mode.File))

	return sha
}

// manifestFilePath returns where the manifest with the given hash is stored below cacheDir.
func manifestFilePath(tb testing.TB, cacheDir, hash string) string {
	tb.Helper()

	relPath, err := ManifestRelPath(hash)
	require.NoError(tb, err)

	return filepath.Join(cacheDir, relPath)
}

// newManifest is a tiny convenience that builds a Manifest from
// separate pack and reftable path slices; packPaths are its OBJS. The result is round-tripped
// through SerializeManifest+ParseManifest so the returned Manifest has
// all derived fields (objStart, objCount, refStackOrder, sorted Paths)
// populated — i.e. it looks exactly like a manifest read back from
// the cache or object storage.
func newManifest(packPaths, refStack []string) *Manifest {
	m, err := assembleManifest(hashAlgoSHA1, packPaths, refStack, nil)
	if err != nil {
		panic("newManifest: assemble: " + err.Error())
	}
	body, _, err := SerializeManifest(m)
	if err != nil {
		panic("newManifest: serialize: " + err.Error())
	}
	parsed, err := ParseManifest(body)
	if err != nil {
		panic("newManifest: parse: " + err.Error())
	}
	return parsed
}

// newDetachedTestMerger returns a Merger whose durable storage is rooted in a
// different directory from its local cache, plus both directories. newTestMerger
// collapses the two — LocalStorage serves artifacts straight out of the cache
// directory — which makes a missing fetchArtifact call invisible. Here an
// artifact seeded into durableDir only becomes readable under cacheDir once the
// merge has actually fetched it.
func newDetachedTestMerger(t *testing.T) (merger *Merger, cacheDir, durableDir string) {
	t.Helper()

	durableRoot := t.TempDir()
	durableDir = filepath.Join(durableRoot, testRepositoryID, mvccCacheDir)
	require.NoError(t, os.MkdirAll(durableDir, mode.Directory))

	cacheDir = filepath.Join(t.TempDir(), testRepositoryID, mvccCacheDir)
	require.NoError(t, os.MkdirAll(cacheDir, mode.Directory))

	storage := NewLocalStorage(durableRoot)
	t.Cleanup(func() { require.NoError(t, storage.TearDown(testhelper.Context(t))) })

	return NewMerger(storage, testRepositoryID, cacheDir), cacheDir, durableDir
}

// gitManifest lays out a manifest body the way Git writes one, without going
// through SerializeManifest: objs are the OBJS run whatever their prefix,
// refs the REFS stack (oldest first), other the paths in PATH outside both,
// and extra any further chunks, written after REFS. It returns the parsed
// manifest and its body.
func gitManifest(tb testing.TB, objs, refs, other []string, extra ...chunkSpec) (*Manifest, []byte) {
	tb.Helper()

	paths := slices.Concat(objs, refs, other)
	sort.Strings(paths)

	width := 16
	for _, p := range paths {
		width = max(width, (len(p)+1+3)&^3)
	}

	index := func(p string) uint32 {
		i := slices.Index(paths, p)
		if i < 0 {
			tb.Fatalf("gitManifest: %q not in PATH", p)
		}
		return uint32(i)
	}

	var objStart uint32
	if len(objs) > 0 {
		objStart = index(slices.Min(objs))
		for i := range objs {
			if !slices.Contains(objs, paths[int(objStart)+i]) {
				tb.Fatalf("gitManifest: OBJS %v is not one run in %v", objs, paths)
			}
		}
	}
	var refIdx []uint32
	for _, r := range refs {
		refIdx = append(refIdx, index(r))
	}

	chunks := []chunkSpec{
		{chunkIDPath, pathChunkData(width, paths...)},
		{chunkIDObjs, objsChunkData(objStart, uint32(len(objs)))},
		{chunkIDRefs, refsChunkData(refIdx...)},
	}
	body := buildManifest(tb, manifestVersion, byte(width), hashAlgoSHA1, append(chunks, extra...))

	m, err := ParseManifest(body)
	if err != nil {
		tb.Fatalf("gitManifest: parse: %v", err)
	}
	return m, body
}

// mergeGitManifests runs a pack-only ThreeWayMerge of the given manifests,
// with ours' body seeded in the cache and named by the pointer, and returns
// the merged manifest as read back from the cache.
func mergeGitManifests(t *testing.T, base, ours, theirs *Manifest, oursBody []byte) (*Manifest, error) {
	t.Helper()
	ctx := testhelper.Context(t)
	s, cacheDir := newTestMerger(t)

	oursSha := fmt.Sprintf("%x", oursBody[len(oursBody)-trailerSize:])
	manifestsDir := filepath.Join(cacheDir, manifestDirectory)
	require.NoError(t, os.MkdirAll(manifestsDir, mode.Directory))
	require.NoError(t, os.WriteFile(filepath.Join(manifestsDir, oursSha), oursBody, mode.File))
	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), mode.File))

	if err := s.ThreeWayMerge(ctx, pointerPath, ours, base, theirs, MergeOpts{
		ReadReftableUpdateRange:  panickyReadRange,
		PatchReftableUpdateRange: panickyPatchRange,
	}); err != nil {
		return nil, err
	}

	newSha, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	// writeAndPointManifest shards the merged manifest into a fanout directory by hash.
	body, err := os.ReadFile(manifestFilePath(t, cacheDir, string(newSha)))
	require.NoError(t, err)
	merged, err := ParseManifest(body)
	require.NoError(t, err)
	return merged, nil
}

// outsidePaths returns m's paths that are in neither OBJS nor REFS.
func outsidePaths(m *Manifest) []string {
	var out []string
	for _, p := range m.Paths {
		if !slices.Contains(m.ObjPaths(), p) && !slices.Contains(m.RefPaths(), p) {
			out = append(out, p)
		}
	}
	return out
}
