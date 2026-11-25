package mvcc

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
)

// panickyReadRange / panickyPatchRange are placeholder callbacks for
// tests where the rebase pipeline should not be invoked (e.g. when
// both sides have no reftables). They fail loudly if called.
func panickyReadRange(string) (uint64, uint64, error) {
	panic("rebase callback invoked unexpectedly")
}

func panickyPatchRange(string, uint64, uint64) error {
	panic("rebase callback invoked unexpectedly")
}

// disjointTableRefs is a ReadReftableRefs stub for rebase-focused tests that do
// not exercise overlap. It gives each reftable a unique refname, so no two
// tables ever set the same ref and checkValueLevelOverlap always passes. Those
// tests change refs on both sides (that is what forces a rebase), so
// ThreeWayMerge now requires a reader; this provides a minimal one.
func disjointTableRefs(path string) (map[string]string, error) {
	return map[string]string{
		"refs/heads/" + filepath.Base(path): "1111111111111111111111111111111111111111",
	}, nil
}

// expandPack expands a single pack-hash slug into the three pack/<h>.{idx,pack,rev}
// paths the binary manifest stores in its OBJS chunk.
func expandPack(hash string) []string {
	return []string{
		"pack/" + hash + ".idx",
		"pack/" + hash + ".pack",
		"pack/" + hash + ".rev",
	}
}

// expandPacks expands several pack-hash slugs and concatenates the
// resulting pack/*.{idx,pack,rev} paths in lexicographic order.
func expandPacks(hashes ...string) []string {
	var out []string
	for _, h := range hashes {
		out = append(out, expandPack(h)...)
	}
	return out
}

func TestMerger_ThreeWayMerge_packMerge(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	// Base has pack P0. Ours adds S1, S2. Theirs adds D1.
	// Merged paths must include P0, D1, S1, S2 (all three .idx/.pack/.rev each).
	base := newManifest(expandPacks("p0"), nil)
	writeManifestBody(t, s, base)

	ours := newManifest(expandPacks("p0", "s1", "s2"), nil)
	oursSha := writeManifestBody(t, s, ours)

	theirs := newManifest(expandPacks("p0", "d1"), nil)

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, ours, base, theirs, MergeOpts{
		ReadReftableUpdateRange:  panickyReadRange,
		PatchReftableUpdateRange: panickyPatchRange,
	})
	require.NoError(t, err)

	// The merged sha is written to the pointer file; read it back.
	newSha, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	require.NotEqual(t, oursSha, string(newSha))

	// Verify the merged manifest body.
	body, err := os.ReadFile(manifestFilePath(t, cacheDir, string(newSha)))
	require.NoError(t, err)
	m, err := ParseManifest(body)
	require.NoError(t, err)

	// All four packs should be present (P0, D1, S1, S2) — three paths each.
	require.Len(t, m.ObjPaths(), 12, "four packs * three files (idx/pack/rev)")

	gotPacks := map[string]struct{}{}
	for _, p := range m.ObjPaths() {
		gotPacks[p] = struct{}{}
	}
	for _, want := range []string{
		"pack/p0.pack", "pack/p0.idx", "pack/p0.rev",
		"pack/d1.pack", "pack/d1.idx", "pack/d1.rev",
		"pack/s1.pack", "pack/s1.idx", "pack/s1.rev",
		"pack/s2.pack", "pack/s2.idx", "pack/s2.rev",
	} {
		_, ok := gotPacks[want]
		require.True(t, ok, "merged manifest must contain %s", want)
	}
}

func TestMerger_ThreeWayMerge_rebaseCallbackInvoked(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	theirsHash := "deadbeeftheirs"
	stagedHash := "feedfacestaged"
	theirsPath := "refs/" + theirsHash + ".ref"
	stagedPath := "refs/" + stagedHash + ".ref"
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, theirsPath), []byte("theirs bytes"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, stagedPath), []byte("staged bytes"), 0o644))

	base := newManifest(expandPacks("p"), nil)
	baseSha := writeManifestBody(t, s, base)
	_ = baseSha

	oursMfst := newManifest(expandPacks("p"), []string{stagedPath})
	oursSha := writeManifestBody(t, s, oursMfst)

	theirsMfst := newManifest(expandPacks("p"), []string{theirsPath})

	var (
		mu                    sync.Mutex
		readPaths, patchPaths []string
	)
	readRange := func(path string) (uint64, uint64, error) {
		mu.Lock()
		readPaths = append(readPaths, filepath.Base(path))
		mu.Unlock()
		if filepath.Base(path) == theirsHash+".ref" {
			return 10, 10, nil
		}
		return 1, 1, nil
	}
	patchRange := func(path string, min, max uint64) error {
		mu.Lock()
		patchPaths = append(patchPaths, filepath.Base(path))
		mu.Unlock()
		require.Equal(t, uint64(11), min, "staged min 1 rebased onto theirsMax 10 -> 11")
		require.Equal(t, uint64(11), max)
		return os.WriteFile(path, []byte("rebased bytes"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         disjointTableRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.NoError(t, err)

	require.Contains(t, readPaths, theirsHash+".ref", "theirs reftable range must be queried")
	require.Contains(t, readPaths, stagedHash+".ref", "staged reftable range must be queried")
	require.Len(t, patchPaths, 1, "patch must be invoked exactly once per staged reftable")

	newSha, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	body, err := os.ReadFile(manifestFilePath(t, cacheDir, string(newSha)))
	require.NoError(t, err)
	m, err := ParseManifest(body)
	require.NoError(t, err)
	require.Equal(t, []string{theirsPath}, m.RefPaths()[:1], "theirs reftable appears first")
	require.Len(t, m.RefPaths(), 2, "single staged reftable rebased into a single rebased entry")
	require.NotEqual(t, stagedPath, m.RefPaths()[1], "rebased path must differ from staged path")
}

func TestMerger_ThreeWayMerge_overlapReturnsError(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))
	rsPath := "refs/rs.ref"
	rwPath := "refs/rw.ref"
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, rsPath), []byte("fake-rs"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, rwPath), []byte("fake-rw"), 0o644))

	base := newManifest(nil, nil)
	oursMfst := newManifest(nil, []string{rsPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{rwPath})

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs: func(path string) (map[string]string, error) {
			// Both sides touch refs/heads/main, with DIFFERENT
			// targets. This is a real value-level conflict that
			// the merge cannot resolve. Same-value writes are
			// covered by TestMerger_ThreeWayMerge_sameValueNotOverlap.
			switch filepath.Base(path) {
			case "rs.ref":
				return map[string]string{"refs/heads/main": "oid-from-ours"}, nil
			case "rw.ref":
				return map[string]string{"refs/heads/main": "oid-from-theirs"}, nil
			}
			t.Fatalf("readRefs invoked on unexpected reftable %q", path)
			return nil, nil
		},
		ReadReftableUpdateRange:  func(string) (uint64, uint64, error) { return 1, 1, nil },
		PatchReftableUpdateRange: func(string, uint64, uint64) error { return nil },
	})
	require.ErrorIs(t, err, ErrOverlap)
}

func TestMerger_ThreeWayMerge_missingCallbacksRejected(t *testing.T) {
	t.Parallel()

	read := func(string) (uint64, uint64, error) { return 0, 0, nil }
	patch := func(string, uint64, uint64) error { return nil }

	for _, tc := range []struct {
		name string
		opts MergeOpts
	}{
		{name: "both nil", opts: MergeOpts{}},
		{name: "read only", opts: MergeOpts{ReadReftableUpdateRange: read}},
		{name: "patch only", opts: MergeOpts{PatchReftableUpdateRange: patch}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := testhelper.Context(t)
			s, _ := newTestMerger(t)

			pointerPath := filepath.Join(t.TempDir(), "pointer")
			require.NoError(t, os.WriteFile(pointerPath, []byte("staged"), 0o644))

			err := s.ThreeWayMerge(ctx, pointerPath, &Manifest{}, &Manifest{}, &Manifest{}, tc.opts)
			require.ErrorContains(t, err, "requires both")
		})
	}
}

// bothSidesChangeRefsFixture lays out base with a pack, ours adding one
// reftable and theirs adding another, so both exclusive ref sets are
// non-empty: the configuration that requires overlap detection (and a rebase).
func bothSidesChangeRefsFixture(t *testing.T) (s *Merger, oursMfst, base, theirsMfst *Manifest, pointerPath string) {
	t.Helper()
	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	oursPath := "refs/ours.ref"
	theirsPath := "refs/theirs.ref"
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, oursPath), []byte("ours bytes"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, theirsPath), []byte("theirs bytes"), 0o644))

	base = newManifest(expandPacks("p"), nil)
	oursMfst = newManifest(expandPacks("p"), []string{oursPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst = newManifest(expandPacks("p"), []string{theirsPath})

	pointerPath = filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))
	return s, oursMfst, base, theirsMfst, pointerPath
}

// When both sides changed refs, overlap detection is mandatory, so a nil
// ReadReftableRefs is a misuse rather than a "no overlap" shortcut.
func TestMerger_ThreeWayMerge_requiresReadRefsWhenBothChangeRefs(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, oursMfst, base, theirsMfst, pointerPath := bothSidesChangeRefsFixture(t)

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		// ReadReftableRefs deliberately nil though both sides changed refs.
		ReadReftableUpdateRange:  func(string) (uint64, uint64, error) { return 1, 1, nil },
		PatchReftableUpdateRange: func(string, uint64, uint64) error { return nil },
	})
	require.ErrorContains(t, err, "requires ReadReftableRefs when both sides changed refs")
}

// A rebase that would push a reftable's update index past uint64 is rejected
// rather than wrapping to a colliding index. Unreachable in practice; guarded
// anyway.
func TestMerger_ThreeWayMerge_rejectsUpdateIndexOverflow(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, oursMfst, base, theirsMfst, pointerPath := bothSidesChangeRefsFixture(t)

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs: disjointTableRefs,
		// theirs' table sits at the uint64 max, so shifting ours above it
		// would overflow.
		ReadReftableUpdateRange: func(path string) (uint64, uint64, error) {
			if filepath.Base(path) == "theirs.ref" {
				return ^uint64(0), ^uint64(0), nil
			}
			return 1, 1, nil
		},
		PatchReftableUpdateRange: func(string, uint64, uint64) error { return nil },
	})
	require.ErrorContains(t, err, "uint64 max")
}

// TestMerger_ThreeWayMerge_sharedPrefixNotRebased locks down the
// invariant that only ours-exclusive reftables (relative to base) are
// fed to the rebase pipeline. Shared reftables must not be patched.
func TestMerger_ThreeWayMerge_sharedPrefixNotRebased(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		sharedR0 = "sharedr0"
		sharedR1 = "sharedr1"
		stagedRS = "stagedrs"
		theirsRW = "theirsrw"
	)
	sharedR0Path := "refs/" + sharedR0 + ".ref"
	sharedR1Path := "refs/" + sharedR1 + ".ref"
	stagedRSPath := "refs/" + stagedRS + ".ref"
	theirsRWPath := "refs/" + theirsRW + ".ref"

	for _, p := range []string{sharedR0Path, sharedR1Path, stagedRSPath, theirsRWPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	base := newManifest(expandPacks("p0"), []string{sharedR0Path, sharedR1Path})
	oursMfst := newManifest(expandPacks("p0"), []string{sharedR0Path, sharedR1Path, stagedRSPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(expandPacks("p0"), []string{sharedR0Path, sharedR1Path, theirsRWPath})

	var mu sync.Mutex
	readPaths := map[string]int{}
	readRange := func(path string) (uint64, uint64, error) {
		base := filepath.Base(path)
		mu.Lock()
		readPaths[base]++
		mu.Unlock()
		switch base {
		case sharedR0 + ".ref":
			return 1, 1, nil
		case sharedR1 + ".ref":
			return 2, 2, nil
		case theirsRW + ".ref":
			return 5, 5, nil
		case stagedRS + ".ref":
			return 3, 3, nil
		default:
			t.Fatalf("readRange invoked on unexpected reftable %q", base)
			return 0, 0, nil
		}
	}

	patchedPaths := map[string]int{}
	patchRange := func(path string, min, max uint64) error {
		base := filepath.Base(path)
		mu.Lock()
		patchedPaths[base]++
		mu.Unlock()
		return os.WriteFile(path, []byte("rebased-"+base), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         disjointTableRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.NoError(t, err)

	// Only the exclusive reftables should be read. The shared tables
	// are never read because reftableMaxIndex only reads the last
	// table in the stack, which here is the exclusive theirsRW.
	require.Equal(t, 1, readPaths[stagedRS+".ref"], "staged-exclusive RS read once")
	require.Equal(t, 1, readPaths[theirsRW+".ref"], "theirs-exclusive RW read once")
	require.Zero(t, readPaths[sharedR0+".ref"], "shared R0 must not be read")
	require.Zero(t, readPaths[sharedR1+".ref"], "shared R1 must not be read")

	// Shared reftables must never be patched.
	require.NotContains(t, patchedPaths, sharedR0+".ref")
	require.NotContains(t, patchedPaths, sharedR1+".ref")
	require.NotContains(t, patchedPaths, theirsRW+".ref")

	totalPatches := 0
	for _, n := range patchedPaths {
		totalPatches += n
	}
	require.Equal(t, 1, totalPatches, "patchRange invoked exactly once (for the single exclusive staged reftable)")

	// Verify merged manifest structure.
	newSha, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	body, err := os.ReadFile(manifestFilePath(t, cacheDir, string(newSha)))
	require.NoError(t, err)
	m, err := ParseManifest(body)
	require.NoError(t, err)
	require.Equal(t,
		[]string{sharedR0Path, sharedR1Path, theirsRWPath},
		m.RefPaths()[:3],
		"merged manifest opens with the theirs stack verbatim",
	)
	require.Len(t, m.RefPaths(), 4, "exactly one rebased entry appended")
	require.NotEqual(t, stagedRSPath, m.RefPaths()[3])
}

// TestMerger_ThreeWayMerge_lastTableShared exercises the fallback
// path in reftableMaxIndex: the last reftable in the theirs stack is a
// shared table (present in both base and theirs), so its range must be
// read directly rather than reused from the exclusive-table parse.
func TestMerger_ThreeWayMerge_lastTableShared(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		sharedR0 = "sharedlast"
		stagedRS = "stagedonly"
	)
	sharedR0Path := "refs/" + sharedR0 + ".ref"
	stagedRSPath := "refs/" + stagedRS + ".ref"
	for _, p := range []string{sharedR0Path, stagedRSPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	// Base and theirs share the same single reftable. Theirs has no
	// exclusive reftables — only ours adds one.
	base := newManifest(nil, []string{sharedR0Path})
	oursMfst := newManifest(nil, []string{sharedR0Path, stagedRSPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{sharedR0Path})

	var mu sync.Mutex
	readPaths := map[string]int{}
	readRange := func(path string) (uint64, uint64, error) {
		base := filepath.Base(path)
		mu.Lock()
		readPaths[base]++
		mu.Unlock()
		switch base {
		case sharedR0 + ".ref":
			return 5, 5, nil // shared table at update-index 5
		case stagedRS + ".ref":
			return 3, 3, nil // staged at update-index 3 (below theirs)
		default:
			t.Fatalf("readRange invoked on unexpected reftable %q", base)
			return 0, 0, nil
		}
	}

	patchRange := func(path string, min, max uint64) error {
		// Staged min=3 rebased onto theirsMax=5 → new min=6.
		require.Equal(t, uint64(6), min)
		require.Equal(t, uint64(6), max)
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         disjointTableRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.NoError(t, err)

	// The shared table must be read exactly once (by reftableMaxIndex),
	// not fetched via the parallel fan-out.
	require.Equal(t, 1, readPaths[sharedR0+".ref"],
		"shared last table read once by reftableMaxIndex")
	require.Equal(t, 1, readPaths[stagedRS+".ref"],
		"staged-exclusive table read once in step 2")

	// Verify merged manifest: theirs stack (sharedR0) + rebased ours.
	newSha, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	body, err := os.ReadFile(manifestFilePath(t, cacheDir, string(newSha)))
	require.NoError(t, err)
	m, err := ParseManifest(body)
	require.NoError(t, err)
	require.Equal(t, sharedR0Path, m.RefPaths()[0])
	require.Len(t, m.RefPaths(), 2)
	require.NotEqual(t, stagedRSPath, m.RefPaths()[1], "rebased path differs from original")
}

// TestMerger_ThreeWayMerge_compactedTheirs_noOverlap verifies the
// value-level overlap check: when theirs has been compacted (so base
// is no longer a prefix of theirs' stack) but the two writers actually
// touched disjoint refs, the merge must succeed rather than falsely
// reporting overlap. A membership-only check would have flagged every
// ref carried forward by the compacted theirs reftable.
func TestMerger_ThreeWayMerge_compactedTheirs_noOverlap(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		baseR0   = "baser0"
		baseR1   = "baser1"
		stagedRS = "stagedrs"
		theirsRC = "theirscompacted"
	)
	baseR0Path := "refs/" + baseR0 + ".ref"
	baseR1Path := "refs/" + baseR1 + ".ref"
	stagedRSPath := "refs/" + stagedRS + ".ref"
	theirsRCPath := "refs/" + theirsRC + ".ref"

	for _, p := range []string{baseR0Path, baseR1Path, stagedRSPath, theirsRCPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	// Base has two tables that carry refs/heads/main and refs/heads/dev.
	// Ours appended a new table touching refs/heads/feature.
	// Theirs *compacted* base's two tables into a single new table
	// (theirsRC) that still carries main and dev (with the same OIDs).
	// Base is not a prefix of theirs: base = [R0, R1] but theirs = [RC].
	base := newManifest(nil, []string{baseR0Path, baseR1Path})
	oursMfst := newManifest(nil, []string{baseR0Path, baseR1Path, stagedRSPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{theirsRCPath})

	const (
		mainOID    = "1111111111111111111111111111111111111111"
		devOID     = "2222222222222222222222222222222222222222"
		featureOID = "3333333333333333333333333333333333333333"
	)
	// Ref contents per table.
	refsByTable := map[string]map[string]string{
		baseR0Path:   {"refs/heads/main": mainOID},
		baseR1Path:   {"refs/heads/dev": devOID},
		stagedRSPath: {"refs/heads/feature": featureOID},
		// Compacted theirs carries main and dev forward unchanged.
		theirsRCPath: {"refs/heads/main": mainOID, "refs/heads/dev": devOID},
	}

	var mu sync.Mutex
	readRefsCalls := map[string]int{}
	readRefs := func(path string) (map[string]string, error) {
		key := filepath.Base(path)
		mu.Lock()
		readRefsCalls[key]++
		mu.Unlock()
		for p, refs := range refsByTable {
			if filepath.Base(p) == key {
				out := make(map[string]string, len(refs))
				for n, t := range refs {
					out[n] = t
				}
				return out, nil
			}
		}
		t.Fatalf("readRefs invoked on unexpected reftable %q", key)
		return nil, nil
	}

	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case baseR0 + ".ref":
			return 1, 1, nil
		case baseR1 + ".ref":
			return 2, 2, nil
		case theirsRC + ".ref":
			return 1, 2, nil // spans the compacted range
		case stagedRS + ".ref":
			return 3, 3, nil
		}
		return 0, 0, nil
	}
	patchRange := func(path string, min, max uint64) error {
		require.Equal(t, uint64(3), min, "staged min 3 rebased past theirs max 2 -> 3")
		require.Equal(t, uint64(3), max)
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.NoError(t, err, "compacted theirs with disjoint ref edits must merge cleanly")

	// The overlap check dedupes reads: each unique reftable across all
	// three stacks is read exactly once. baseR0 and baseR1 are
	// shared between base and ours; theirsRC and stagedRS are each
	// in a single stack. Every path is read once and only once.
	require.Equal(t, 1, readRefsCalls[theirsRC+".ref"], "compacted theirs table read once for value-level diff")
	require.Equal(t, 1, readRefsCalls[baseR0+".ref"], "base/ours shared table R0 deduped to one read")
	require.Equal(t, 1, readRefsCalls[baseR1+".ref"], "base/ours shared table R1 deduped to one read")
	require.Equal(t, 1, readRefsCalls[stagedRS+".ref"], "ours-exclusive table read once for value-level diff")

	// Merged manifest: theirs single compacted table, then ours
	// rebased.
	newSha, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	body, err := os.ReadFile(manifestFilePath(t, cacheDir, string(newSha)))
	require.NoError(t, err)
	m, err := ParseManifest(body)
	require.NoError(t, err)
	require.Equal(t, theirsRCPath, m.RefPaths()[0], "compacted theirs at stack base")
	require.Len(t, m.RefPaths(), 2, "exactly one rebased entry from ours")
}

// TestMerger_ThreeWayMerge_compactedTheirs_realOverlap verifies that
// the value-level check correctly flags an actual ref conflict
// even when compaction is in the picture. Both writers advanced
// refs/heads/main to different OIDs; theirs compacted before ours
// retried.
func TestMerger_ThreeWayMerge_compactedTheirs_realOverlap(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		baseR0   = "baser0"
		stagedRS = "stagedrs"
		theirsRC = "theirscompacted"
	)
	baseR0Path := "refs/" + baseR0 + ".ref"
	stagedRSPath := "refs/" + stagedRS + ".ref"
	theirsRCPath := "refs/" + theirsRC + ".ref"

	for _, p := range []string{baseR0Path, stagedRSPath, theirsRCPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	base := newManifest(nil, []string{baseR0Path})
	oursMfst := newManifest(nil, []string{baseR0Path, stagedRSPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	// Compaction: theirs replaced baseR0 with theirsRC.
	theirsMfst := newManifest(nil, []string{theirsRCPath})

	const (
		baseMainOID   = "1111111111111111111111111111111111111111"
		oursMainOID   = "2222222222222222222222222222222222222222"
		theirsMainOID = "3333333333333333333333333333333333333333"
	)
	refsByTable := map[string]map[string]string{
		baseR0Path:   {"refs/heads/main": baseMainOID},
		stagedRSPath: {"refs/heads/main": oursMainOID},
		theirsRCPath: {"refs/heads/main": theirsMainOID},
	}

	readRefs := func(path string) (map[string]string, error) {
		key := filepath.Base(path)
		for p, refs := range refsByTable {
			if filepath.Base(p) == key {
				out := make(map[string]string, len(refs))
				for n, t := range refs {
					out[n] = t
				}
				return out, nil
			}
		}
		t.Fatalf("readRefs invoked on unexpected reftable %q", key)
		return nil, nil
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  func(string) (uint64, uint64, error) { return 1, 1, nil },
		PatchReftableUpdateRange: func(string, uint64, uint64) error { return nil },
	})
	require.ErrorIs(t, err, ErrOverlap, "both sides set refs/heads/main to different OIDs -> overlap")
}

// TestMerger_ThreeWayMerge_sameValueNotOverlap verifies that the
// value-level merge correctly treats "both sides set the same ref to
// the same target" as a non-conflict. Two clients running the same
// `git fetch` would commit identical writes, and comparing values
// rather than membership keeps them from being flagged as overlap.
func TestMerger_ThreeWayMerge_sameValueNotOverlap(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		baseR0   = "baser0"
		stagedRS = "stagedrs"
		theirsRC = "theirsrc"
	)
	baseR0Path := "refs/" + baseR0 + ".ref"
	stagedRSPath := "refs/" + stagedRS + ".ref"
	theirsRCPath := "refs/" + theirsRC + ".ref"

	for _, p := range []string{baseR0Path, stagedRSPath, theirsRCPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	// Theirs compacted base's table.
	base := newManifest(nil, []string{baseR0Path})
	oursMfst := newManifest(nil, []string{baseR0Path, stagedRSPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{theirsRCPath})

	const (
		baseMainOID = "1111111111111111111111111111111111111111"
		newMainOID  = "2222222222222222222222222222222222222222"
	)
	// Both ours and theirs advance refs/heads/main to the SAME OID.
	refsByTable := map[string]map[string]string{
		baseR0Path:   {"refs/heads/main": baseMainOID},
		stagedRSPath: {"refs/heads/main": newMainOID},
		theirsRCPath: {"refs/heads/main": newMainOID},
	}

	readRefs := func(path string) (map[string]string, error) {
		key := filepath.Base(path)
		for p, refs := range refsByTable {
			if filepath.Base(p) == key {
				out := make(map[string]string, len(refs))
				for n, t := range refs {
					out[n] = t
				}
				return out, nil
			}
		}
		t.Fatalf("readRefs invoked on unexpected reftable %q", key)
		return nil, nil
	}

	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case baseR0 + ".ref":
			return 1, 1, nil
		case theirsRC + ".ref":
			return 2, 2, nil
		case stagedRS + ".ref":
			return 2, 2, nil
		}
		return 0, 0, nil
	}
	patchRange := func(path string, min, max uint64) error {
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.NoError(t, err, "identical value-level write on both sides must merge cleanly")
}

// TestMerger_ThreeWayMerge_compactedOurs_noOverlap exercises the
// symmetric case: ours compacted, theirs untouched. The prefix
// invariant breaks because ours is no longer an extension of base.
func TestMerger_ThreeWayMerge_compactedOurs_noOverlap(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		baseR0   = "baser0"
		baseR1   = "baser1"
		oursRC   = "ourscompacted"
		theirsRT = "theirstable"
	)
	baseR0Path := "refs/" + baseR0 + ".ref"
	baseR1Path := "refs/" + baseR1 + ".ref"
	oursRCPath := "refs/" + oursRC + ".ref"
	theirsRTPath := "refs/" + theirsRT + ".ref"

	for _, p := range []string{baseR0Path, baseR1Path, oursRCPath, theirsRTPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	// Base = [R0, R1]; ours compacted to [RC] then appended nothing
	// extra (ours = [RC]); theirs extended cleanly with a new table.
	base := newManifest(nil, []string{baseR0Path, baseR1Path})
	oursMfst := newManifest(nil, []string{oursRCPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{baseR0Path, baseR1Path, theirsRTPath})

	const (
		mainOID    = "1111111111111111111111111111111111111111"
		devOID     = "2222222222222222222222222222222222222222"
		featureOID = "3333333333333333333333333333333333333333"
	)
	refsByTable := map[string]map[string]string{
		baseR0Path:   {"refs/heads/main": mainOID},
		baseR1Path:   {"refs/heads/dev": devOID},
		oursRCPath:   {"refs/heads/main": mainOID, "refs/heads/dev": devOID},
		theirsRTPath: {"refs/heads/feature": featureOID},
	}

	readRefs := func(path string) (map[string]string, error) {
		key := filepath.Base(path)
		for p, refs := range refsByTable {
			if filepath.Base(p) == key {
				out := make(map[string]string, len(refs))
				for n, t := range refs {
					out[n] = t
				}
				return out, nil
			}
		}
		t.Fatalf("readRefs invoked on unexpected reftable %q", key)
		return nil, nil
	}

	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case baseR0 + ".ref":
			return 1, 1, nil
		case baseR1 + ".ref":
			return 2, 2, nil
		case theirsRT + ".ref":
			return 3, 3, nil
		case oursRC + ".ref":
			return 1, 2, nil
		}
		return 0, 0, nil
	}
	patchRange := func(path string, min, max uint64) error {
		// theirsMax = 3, oursMin = 1 -> delta = 3, ours rebases to [4, 5].
		require.Equal(t, uint64(4), min)
		require.Equal(t, uint64(5), max)
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.NoError(t, err, "compacted ours with disjoint theirs edits must merge cleanly")
}

// TestMerger_ThreeWayMerge_packsOnly_acceptsWithoutReadRefs verifies
// that when ReadReftableRefs is not wired (pack-only flows), the merge
// succeeds even with compaction in the picture. An absent callback is
// treated as "no overlap", which is safe because the writers did not
// touch any refs.
func TestMerger_ThreeWayMerge_packsOnly_acceptsWithoutReadRefs(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	// Base has two tables; theirs compacted them into one (prefix
	// invariant broken). Ours adds only a pack.
	const (
		baseR0   = "baser0"
		baseR1   = "baser1"
		theirsRC = "theirsrc"
	)
	baseR0Path := "refs/" + baseR0 + ".ref"
	baseR1Path := "refs/" + baseR1 + ".ref"
	theirsRCPath := "refs/" + theirsRC + ".ref"
	for _, p := range []string{baseR0Path, baseR1Path, theirsRCPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	base := newManifest(expandPacks("p0"), []string{baseR0Path, baseR1Path})
	oursMfst := newManifest(expandPacks("p0", "s1"), []string{baseR0Path, baseR1Path})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(expandPacks("p0"), []string{theirsRCPath})

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		// ReadReftableRefs deliberately nil.
		ReadReftableUpdateRange:  func(string) (uint64, uint64, error) { return 1, 1, nil },
		PatchReftableUpdateRange: func(string, uint64, uint64) error { return nil },
	})
	require.NoError(t, err, "pack-only merge with compaction must succeed when ReadReftableRefs is nil")

	newSha, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	body, err := os.ReadFile(manifestFilePath(t, cacheDir, string(newSha)))
	require.NoError(t, err)
	m, err := ParseManifest(body)
	require.NoError(t, err)
	// Both packs survive (P0 shared, S1 ours-exclusive).
	require.Len(t, m.ObjPaths(), 6, "two packs * three files (idx/pack/rev)")
}

// TestRefDiff_basic exercises refDiff's modification, creation, and
// deletion semantics directly, without going through ThreeWayMerge.
func TestRefDiff_basic(t *testing.T) {
	t.Parallel()

	before := map[string]string{
		"refs/heads/main": "aaaa",
		"refs/heads/dev":  "bbbb",
		"refs/heads/old":  "cccc",
		"refs/heads/keep": "dddd",
	}
	after := map[string]string{
		"refs/heads/main": "aaaa", // unchanged
		"refs/heads/dev":  "BBBB", // modified
		"refs/heads/new":  "eeee", // created
		"refs/heads/keep": "dddd", // unchanged
		// refs/heads/old deleted
	}

	diff := refDiff(before, after)
	require.Equal(t, "BBBB", diff["refs/heads/dev"], "modified ref carries new target")
	require.Equal(t, "eeee", diff["refs/heads/new"], "created ref appears with its target")
	require.Equal(t, "", diff["refs/heads/old"], "deleted ref sentinels with empty target")
	require.NotContains(t, diff, "refs/heads/main", "unchanged ref dropped")
	require.NotContains(t, diff, "refs/heads/keep", "unchanged ref dropped")
}

// TestMerger_ThreeWayMerge_partialCompaction_dedupes verifies the
// hot-path optimization for the common case of partial compaction.
// Two layers of optimisation are at play:
//
//  1. Tables present in *more than one* stack but not in all three are
//     read exactly once (e.g. B2 and B3 are in base and theirs but not
//     ours — one read each, not two).
//
//  2. Tables present in *all three* stacks are skipped entirely; their
//     contribution to every side's folded ref map is identical, so
//     skipping them changes no overlap answer and reading them
//     would be wasted work (e.g. B0 and B1 are in base, ours, AND
//     theirs — zero reads).
//
// The scenario: base has a 4-table stack [B0, B1, B2, B3]. Theirs
// extended cleanly with one new table T_top. Ours compacted the top
// two base tables into a single new exclusive table OC and appended
// nothing else; ours' stack is [B0, B1, OC].
func TestMerger_ThreeWayMerge_partialCompaction_dedupes(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		b0     = "stackb0"
		b1     = "stackb1"
		b2     = "stackb2"
		b3     = "stackb3"
		oursOC = "ourscompacted"
		theirT = "theirstop"
	)
	pathOf := func(slug string) string { return "refs/" + slug + ".ref" }
	for _, slug := range []string{b0, b1, b2, b3, oursOC, theirT} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, pathOf(slug)),
			[]byte("bytes-of-"+slug),
			0o644,
		))
	}

	// Base = [B0, B1, B2, B3]; ours = [B0, B1, OC]; theirs = [B0, B1, B2, B3, T_top].
	// B0 and B1 are shared across all three. B2 and B3 are shared between
	// base and theirs. OC is ours-exclusive. T_top is theirs-exclusive.
	base := newManifest(nil, []string{pathOf(b0), pathOf(b1), pathOf(b2), pathOf(b3)})
	oursMfst := newManifest(nil, []string{pathOf(b0), pathOf(b1), pathOf(oursOC)})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{pathOf(b0), pathOf(b1), pathOf(b2), pathOf(b3), pathOf(theirT)})

	const (
		mainOID    = "1111111111111111111111111111111111111111"
		devOID     = "2222222222222222222222222222222222222222"
		stableOID  = "3333333333333333333333333333333333333333"
		releaseOID = "4444444444444444444444444444444444444444"
		newOID     = "5555555555555555555555555555555555555555"
	)
	refsByTable := map[string]map[string]string{
		pathOf(b0): {"refs/heads/main": mainOID},
		pathOf(b1): {"refs/heads/dev": devOID},
		pathOf(b2): {"refs/heads/stable": stableOID},
		pathOf(b3): {"refs/heads/release": releaseOID},
		// Ours' compacted table carries main/dev/stable/release forward
		// AND nothing new; the disjoint edit is implicit in the fact that
		// theirs introduced a new ref (refs/heads/feature) that ours
		// doesn't have. Conflict-free.
		pathOf(oursOC): {"refs/heads/main": mainOID, "refs/heads/dev": devOID, "refs/heads/stable": stableOID, "refs/heads/release": releaseOID},
		pathOf(theirT): {"refs/heads/feature": newOID},
	}

	var (
		mu        sync.Mutex
		readCalls = map[string]int{}
	)
	readRefs := func(path string) (map[string]string, error) {
		key := filepath.Base(path)
		mu.Lock()
		readCalls[key]++
		mu.Unlock()
		for p, refs := range refsByTable {
			if filepath.Base(p) == key {
				out := make(map[string]string, len(refs))
				for n, t := range refs {
					out[n] = t
				}
				return out, nil
			}
		}
		t.Fatalf("readRefs invoked on unexpected reftable %q", key)
		return nil, nil
	}

	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case b0 + ".ref":
			return 1, 1, nil
		case b1 + ".ref":
			return 2, 2, nil
		case b2 + ".ref":
			return 3, 3, nil
		case b3 + ".ref":
			return 4, 4, nil
		case oursOC + ".ref":
			return 3, 4, nil // ours' compacted table spans 3..4
		case theirT + ".ref":
			return 5, 5, nil
		}
		return 0, 0, nil
	}
	patchRange := func(path string, min, max uint64) error {
		// theirsMax=5, oursMin=3 -> delta=3, ours rebases to [6, 7].
		require.Equal(t, uint64(6), min)
		require.Equal(t, uint64(7), max)
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.NoError(t, err, "partial compaction with disjoint edits must merge")

	// Triply-shared tables are skipped entirely (zero reads);
	// doubly-shared and exclusive tables are read exactly once.
	require.Equal(t, 0, readCalls[b0+".ref"], "B0 shared across all 3 stacks -> skipped")
	require.Equal(t, 0, readCalls[b1+".ref"], "B1 shared across all 3 stacks -> skipped")
	require.Equal(t, 1, readCalls[b2+".ref"], "B2 in base+theirs (doubly shared) -> one read")
	require.Equal(t, 1, readCalls[b3+".ref"], "B3 in base+theirs (doubly shared) -> one read")
	require.Equal(t, 1, readCalls[oursOC+".ref"], "ours-exclusive compacted table -> one read")
	require.Equal(t, 1, readCalls[theirT+".ref"], "theirs-exclusive top table -> one read")
}

// TestFoldStack verifies the stack-folding semantics directly: later
// entries shadow earlier ones, and tables absent from the cache (an
// invalid call but defensible behaviour) contribute nothing.
func TestFoldStack(t *testing.T) {
	t.Parallel()

	tableRefs := map[string]map[string]string{
		"refs/a.ref": {"refs/heads/main": "AAAA", "refs/heads/dev": "BBBB"},
		"refs/b.ref": {"refs/heads/main": "CCCC", "refs/heads/feat": "DDDD"},
		"refs/c.ref": {"refs/heads/dev": "EEEE"},
	}

	for _, tc := range []struct {
		name  string
		stack []string
		want  map[string]string
	}{
		{
			name:  "empty stack",
			stack: nil,
			want:  map[string]string{},
		},
		{
			name:  "single table",
			stack: []string{"refs/a.ref"},
			want:  map[string]string{"refs/heads/main": "AAAA", "refs/heads/dev": "BBBB"},
		},
		{
			name:  "later shadows earlier",
			stack: []string{"refs/a.ref", "refs/b.ref"},
			want: map[string]string{
				"refs/heads/main": "CCCC", // b shadows a
				"refs/heads/dev":  "BBBB",
				"refs/heads/feat": "DDDD",
			},
		},
		{
			name:  "three-layer shadow",
			stack: []string{"refs/a.ref", "refs/b.ref", "refs/c.ref"},
			want: map[string]string{
				"refs/heads/main": "CCCC", // b shadows a
				"refs/heads/dev":  "EEEE", // c shadows a
				"refs/heads/feat": "DDDD",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, foldStack(tc.stack, tableRefs))
		})
	}
}

// TestFoldStack_tombstones pins down the foldStack contract for
// deletion records. reftable.ReadRefsAt emits a deletion as a
// {refname: zeroOID} entry (rather than omitting the refname),
// because foldStack relies on the entry being present to shadow any
// earlier table's real target for the same refname. If the entry were
// omitted, the earlier value would leak through.
//
// The zero-OID string is unrepresentable in a real ref, so it serves
// as an in-band tombstone marker that refDiff treats as differing
// from any real target.
func TestFoldStack_tombstones(t *testing.T) {
	t.Parallel()

	const (
		realOID    = "1111111111111111111111111111111111111111"
		updatedOID = "2222222222222222222222222222222222222222"
		zeroOID    = "0000000000000000000000000000000000000000"
	)

	tableRefs := map[string]map[string]string{
		// bottomCreate creates refs/heads/main and refs/heads/dev.
		"refs/bottom-create.ref": {
			"refs/heads/main": realOID,
			"refs/heads/dev":  realOID,
		},
		// midDelete tombstones refs/heads/main.
		"refs/mid-delete.ref": {
			"refs/heads/main": zeroOID,
		},
		// topRecreate brings refs/heads/main back at a different OID
		// (a delete-then-recreate sequence).
		"refs/top-recreate.ref": {
			"refs/heads/main": updatedOID,
		},
		// topDeleteDev tombstones refs/heads/dev at the top of the stack.
		"refs/top-delete-dev.ref": {
			"refs/heads/dev": zeroOID,
		},
	}

	for _, tc := range []struct {
		name  string
		stack []string
		want  map[string]string
	}{
		{
			name:  "top-level tombstone is preserved",
			stack: []string{"refs/bottom-create.ref", "refs/top-delete-dev.ref"},
			want: map[string]string{
				"refs/heads/main": realOID,
				"refs/heads/dev":  zeroOID, // tombstone retained, not erased
			},
		},
		{
			name:  "mid-stack tombstone shadows earlier real value",
			stack: []string{"refs/bottom-create.ref", "refs/mid-delete.ref"},
			want: map[string]string{
				"refs/heads/main": zeroOID, // tombstone shadows the bottom's realOID
				"refs/heads/dev":  realOID,
			},
		},
		{
			name:  "top recreate overwrites mid-stack tombstone",
			stack: []string{"refs/bottom-create.ref", "refs/mid-delete.ref", "refs/top-recreate.ref"},
			want: map[string]string{
				"refs/heads/main": updatedOID, // tombstone in turn shadowed by re-creation
				"refs/heads/dev":  realOID,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, foldStack(tc.stack, tableRefs))
		})
	}
}

// TestMerger_ThreeWayMerge_triplySharedShadowedByOurs is the
// regression test for the triply-shared skip optimisation in the
// presence of shadowing. It pins down the cancellation argument: a
// table in all three of {base, ours, theirs} that defines a ref later
// shadowed by an ours-exclusive table must NOT generate a false
// positive overlap when skipped. Theirs sees the original value via
// the triply-shared table (which we skip), ours sees the shadowed
// value via the exclusive table -- symmetric missing-on-{base,theirs}
// means the overlap checks still produce the right answer.
func TestMerger_ThreeWayMerge_triplySharedShadowedByOurs(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		// r1 defines refs/heads/main = A. Triply shared.
		r1Slug = "tripleshared"
		// r4 is theirs-exclusive but does NOT touch main. It adds
		// refs/heads/feature, so theirs.RefPaths() != base.RefPaths().
		r4Slug = "theirsexclusive"
		// r5 is ours-exclusive and shadows r1's main = A with main = B.
		r5Slug = "oursexclusive"
	)
	pathOf := func(slug string) string { return "refs/" + slug + ".ref" }
	for _, slug := range []string{r1Slug, r4Slug, r5Slug} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, pathOf(slug)),
			[]byte("bytes-of-"+slug),
			0o644,
		))
	}

	base := newManifest(nil, []string{pathOf(r1Slug)})
	oursMfst := newManifest(nil, []string{pathOf(r1Slug), pathOf(r5Slug)})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{pathOf(r1Slug), pathOf(r4Slug)})

	const (
		mainA      = "1111111111111111111111111111111111111111"
		mainB      = "2222222222222222222222222222222222222222"
		featureOID = "3333333333333333333333333333333333333333"
	)
	refsByTable := map[string]map[string]string{
		pathOf(r1Slug): {"refs/heads/main": mainA},
		pathOf(r4Slug): {"refs/heads/feature": featureOID},
		pathOf(r5Slug): {"refs/heads/main": mainB}, // shadows r1 in ours only
	}

	var mu sync.Mutex
	readCalls := map[string]int{}
	readRefs := func(path string) (map[string]string, error) {
		key := filepath.Base(path)
		mu.Lock()
		readCalls[key]++
		mu.Unlock()
		for p, refs := range refsByTable {
			if filepath.Base(p) == key {
				out := make(map[string]string, len(refs))
				for n, t := range refs {
					out[n] = t
				}
				return out, nil
			}
		}
		t.Fatalf("readRefs invoked on unexpected reftable %q", key)
		return nil, nil
	}
	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case r1Slug + ".ref":
			return 1, 1, nil
		case r4Slug + ".ref":
			return 2, 2, nil
		case r5Slug + ".ref":
			return 2, 2, nil
		}
		return 0, 0, nil
	}
	patchRange := func(path string, min, max uint64) error {
		// theirsMax=2, oursMin=2 -> delta=1, ours rebases to [3,3].
		require.Equal(t, uint64(3), min)
		require.Equal(t, uint64(3), max)
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	// Ours moved main A->B; theirs didn't touch main; no conflict.
	require.NoError(t, err, "triply-shared shadowing on ours only must NOT flag overlap")

	// The triply-shared table is skipped entirely; both exclusive
	// tables are read once each.
	require.Equal(t, 0, readCalls[r1Slug+".ref"], "triply-shared r1 -> skipped")
	require.Equal(t, 1, readCalls[r4Slug+".ref"], "theirs-exclusive r4 -> one read")
	require.Equal(t, 1, readCalls[r5Slug+".ref"], "ours-exclusive r5 -> one read")
}

// TestMerger_ThreeWayMerge_triplySharedShadowedByBoth is the second
// regression test for the triply-shared skip in the presence of
// shadowing: when both ours AND theirs shadow a triply-shared ref to
// different values, we MUST detect the conflict. This is the case
// where missing-on-base is asymmetric: ours has B, theirs has C, base
// is missing (skipped), so both ends up in their respective diffs and
// the value-mismatch predicate fires.
func TestMerger_ThreeWayMerge_triplySharedShadowedByBoth(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		r1Slug = "tripleshared"
		r4Slug = "theirsexclusive"
		r5Slug = "oursexclusive"
	)
	pathOf := func(slug string) string { return "refs/" + slug + ".ref" }
	for _, slug := range []string{r1Slug, r4Slug, r5Slug} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, pathOf(slug)),
			[]byte("bytes-of-"+slug),
			0o644,
		))
	}

	base := newManifest(nil, []string{pathOf(r1Slug)})
	oursMfst := newManifest(nil, []string{pathOf(r1Slug), pathOf(r5Slug)})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{pathOf(r1Slug), pathOf(r4Slug)})

	const (
		mainA = "1111111111111111111111111111111111111111"
		mainB = "2222222222222222222222222222222222222222"
		mainC = "3333333333333333333333333333333333333333"
	)
	refsByTable := map[string]map[string]string{
		pathOf(r1Slug): {"refs/heads/main": mainA},
		pathOf(r4Slug): {"refs/heads/main": mainC}, // theirs shadows main to C
		pathOf(r5Slug): {"refs/heads/main": mainB}, // ours shadows main to B
	}

	readRefs := func(path string) (map[string]string, error) {
		key := filepath.Base(path)
		for p, refs := range refsByTable {
			if filepath.Base(p) == key {
				out := make(map[string]string, len(refs))
				for n, t := range refs {
					out[n] = t
				}
				return out, nil
			}
		}
		t.Fatalf("readRefs invoked on unexpected reftable %q", key)
		return nil, nil
	}
	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case r1Slug + ".ref":
			return 1, 1, nil
		case r4Slug + ".ref":
			return 2, 2, nil
		case r5Slug + ".ref":
			return 2, 2, nil
		}
		return 0, 0, nil
	}
	patchRange := func(string, uint64, uint64) error {
		t.Fatal("patchRange should not be invoked when overlap is detected")
		return nil
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.ErrorIs(t, err, ErrOverlap,
		"both sides shadow main to different values -> real conflict")
}

// TestMerger_ThreeWayMerge_divergenceStopsTriplyShared locks in the
// positional-prefix contract for the triply-shared skip: once any of
// the three stacks diverges at some position k, no later position is
// treated as triply shared, even if a later path happens to appear in
// all three stacks by name.
//
// In valid reftable stacks this configuration is unreachable (a
// content-addressed reftable's hash depends on its predecessor's max
// update-index via the header, so the same file cannot appear at the
// same position in stacks with divergent prefixes), but the test pins
// down the algorithm's behaviour and acts as a regression guard
// against accidentally reverting to a set-based check.
//
// Stacks (X appears positionally at index 2 in all three, but ours
// diverges at index 1):
//
//	base   = [B0, B1, X]
//	ours   = [B0, OC, X]
//	theirs = [B0, B1, X]
//
// Expected reads: B0 (triply-shared at position 0) is skipped; B1, OC,
// and X are all read (because divergence at position 1 ends the
// prefix).
func TestMerger_ThreeWayMerge_divergenceStopsTriplyShared(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		b0Slug = "b0prefix"
		b1Slug = "b1base"
		ocSlug = "ourscompacted"
		xSlug  = "tailx"
	)
	pathOf := func(slug string) string { return "refs/" + slug + ".ref" }
	for _, slug := range []string{b0Slug, b1Slug, ocSlug, xSlug} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, pathOf(slug)),
			[]byte("bytes-of-"+slug),
			0o644,
		))
	}

	base := newManifest(nil, []string{pathOf(b0Slug), pathOf(b1Slug), pathOf(xSlug)})
	oursMfst := newManifest(nil, []string{pathOf(b0Slug), pathOf(ocSlug), pathOf(xSlug)})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{pathOf(b0Slug), pathOf(b1Slug), pathOf(xSlug)})

	const (
		mainOID    = "1111111111111111111111111111111111111111"
		devOID     = "2222222222222222222222222222222222222222"
		featureOID = "3333333333333333333333333333333333333333"
	)
	refsByTable := map[string]map[string]string{
		pathOf(b0Slug): {"refs/heads/main": mainOID},
		pathOf(b1Slug): {"refs/heads/dev": devOID},
		pathOf(ocSlug): {"refs/heads/main": mainOID, "refs/heads/dev": devOID},
		pathOf(xSlug):  {"refs/heads/feature": featureOID},
	}
	var mu sync.Mutex
	readCalls := map[string]int{}
	readRefs := func(path string) (map[string]string, error) {
		key := filepath.Base(path)
		mu.Lock()
		readCalls[key]++
		mu.Unlock()
		for p, refs := range refsByTable {
			if filepath.Base(p) == key {
				out := make(map[string]string, len(refs))
				for n, t := range refs {
					out[n] = t
				}
				return out, nil
			}
		}
		t.Fatalf("readRefs invoked on unexpected reftable %q", key)
		return nil, nil
	}
	// theirs.RefPaths() == base.RefPaths(), so only ours diverges.
	readRange := func(path string) (uint64, uint64, error) { return 1, 1, nil }
	patchRange := func(path string, min, max uint64) error {
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.NoError(t, err, "no value-level conflict; merge must succeed")

	// B0 sits at position 0 of every stack -> triply shared -> skipped.
	require.Equal(t, 0, readCalls[b0Slug+".ref"],
		"B0 at the triply-shared prefix position -> skipped")
	// B1 appears in base and theirs only; not at a triply-shared
	// position -> read once.
	require.Equal(t, 1, readCalls[b1Slug+".ref"],
		"B1 not at a triply-shared position -> one read")
	// OC is ours-exclusive -> read once.
	require.Equal(t, 1, readCalls[ocSlug+".ref"],
		"OC ours-exclusive -> one read")
	// X appears in all three stacks by name and at the same numeric
	// index, but the prefix already diverged at position 1 (B1 vs OC),
	// so X must be read on every side. This is the regression: a
	// set-based check would have skipped X.
	require.Equal(t, 1, readCalls[xSlug+".ref"],
		"X past the prefix divergence -> one read, not skipped")
}

// TestMerger_ThreeWayMerge_crossSuffixDedupe pins down that the
// `seen` map in readUniqueReftables dedupes paths that appear in two
// of the three suffixes (past prefixLen) but not in base.
//
// In valid reftable stacks this can happen because a content-addressed
// reftable's hash depends only on its predecessor's max_update_index.
// If ours compacts B1 into OC and theirs leaves it untouched, both
// stacks can legitimately append the same T on top (T's predecessor
// max-index matches in both stacks because OC.max == B1.max).
//
// Stacks:
//
//	base   = [B0, B1]
//	ours   = [B0, OC, T]
//	theirs = [B0, B1, T]
//
// prefixLen=1 (B0 matches, position 1 diverges). ours[1:]=[OC,T] and
// theirs[1:]=[B1,T] both contain T. Without dedupe, T would be read
// twice. With the seen map, T is read exactly once.
func TestMerger_ThreeWayMerge_crossSuffixDedupe(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		b0Slug = "b0prefix"
		b1Slug = "b1base"
		ocSlug = "ourscompacted"
		tSlug  = "shared"
	)
	pathOf := func(slug string) string { return "refs/" + slug + ".ref" }
	for _, slug := range []string{b0Slug, b1Slug, ocSlug, tSlug} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, pathOf(slug)),
			[]byte("bytes-of-"+slug),
			0o644,
		))
	}

	base := newManifest(nil, []string{pathOf(b0Slug), pathOf(b1Slug)})
	oursMfst := newManifest(nil, []string{pathOf(b0Slug), pathOf(ocSlug), pathOf(tSlug)})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{pathOf(b0Slug), pathOf(b1Slug), pathOf(tSlug)})

	const (
		mainOID    = "1111111111111111111111111111111111111111"
		devOID     = "2222222222222222222222222222222222222222"
		featureOID = "3333333333333333333333333333333333333333"
	)
	refsByTable := map[string]map[string]string{
		pathOf(b0Slug): {"refs/heads/main": mainOID},
		pathOf(b1Slug): {"refs/heads/dev": devOID},
		pathOf(ocSlug): {"refs/heads/main": mainOID, "refs/heads/dev": devOID},
		pathOf(tSlug):  {"refs/heads/feature": featureOID},
	}
	var mu sync.Mutex
	readCalls := map[string]int{}
	readRefs := func(path string) (map[string]string, error) {
		key := filepath.Base(path)
		mu.Lock()
		readCalls[key]++
		mu.Unlock()
		for p, refs := range refsByTable {
			if filepath.Base(p) == key {
				out := make(map[string]string, len(refs))
				for n, t := range refs {
					out[n] = t
				}
				return out, nil
			}
		}
		t.Fatalf("readRefs invoked on unexpected reftable %q", key)
		return nil, nil
	}
	readRange := func(path string) (uint64, uint64, error) { return 1, 1, nil }
	patchRange := func(path string, min, max uint64) error {
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.NoError(t, err, "no value-level conflict; merge must succeed")

	// B0 triply shared at position 0 -> skipped.
	require.Equal(t, 0, readCalls[b0Slug+".ref"],
		"B0 in the triply-shared prefix -> skipped")
	// B1 appears in base and theirs only -> read once.
	require.Equal(t, 1, readCalls[b1Slug+".ref"],
		"B1 only in base+theirs suffixes -> one read")
	// OC ours-exclusive -> read once.
	require.Equal(t, 1, readCalls[ocSlug+".ref"],
		"OC ours-exclusive -> one read")
	// T appears in BOTH ours[1:] and theirs[1:] (not in base[1:]).
	// The seen map must dedupe across suffixes so T is read exactly
	// once, not twice.
	require.Equal(t, 1, readCalls[tSlug+".ref"],
		"T shared across ours and theirs suffixes -> deduped to one read")
}

// TestMerger_ThreeWayMerge_directoryFileOverlap verifies that a
// directory/file conflict between ours and theirs surfaces as
// ErrOverlap. Ours creates refs/heads/nested, theirs creates
// refs/heads/nested/a: the merged manifest cannot exist in the loose
// ref layout (nested would be both a file and a directory) so the
// merge must be rejected even though the refnames are not equal.
func TestMerger_ThreeWayMerge_directoryFileOverlap(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	const (
		oursTbl   = "oursdftbl"
		theirsTbl = "theirsdftbl"
	)
	oursTblPath := "refs/" + oursTbl + ".ref"
	theirsTblPath := "refs/" + theirsTbl + ".ref"

	for _, p := range []string{oursTblPath, theirsTblPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	// base empty so each side's exclusive table is the full diff
	// relative to base.
	base := newManifest(nil, nil)
	oursMfst := newManifest(nil, []string{oursTblPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{theirsTblPath})

	const (
		nestedOID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		childOID  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	refsByTable := map[string]map[string]string{
		oursTblPath:   {"refs/heads/nested": nestedOID},
		theirsTblPath: {"refs/heads/nested/a": childOID},
	}

	readRefs := func(path string) (map[string]string, error) {
		key := filepath.Base(path)
		for p, refs := range refsByTable {
			if filepath.Base(p) == key {
				out := make(map[string]string, len(refs))
				for n, t := range refs {
					out[n] = t
				}
				return out, nil
			}
		}
		t.Fatalf("readRefs invoked on unexpected reftable %q", key)
		return nil, nil
	}

	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case oursTbl + ".ref":
			return 1, 1, nil
		case theirsTbl + ".ref":
			return 1, 1, nil
		}
		return 0, 0, nil
	}
	patchRange := func(path string, min, max uint64) error {
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.ErrorIs(t, err, ErrOverlap,
		"ours creates refs/heads/nested, theirs creates refs/heads/nested/a -> D/F overlap")
}

// TestIsLiveTarget pins the predicate distinguishing live targets
// (creations/updates) from deletion sentinels (empty string, all-zero
// hash).
func TestIsLiveTarget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		target string
		live   bool
	}{
		{"empty string", "", false},
		{"sha1 zero oid", "0000000000000000000000000000000000000000", false},
		{"sha256 zero oid", "0000000000000000000000000000000000000000000000000000000000000000", false},
		{"sha1 real oid", "1234567890abcdef1234567890abcdef12345678", true},
		{"sha1 single non-zero", "0000000000000000000000000000000000000001", true},
		{"symref target", "ref: refs/heads/main", true},
		{"short non-zero", "1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.live, isLiveTarget(tc.target))
		})
	}
}

// TestPathTrie_conflicts exercises the trie's D/F conflict predicate
// directly. The merge-time end-to-end tests above cover the full
// pipeline; this one isolates the trie semantics so collision shapes
// are easy to read off.
func TestPathTrie_conflicts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		inserts []string
		probe   string
		want    bool
	}{
		{
			name:    "ancestor in trie, probe is descendant",
			inserts: []string{"refs/heads/nested"},
			probe:   "refs/heads/nested/a",
			want:    true,
		},
		{
			name:    "descendant in trie, probe is ancestor",
			inserts: []string{"refs/heads/nested/a"},
			probe:   "refs/heads/nested",
			want:    true,
		},
		{
			name:    "exact match -- not a D/F conflict",
			inserts: []string{"refs/heads/main"},
			probe:   "refs/heads/main",
			want:    false,
		},
		{
			name:    "sibling names with shared prefix -- not a D/F conflict",
			inserts: []string{"refs/heads/nest"},
			probe:   "refs/heads/nested",
			want:    false,
		},
		{
			name:    "disjoint refs",
			inserts: []string{"refs/heads/main"},
			probe:   "refs/tags/v1",
			want:    false,
		},
		{
			name:    "empty trie",
			inserts: nil,
			probe:   "refs/heads/main",
			want:    false,
		},
		{
			name:    "deep descendant via multi-step probe",
			inserts: []string{"refs/heads/a"},
			probe:   "refs/heads/a/b/c/d",
			want:    true,
		},
		{
			name:    "multiple inserts, one is an ancestor",
			inserts: []string{"refs/tags/v1", "refs/heads/main", "refs/heads/main/child"},
			probe:   "refs/heads/main", // exact match on a live ref BUT has children
			want:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trie := newPathTrie()
			for _, ins := range tc.inserts {
				trie.insert(ins)
			}
			require.Equal(t, tc.want, trie.conflicts(tc.probe))
		})
	}
}

// TestMerger_ThreeWayMerge_emptyTheirsRefStack covers a pack-only
// write racing against a base that carries no reftables at all:
// ParseManifest leaves refStackOrder nil when the REFS chunk is
// absent, so theirs' stack is empty while ours has a staged table.
// reftableMaxIndex must report 0 for the empty stack rather than
// indexing past the end of the slice.
func TestMerger_ThreeWayMerge_emptyTheirsRefStack(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	stagedPath := "refs/stagedtable.ref"
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, stagedPath), []byte("staged bytes"), 0o644))

	// Neither base nor theirs has a REFS chunk; ours stages one table.
	base := newManifest(expandPacks("p0"), nil)
	oursMfst := newManifest(expandPacks("p0"), []string{stagedPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(expandPacks("p0", "d1"), nil)

	require.Empty(t, theirsMfst.RefPaths(), "theirs must have an empty ref stack")

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableUpdateRange:  func(string) (uint64, uint64, error) { return 1, 1, nil },
		PatchReftableUpdateRange: panickyPatchRange,
	})
	require.NoError(t, err)

	newSha, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	body, err := os.ReadFile(manifestFilePath(t, cacheDir, string(newSha)))
	require.NoError(t, err)
	merged, err := ParseManifest(body)
	require.NoError(t, err)

	// Ours' table sits on top of an empty theirs stack, unrebased.
	require.Equal(t, []string{stagedPath}, merged.RefPaths())
	require.Len(t, merged.ObjPaths(), 6, "two packs * three files (idx/pack/rev)")
}

// TestMerger_ThreeWayMerge_fetchesUnparsedLastReftable covers the
// branch of reftableMaxIndex that reads theirs' last reftable
// directly: theirs appended nothing, so the table is not in the
// exclusive set that step 2 fetched. The merge must pull it down
// itself rather than relying on some other component having warmed
// the cache first.
func TestMerger_ThreeWayMerge_fetchesUnparsedLastReftable(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir, durableDir := newDetachedTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(durableDir, "refs"), 0o755))

	baseR0Path := "refs/baser0.ref"
	stagedPath := "refs/stagedtable.ref"
	// Seed both tables into durable storage only. Nothing pre-warms
	// the local cache, so every read has to be preceded by a fetch.
	require.NoError(t, os.WriteFile(filepath.Join(durableDir, baseR0Path), []byte("7"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(durableDir, stagedPath), []byte("1"), 0o644))

	// Theirs matches base: it added a pack but no reftable, so its
	// last reftable is never part of the exclusive set.
	base := newManifest(expandPacks("p0"), []string{baseR0Path})
	oursMfst := newManifest(expandPacks("p0"), []string{baseR0Path, stagedPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(expandPacks("p0", "d1"), []string{baseR0Path})

	// readRange reads the file, so it fails if the merge did not
	// fetch the reftable into the local cache first.
	readRange := func(path string) (uint64, uint64, error) {
		raw, err := os.ReadFile(path)
		if err != nil {
			return 0, 0, err
		}
		idx, err := strconv.ParseUint(string(raw), 10, 64)
		if err != nil {
			return 0, 0, err
		}
		return idx, idx, nil
	}
	patchRange := func(path string, min, max uint64) error {
		require.Equal(t, uint64(8), min, "staged min 1 rebased onto theirsMax 7 -> 8")
		require.Equal(t, uint64(8), max)
		return os.WriteFile(path, []byte("8"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         disjointTableRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.NoError(t, err)

	exists, err := fileExist(filepath.Join(cacheDir, baseR0Path))
	require.NoError(t, err)
	require.True(t, exists, "theirs' last reftable must have been fetched into the local cache")
}

// TestMerger_ThreeWayMerge_theirsMaxIndexNotLast pins the rebase to
// the highest update index anywhere in theirs' stack, not just the
// one carried by the last table. Reading only the last entry assumes
// stack order implies monotonically increasing update indices; when
// that does not hold, ours is rebased too low and its indices
// interleave with theirs'.
func TestMerger_ThreeWayMerge_theirsMaxIndexNotLast(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	baseR0Path := "refs/baser0.ref"
	theirsT1Path := "refs/theirst1.ref"
	theirsT2Path := "refs/theirst2.ref"
	stagedPath := "refs/stagedtable.ref"
	for _, p := range []string{baseR0Path, theirsT1Path, theirsT2Path, stagedPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	base := newManifest(nil, []string{baseR0Path})
	oursMfst := newManifest(nil, []string{baseR0Path, stagedPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{baseR0Path, theirsT1Path, theirsT2Path})

	// T1 carries a higher max update index than the last table, T2.
	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case filepath.Base(baseR0Path):
			return 1, 1, nil
		case filepath.Base(theirsT1Path):
			return 2, 9, nil
		case filepath.Base(theirsT2Path):
			return 2, 3, nil
		case filepath.Base(stagedPath):
			return 2, 2, nil
		}
		t.Fatalf("readRange invoked on unexpected reftable %q", path)
		return 0, 0, nil
	}
	patchRange := func(path string, min, max uint64) error {
		require.Equal(t, uint64(10), min, "staged min 2 rebased past theirs' highest max index 9")
		require.Equal(t, uint64(10), max)
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	require.NoError(t, s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         disjointTableRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	}))
}

// TestMerger_ThreeWayMerge_deleteVsDeleteNotOverlap covers two
// clients deleting the same ref concurrently. "Deleted" reaches the
// overlap check in two encodings — ours kept a reftable tombstone
// (the zero OID), theirs compacted the tombstone away so refDiff
// records the empty-string sentinel — and comparing the raw strings
// reports them as a conflict. They describe the same post-merge
// state, so the merge must absorb them.
func TestMerger_ThreeWayMerge_deleteVsDeleteNotOverlap(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	baseR0Path := "refs/baser0.ref"
	oursTombstonePath := "refs/ourstombstone.ref"
	theirsCompactedPath := "refs/theirscompacted.ref"
	for _, p := range []string{baseR0Path, oursTombstonePath, theirsCompactedPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	const (
		mainOID  = "1111111111111111111111111111111111111111"
		staleOID = "2222222222222222222222222222222222222222"
		zeroOID  = "0000000000000000000000000000000000000000"
	)

	base := newManifest(nil, []string{baseR0Path})
	// Ours appends a tombstone for refs/heads/stale.
	oursMfst := newManifest(nil, []string{baseR0Path, oursTombstonePath})
	oursSha := writeManifestBody(t, s, oursMfst)
	// Theirs deleted the same ref but compacted the tombstone away,
	// so the ref is simply absent from its stack.
	theirsMfst := newManifest(nil, []string{theirsCompactedPath})

	refsByTable := map[string]map[string]string{
		baseR0Path:          {"refs/heads/main": mainOID, "refs/heads/stale": staleOID},
		oursTombstonePath:   {"refs/heads/stale": zeroOID},
		theirsCompactedPath: {"refs/heads/main": mainOID},
	}
	readRefs := func(path string) (map[string]string, error) {
		refs, ok := refsByTable[filepath.Join("refs", filepath.Base(path))]
		if !ok {
			t.Fatalf("readRefs invoked on unexpected reftable %q", path)
		}
		return maps.Clone(refs), nil
	}
	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case filepath.Base(baseR0Path):
			return 1, 1, nil
		case filepath.Base(oursTombstonePath):
			return 2, 2, nil
		case filepath.Base(theirsCompactedPath):
			return 1, 2, nil
		}
		t.Fatalf("readRange invoked on unexpected reftable %q", path)
		return 0, 0, nil
	}
	patchRange := func(path string, min, max uint64) error {
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.NoError(t, err, "both sides deleting the same ref must merge cleanly")
}

// TestMerger_ThreeWayMerge_compactedOurs_shadowsTheirs is
// TestMerger_ThreeWayMerge_compactedOurs_noOverlap with theirs
// fast-forwarding a ref that ours' compacted table carries forward
// unchanged. Ours changed nothing, so a diff-based overlap check sees
// no conflict — but the merged stack appends ours' table on top of
// theirs, where it reasserts the old value and loses theirs' push.
// Reporting success to both clients while dropping one of their
// writes is the worst outcome available, so this has to be an
// overlap.
func TestMerger_ThreeWayMerge_compactedOurs_shadowsTheirs(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	baseR0Path := "refs/baser0.ref"
	baseR1Path := "refs/baser1.ref"
	oursRCPath := "refs/ourscompacted.ref"
	theirsRTPath := "refs/theirstable.ref"
	for _, p := range []string{baseR0Path, baseR1Path, oursRCPath, theirsRTPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	const (
		mainOID    = "1111111111111111111111111111111111111111"
		devOID     = "2222222222222222222222222222222222222222"
		newMainOID = "4444444444444444444444444444444444444444"
	)

	// Ours compacted base's two tables into one, changing nothing.
	base := newManifest(nil, []string{baseR0Path, baseR1Path})
	oursMfst := newManifest(nil, []string{oursRCPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	// Theirs appended a table that fast-forwards refs/heads/main.
	theirsMfst := newManifest(nil, []string{baseR0Path, baseR1Path, theirsRTPath})

	refsByTable := map[string]map[string]string{
		baseR0Path:   {"refs/heads/main": mainOID},
		baseR1Path:   {"refs/heads/dev": devOID},
		oursRCPath:   {"refs/heads/main": mainOID, "refs/heads/dev": devOID},
		theirsRTPath: {"refs/heads/main": newMainOID},
	}
	readRefs := func(path string) (map[string]string, error) {
		refs, ok := refsByTable[filepath.Join("refs", filepath.Base(path))]
		if !ok {
			t.Fatalf("readRefs invoked on unexpected reftable %q", path)
		}
		return maps.Clone(refs), nil
	}
	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case filepath.Base(baseR0Path):
			return 1, 1, nil
		case filepath.Base(baseR1Path):
			return 2, 2, nil
		case filepath.Base(theirsRTPath):
			return 3, 3, nil
		case filepath.Base(oursRCPath):
			return 1, 2, nil
		}
		t.Fatalf("readRange invoked on unexpected reftable %q", path)
		return 0, 0, nil
	}

	// A working patch callback, so that a merge which wrongly accepts
	// this conflict fails on the assertion below rather than on a
	// panicking placeholder.
	patchRange := func(path string, min, max uint64) error {
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.ErrorIs(t, err, ErrOverlap, "ours shadowing theirs' write must be reported as overlap")

	// The pointer must still name ours: a rejected merge publishes nothing.
	pointer, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	require.Equal(t, oursSha, string(pointer))
}

// TestMerger_ThreeWayMerge_compactedOurs_sameValueAsTheirs is the
// escape hatch that keeps the membership check from rejecting
// agreement: ours' compacted table mentions refs/heads/main, so it
// shadows theirs in the merged stack, but it names the same target
// theirs does. Two clients ran the same fetch and then one compacted;
// there is nothing to lose, so the merge must go through.
func TestMerger_ThreeWayMerge_compactedOurs_sameValueAsTheirs(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	baseR0Path := "refs/baser0.ref"
	oursRCPath := "refs/ourscompacted.ref"
	theirsRTPath := "refs/theirstable.ref"
	for _, p := range []string{baseR0Path, oursRCPath, theirsRTPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	const (
		mainOID    = "1111111111111111111111111111111111111111"
		newMainOID = "4444444444444444444444444444444444444444"
	)

	base := newManifest(nil, []string{baseR0Path})
	oursMfst := newManifest(nil, []string{oursRCPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(nil, []string{baseR0Path, theirsRTPath})

	refsByTable := map[string]map[string]string{
		baseR0Path:   {"refs/heads/main": mainOID},
		oursRCPath:   {"refs/heads/main": newMainOID},
		theirsRTPath: {"refs/heads/main": newMainOID},
	}
	readRefs := func(path string) (map[string]string, error) {
		refs, ok := refsByTable[filepath.Join("refs", filepath.Base(path))]
		if !ok {
			t.Fatalf("readRefs invoked on unexpected reftable %q", path)
		}
		return maps.Clone(refs), nil
	}
	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case filepath.Base(baseR0Path):
			return 1, 1, nil
		case filepath.Base(theirsRTPath):
			return 2, 2, nil
		case filepath.Base(oursRCPath):
			return 1, 2, nil
		}
		t.Fatalf("readRange invoked on unexpected reftable %q", path)
		return 0, 0, nil
	}
	patchRange := func(path string, min, max uint64) error {
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	require.NoError(t, s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	}), "both sides naming the same target must merge cleanly")
}

// TestMerger_ThreeWayMerge_compactedOurs_droppedTombstone covers ours
// deleting a ref and then compacting from the bottom of the stack,
// which drops the tombstone. Neither side's diff overlaps the other's,
// but the merged stack keeps theirs' copy of base's table below ours'
// compacted one, so the deleted ref resolves to its base value again.
func TestMerger_ThreeWayMerge_compactedOurs_droppedTombstone(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	baseR0Path := "refs/baser0.ref"
	oursRCPath := "refs/ourscompacted.ref"
	theirsRTPath := "refs/theirstable.ref"
	for _, p := range []string{baseR0Path, oursRCPath, theirsRTPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	const (
		mainOID    = "1111111111111111111111111111111111111111"
		featureOID = "2222222222222222222222222222222222222222"
		devOID     = "3333333333333333333333333333333333333333"
	)

	base := newManifest(nil, []string{baseR0Path})
	// Ours deleted refs/heads/feature, then compacted base's table and
	// the tombstone into one table that no longer mentions the ref.
	oursMfst := newManifest(nil, []string{oursRCPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	// Theirs appended a table creating an unrelated ref.
	theirsMfst := newManifest(nil, []string{baseR0Path, theirsRTPath})

	refsByTable := map[string]map[string]string{
		baseR0Path:   {"refs/heads/main": mainOID, "refs/heads/feature": featureOID},
		oursRCPath:   {"refs/heads/main": mainOID},
		theirsRTPath: {"refs/heads/dev": devOID},
	}
	readRefs := func(path string) (map[string]string, error) {
		refs, ok := refsByTable[filepath.Join("refs", filepath.Base(path))]
		if !ok {
			t.Fatalf("readRefs invoked on unexpected reftable %q", path)
		}
		return maps.Clone(refs), nil
	}
	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case filepath.Base(baseR0Path):
			return 1, 1, nil
		case filepath.Base(theirsRTPath):
			return 2, 2, nil
		case filepath.Base(oursRCPath):
			return 1, 2, nil
		}
		t.Fatalf("readRange invoked on unexpected reftable %q", path)
		return 0, 0, nil
	}
	patchRange := func(path string, min, max uint64) error {
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.ErrorIs(t, err, ErrOverlap, "a delete the merged stack cannot represent must be reported as overlap")

	pointer, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	require.Equal(t, oursSha, string(pointer))
}

// TestMerger_ThreeWayMerge_partialCompactionOurs_rollsBack covers ours
// compacting a range in the middle of the stack. The compacted table
// carries an older value of a ref than a later base table that both
// sides still hold, so appending it above theirs' stack rolls the ref
// back. Neither side changed the ref, so it is in neither diff.
func TestMerger_ThreeWayMerge_partialCompactionOurs_rollsBack(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "refs"), 0o755))

	baseR0Path := "refs/baser0.ref"
	baseR1Path := "refs/baser1.ref"
	baseR2Path := "refs/baser2.ref"
	baseR3Path := "refs/baser3.ref"
	oursRCPath := "refs/ourscompacted.ref"
	oursRTPath := "refs/ourstable.ref"
	theirsRTPath := "refs/theirstable.ref"
	for _, p := range []string{baseR0Path, baseR1Path, baseR2Path, baseR3Path, oursRCPath, oursRTPath, theirsRTPath} {
		require.NoError(t, os.WriteFile(
			filepath.Join(cacheDir, p),
			[]byte("bytes-of-"+filepath.Base(p)),
			0o644,
		))
	}

	const (
		mainOID  = "1111111111111111111111111111111111111111"
		v1OID    = "2222222222222222222222222222222222222222"
		v2OID    = "3333333333333333333333333333333333333333"
		v3OID    = "4444444444444444444444444444444444444444"
		oursOID  = "5555555555555555555555555555555555555555"
		devOID   = "6666666666666666666666666666666666666666"
		movedRef = "refs/heads/moved"
	)

	base := newManifest(nil, []string{baseR0Path, baseR1Path, baseR2Path, baseR3Path})
	// Ours compacted R1..R2 into one table and appended its own write.
	oursMfst := newManifest(nil, []string{baseR0Path, oursRCPath, baseR3Path, oursRTPath})
	oursSha := writeManifestBody(t, s, oursMfst)
	// Theirs appended a table creating an unrelated ref.
	theirsMfst := newManifest(nil, []string{baseR0Path, baseR1Path, baseR2Path, baseR3Path, theirsRTPath})

	refsByTable := map[string]map[string]string{
		baseR0Path:   {"refs/heads/main": mainOID},
		baseR1Path:   {movedRef: v1OID},
		baseR2Path:   {movedRef: v2OID},
		baseR3Path:   {movedRef: v3OID},
		oursRCPath:   {movedRef: v2OID},
		oursRTPath:   {"refs/heads/ours": oursOID},
		theirsRTPath: {"refs/heads/dev": devOID},
	}
	readRefs := func(path string) (map[string]string, error) {
		refs, ok := refsByTable[filepath.Join("refs", filepath.Base(path))]
		if !ok {
			t.Fatalf("readRefs invoked on unexpected reftable %q", path)
		}
		return maps.Clone(refs), nil
	}
	readRange := func(path string) (uint64, uint64, error) {
		switch filepath.Base(path) {
		case filepath.Base(baseR0Path):
			return 1, 1, nil
		case filepath.Base(baseR1Path):
			return 2, 2, nil
		case filepath.Base(baseR2Path):
			return 3, 3, nil
		case filepath.Base(baseR3Path):
			return 4, 4, nil
		case filepath.Base(theirsRTPath), filepath.Base(oursRTPath):
			return 5, 5, nil
		case filepath.Base(oursRCPath):
			return 2, 3, nil
		}
		t.Fatalf("readRange invoked on unexpected reftable %q", path)
		return 0, 0, nil
	}
	patchRange := func(path string, min, max uint64) error {
		return os.WriteFile(path, []byte("rebased"), 0o644)
	}

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	err := s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableRefs:         readRefs,
		ReadReftableUpdateRange:  readRange,
		PatchReftableUpdateRange: patchRange,
	})
	require.ErrorIs(t, err, ErrOverlap, "a rollback the merged stack would introduce must be reported as overlap")

	pointer, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	require.Equal(t, oursSha, string(pointer))
}

// TestMerger_ThreeWayMerge_theirsPackDeletionSurvives covers theirs
// dropping a pack that base carried — a repack, or housekeeping
// pruning it. Differencing ours' packs against theirs instead of
// against base hands the dropped pack straight back, and garbage
// collection is free to delete it as soon as theirs' commit made it
// unreachable from the durable pointer. The merged manifest would
// then name an artifact that no longer exists.
func TestMerger_ThreeWayMerge_theirsPackDeletionSurvives(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, cacheDir := newTestMerger(t)

	// Base carries p0 and old. Theirs repacked: it dropped old and
	// added new. Ours only added s1 on top of base.
	base := newManifest(expandPacks("old", "p0"), nil)
	oursMfst := newManifest(expandPacks("old", "p0", "s1"), nil)
	oursSha := writeManifestBody(t, s, oursMfst)
	theirsMfst := newManifest(expandPacks("new", "p0"), nil)

	pointerPath := filepath.Join(t.TempDir(), "pointer")
	require.NoError(t, os.WriteFile(pointerPath, []byte(oursSha), 0o644))

	require.NoError(t, s.ThreeWayMerge(ctx, pointerPath, oursMfst, base, theirsMfst, MergeOpts{
		ReadReftableUpdateRange:  panickyReadRange,
		PatchReftableUpdateRange: panickyPatchRange,
	}))

	newSha, err := os.ReadFile(pointerPath)
	require.NoError(t, err)
	body, err := os.ReadFile(manifestFilePath(t, cacheDir, string(newSha)))
	require.NoError(t, err)
	merged, err := ParseManifest(body)
	require.NoError(t, err)

	require.ElementsMatch(t, expandPacks("new", "p0", "s1"), merged.ObjPaths(),
		"theirs' pack deletion must survive the merge")
}

// TestMerger_ThreeWayMerge_keepsOBJSMembership is gitaly#7465: Git keeps
// the commit-graph in OBJS next to the packs and only reads it from there.
// A repack (theirs) replaced the graph while a push (ours) added a pack;
// the merged OBJS must hold theirs' graph and both sides' packs, rather
// than recompute OBJS from the "pack/" prefix and leave the graph outside.
func TestMerger_ThreeWayMerge_keepsOBJSMembership(t *testing.T) {
	t.Parallel()

	base, _ := gitManifest(t, append([]string{"graph/g1.graph"}, expandPacks("p0")...), nil, nil)
	ours, oursBody := gitManifest(t, append([]string{"graph/g1.graph"}, expandPacks("p0", "s1")...), nil, nil)
	theirs, _ := gitManifest(t, append([]string{"graph/g2.graph", "midx/m2.midx"}, expandPacks("p1")...), nil, nil)

	merged, err := mergeGitManifests(t, base, ours, theirs, oursBody)
	require.NoError(t, err)
	require.ElementsMatch(t, append([]string{"graph/g2.graph", "midx/m2.midx"}, expandPacks("p1", "s1")...), merged.ObjPaths())
	require.Empty(t, outsidePaths(merged))
}

// TestMerger_ThreeWayMerge_otherPaths covers every path the merge has no
// rule for: neither a pack in OBJS nor a reftable in REFS, inside OBJS or
// not. One side's set is taken whole, removals and membership included,
// and a merge that would need to know what the paths mean is ErrOverlap.
func TestMerger_ThreeWayMerge_otherPaths(t *testing.T) {
	t.Parallel()

	type side struct{ objs, other []string }
	packs := expandPacks("p0")
	for _, tc := range []struct {
		desc                string
		base, ours, theirs  side
		wantObjs, wantOther []string
		wantOverlap         bool
	}{
		{
			desc:      "only theirs changed them",
			base:      side{other: []string{"x/a"}},
			ours:      side{other: []string{"x/a"}},
			theirs:    side{objs: []string{"graph/g.graph"}},
			wantObjs:  []string{"graph/g.graph"},
			wantOther: nil,
		},
		{
			desc:      "ours removed one theirs kept",
			base:      side{other: []string{"x/a", "x/b"}},
			ours:      side{other: []string{"x/b"}},
			theirs:    side{other: []string{"x/a", "x/b"}},
			wantOther: []string{"x/b"},
		},
		{
			desc:      "both made the same change",
			base:      side{other: []string{"x/a"}},
			ours:      side{other: []string{"x/a", "x/n"}},
			theirs:    side{other: []string{"x/a", "x/n"}},
			wantOther: []string{"x/a", "x/n"},
		},
		{
			desc:        "both added different paths",
			base:        side{other: []string{"x/a"}},
			ours:        side{other: []string{"x/a", "x/o"}},
			theirs:      side{other: []string{"x/a", "x/t"}},
			wantOverlap: true,
		},
		{
			// Both sides rewrote the commit-graph to different values. A
			// commit-graph names commits, which the merged packs still hold
			// under the reachability precondition, so either graph stays
			// valid; mergeAux keeps theirs rather than drop it (gitaly!9388).
			desc:     "both rewrote the graph",
			base:     side{objs: []string{"graph/g1.graph"}},
			ours:     side{objs: []string{"graph/g2.graph"}},
			theirs:   side{objs: []string{"graph/g3.graph"}},
			wantObjs: []string{"graph/g3.graph"},
		},
		{
			desc:        "same paths, membership changed on one side only",
			base:        side{other: []string{"x/a"}},
			ours:        side{other: []string{"x/a", "x/o"}},
			theirs:      side{objs: []string{"x/a"}},
			wantOverlap: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			build := func(s side) (*Manifest, []byte) {
				return gitManifest(t, append(slices.Clone(s.objs), packs...), nil, s.other)
			}
			base, _ := build(tc.base)
			ours, oursBody := build(tc.ours)
			theirs, _ := build(tc.theirs)

			merged, err := mergeGitManifests(t, base, ours, theirs, oursBody)
			if tc.wantOverlap {
				require.ErrorIs(t, err, ErrOverlap)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, append(slices.Clone(tc.wantObjs), packs...), merged.ObjPaths())
			require.ElementsMatch(t, tc.wantOther, outsidePaths(merged))
		})
	}
}

// TestMerger_ThreeWayMerge_packRemoval covers the second half of gitaly#7465:
// the merge must apply ours' pack removals, not just its additions, so a pack a
// concurrent repack retired stays retired instead of being resurrected by a
// set-union.
func TestMerger_ThreeWayMerge_packRemoval(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc               string
		base, ours, theirs []string
		want               []string
	}{
		{
			// Ours repacked p0+p1 into p2; theirs left them. The retired p0/p1
			// must not come back.
			desc:   "ours retired packs theirs kept stay retired",
			base:   []string{"p0", "p1"},
			ours:   []string{"p2"},
			theirs: []string{"p0", "p1"},
			want:   []string{"p2"},
		},
		{
			desc:   "ours added a pack",
			base:   []string{"p0"},
			ours:   []string{"p0", "p1"},
			theirs: []string{"p0"},
			want:   []string{"p0", "p1"},
		},
		{
			// Ours repacked p0,p1 into p2 while theirs pushed p3. Ours'
			// retirement sticks and theirs' new pack is added.
			desc:   "ours repacked while theirs pushed",
			base:   []string{"p0", "p1"},
			ours:   []string{"p2"},
			theirs: []string{"p0", "p1", "p3"},
			want:   []string{"p2", "p3"},
		},
		{
			// Each side retired a pack the other kept. A repack omits objects
			// already in a pack it keeps, so neither retirement is safe to apply
			// against the other side; keep every pack of both.
			desc:   "both retired the other's kept pack keeps both",
			base:   []string{"p0", "p1"},
			ours:   []string{"p0", "pa"},
			theirs: []string{"p1", "pb"},
			want:   []string{"p0", "p1", "pa", "pb"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			build := func(slugs []string) (*Manifest, []byte) {
				return gitManifest(t, expandPacks(slugs...), nil, nil)
			}
			base, _ := build(tc.base)
			ours, oursBody := build(tc.ours)
			theirs, _ := build(tc.theirs)

			merged, err := mergeGitManifests(t, base, ours, theirs, oursBody)
			require.NoError(t, err)
			require.ElementsMatch(t, expandPacks(tc.want...), merged.ObjPaths())
		})
	}
}

// TestMerger_ThreeWayMerge_auxBothChanged covers two housekeeping runs racing:
// each publishes its own rolled-up midx and commit-graph, so both sides change
// them against base. mergeMidx (midx) and mergeAux (commit-graph) keep one side
// rather than dropping both, since dropping a midx forces git pack-objects into
// a slow reachability walk and clones ran 1.4-2x slower in private beta
// (gitaly!9388). The midx keeps theirs when all of theirs' packs survive, else
// ours on the same test, dropping only when neither side's packs are fully
// kept; the commit-graph is kept regardless, as it names commits, not packs.
func TestMerger_ThreeWayMerge_auxBothChanged(t *testing.T) {
	t.Parallel()

	// obj builds an OBJS list from pack slugs plus any aux paths; the aux
	// paths sort ahead of the packs (graph/ < midx/ < pack/), so the run
	// stays contiguous.
	obj := func(packs []string, aux ...string) []string {
		return append(slices.Clone(aux), expandPacks(packs...)...)
	}
	for _, tc := range []struct {
		desc               string
		base, ours, theirs []string
		wantPacks          []string
		wantAux            []string // midx/graph kept in OBJS
	}{
		{
			// Both repacked; theirs' packs all survive, so theirs' aux wins.
			desc:      "both repacked, keep theirs",
			base:      obj([]string{"p1", "p2", "p3", "p4"}, "graph/g0.graph", "midx/m0.midx"),
			ours:      obj([]string{"po", "p3", "p4"}, "graph/go.graph", "midx/mo.midx"),
			theirs:    obj([]string{"pt", "p4"}, "graph/gt.graph", "midx/mt.midx"),
			wantPacks: []string{"p4", "po", "pt"},
			wantAux:   []string{"graph/gt.graph", "midx/mt.midx"},
		},
		{
			// Two nodes rolled up the same base and wrote the same graph.
			desc:      "two nodes wrote the same graph",
			base:      obj([]string{"p1", "p2", "p3"}),
			ours:      obj([]string{"po"}, "graph/gs.graph", "midx/mo.midx"),
			theirs:    obj([]string{"pt"}, "graph/gs.graph", "midx/mt.midx"),
			wantPacks: []string{"po", "pt"},
			wantAux:   []string{"graph/gs.graph", "midx/mt.midx"},
		},
		{
			// Ours retired p3, which theirs' midx indexes, so theirs' packs
			// are not all kept and ours' midx wins. The graph is forced
			// valid, so theirs' still wins.
			desc:      "both repacked, theirs lost a pack so ours' midx wins",
			base:      obj([]string{"p1", "p2", "p3", "p4"}, "graph/g0.graph", "midx/m0.midx"),
			ours:      obj([]string{"po", "p4"}, "graph/go.graph", "midx/mo.midx"),
			theirs:    obj([]string{"pt", "p3", "p4"}, "graph/gt.graph", "midx/mt.midx"),
			wantPacks: []string{"p4", "po", "pt"},
			wantAux:   []string{"graph/gt.graph", "midx/mo.midx"},
		},
		{
			// Theirs dropped its aux (an earlier merge), and ours lost p3
			// that its midx indexes, so the midx drops but the graph, which
			// stays valid, is kept.
			desc:      "both changed the midx, only ours has one, and ours lost a pack",
			base:      obj([]string{"p1", "p2", "p3"}, "graph/g0.graph", "midx/m0.midx"),
			ours:      obj([]string{"po", "p3"}, "graph/go.graph", "midx/mo.midx"),
			theirs:    obj([]string{"pt"}),
			wantPacks: []string{"po", "pt"},
			wantAux:   []string{"graph/go.graph"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			base, _ := gitManifest(t, tc.base, nil, nil)
			ours, oursBody := gitManifest(t, tc.ours, nil, nil)
			theirs, _ := gitManifest(t, tc.theirs, nil, nil)

			merged, err := mergeGitManifests(t, base, ours, theirs, oursBody)
			require.NoError(t, err)
			require.ElementsMatch(t, append(slices.Clone(tc.wantAux), expandPacks(tc.wantPacks...)...), merged.ObjPaths())
			require.Empty(t, outsidePaths(merged))
		})
	}
}

// TestMerger_ThreeWayMerge_auxOneSide covers the single-side midx branches:
// only ours changed it. It is kept while ours' packs survive, and dropped once
// a pack it indexes is gone.
func TestMerger_ThreeWayMerge_auxOneSide(t *testing.T) {
	t.Parallel()

	obj := func(packs []string, aux ...string) []string {
		return append(slices.Clone(aux), expandPacks(packs...)...)
	}
	for _, tc := range []struct {
		desc               string
		base, ours, theirs []string
		wantPacks          []string
		wantAux            []string
	}{
		{
			// Ours rewrote the midx; theirs only pushed a pack and left the
			// midx alone. Ours' packs all survive, so ours' midx is kept.
			desc:      "ours' midx kept when theirs added a pack",
			base:      obj([]string{"p0"}, "midx/m0.midx"),
			ours:      obj([]string{"p0"}, "midx/mo.midx"),
			theirs:    obj([]string{"p0", "pt"}, "midx/m0.midx"),
			wantPacks: []string{"p0", "pt"},
			wantAux:   []string{"midx/mo.midx"},
		},
		{
			// Ours rewrote the midx over p0,p1, but theirs retired p1, so a
			// pack ours' midx indexes is gone and the midx is dropped.
			desc:      "ours' midx dropped when it loses a pack",
			base:      obj([]string{"p0", "p1"}, "midx/m0.midx"),
			ours:      obj([]string{"p0", "p1"}, "midx/mo.midx"),
			theirs:    obj([]string{"p0"}, "midx/m0.midx"),
			wantPacks: []string{"p0"},
			wantAux:   nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			base, _ := gitManifest(t, tc.base, nil, nil)
			ours, oursBody := gitManifest(t, tc.ours, nil, nil)
			theirs, _ := gitManifest(t, tc.theirs, nil, nil)

			merged, err := mergeGitManifests(t, base, ours, theirs, oursBody)
			require.NoError(t, err)
			require.ElementsMatch(t, append(slices.Clone(tc.wantAux), expandPacks(tc.wantPacks...)...), merged.ObjPaths())
			require.Empty(t, outsidePaths(merged))
		})
	}
}

// TestMerger_ThreeWayMerge_graphChain covers a split commit-graph, which Gitaly
// always writes (`git commit-graph write --split`). A chain is several graph/
// paths, so mergeAux's single-path rule does not apply; the chain is carried
// leniently as unknown paths, exactly as the pre-removal union did, instead of
// failing because more than one graph/ path is named.
func TestMerger_ThreeWayMerge_graphChain(t *testing.T) {
	t.Parallel()

	chain := []string{"graph/g1a.graph", "graph/g1b.graph"}
	for _, tc := range []struct {
		desc               string
		base, ours, theirs []string // graph/ paths per side
		wantGraph          []string
		wantOverlap        bool
	}{
		{
			desc:      "identical chain carried unchanged",
			base:      chain,
			ours:      chain,
			theirs:    chain,
			wantGraph: chain,
		},
		{
			desc:      "ours grew the chain, theirs unchanged",
			base:      chain,
			ours:      append(slices.Clone(chain), "graph/g1c.graph"),
			theirs:    chain,
			wantGraph: append(slices.Clone(chain), "graph/g1c.graph"),
		},
		{
			desc:        "both grew the chain differently",
			base:        chain,
			ours:        append(slices.Clone(chain), "graph/g1c.graph"),
			theirs:      append(slices.Clone(chain), "graph/g1d.graph"),
			wantOverlap: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			build := func(graph []string) (*Manifest, []byte) {
				return gitManifest(t, append(slices.Clone(graph), expandPacks("p0")...), nil, nil)
			}
			base, _ := build(tc.base)
			ours, oursBody := build(tc.ours)
			theirs, _ := build(tc.theirs)

			merged, err := mergeGitManifests(t, base, ours, theirs, oursBody)
			if tc.wantOverlap {
				require.ErrorIs(t, err, ErrOverlap)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, append(slices.Clone(tc.wantGraph), expandPacks("p0")...), merged.ObjPaths())
			require.Empty(t, outsidePaths(merged))
		})
	}
}

// TestMerger_ThreeWayMerge_midxChain covers an incremental multi-pack-index
// chain (several midx/ paths). mergeMidx merges the midx as a set, gated on the
// owning side's packs, so a chain is kept while its packs survive and dropped
// once one is retired. Unlike a commit-graph chain it is not carried leniently,
// since a midx names packfiles and a stale one would dangle over gone packs.
func TestMerger_ThreeWayMerge_midxChain(t *testing.T) {
	t.Parallel()

	chain := []string{"midx/a.midx", "midx/b.midx"}
	obj := func(midx, packs []string) []string {
		return append(slices.Clone(midx), expandPacks(packs...)...)
	}
	for _, tc := range []struct {
		desc               string
		base, ours, theirs []string
		wantPacks          []string
		wantMidx           []string
	}{
		{
			// Ours retired p0,p1, which the unchanged chain's packs include,
			// so theirs' packs are not all kept and the chain is dropped
			// rather than left dangling over gone packs.
			desc:      "stale chain dropped when its packs are retired",
			base:      obj(chain, []string{"p0", "p1"}),
			ours:      obj(chain, []string{"p2"}),
			theirs:    obj(chain, []string{"p0", "p1", "p3"}),
			wantPacks: []string{"p2", "p3"},
			wantMidx:  nil,
		},
		{
			// Theirs only added a pack; the chain's packs all survive, so it
			// rides through.
			desc:      "chain kept when its packs survive",
			base:      obj(chain, []string{"p0"}),
			ours:      obj(chain, []string{"p0", "p1"}),
			theirs:    obj(chain, []string{"p0"}),
			wantPacks: []string{"p0", "p1"},
			wantMidx:  chain,
		},
		{
			// Ours repacked to a new chain and retired p0,p1; ours' packs
			// survive, so ours' chain wins over theirs' now-stale one.
			desc:      "ours' new chain wins over theirs' stale one",
			base:      obj([]string{"midx/a.midx"}, []string{"p0", "p1"}),
			ours:      obj([]string{"midx/c.midx", "midx/d.midx"}, []string{"p2"}),
			theirs:    obj([]string{"midx/a.midx"}, []string{"p0", "p1", "p3"}),
			wantPacks: []string{"p2", "p3"},
			wantMidx:  []string{"midx/c.midx", "midx/d.midx"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			base, _ := gitManifest(t, tc.base, nil, nil)
			ours, oursBody := gitManifest(t, tc.ours, nil, nil)
			theirs, _ := gitManifest(t, tc.theirs, nil, nil)

			merged, err := mergeGitManifests(t, base, ours, theirs, oursBody)
			require.NoError(t, err)
			require.ElementsMatch(t, append(slices.Clone(tc.wantMidx), expandPacks(tc.wantPacks...)...), merged.ObjPaths())
			require.Empty(t, outsidePaths(merged))
		})
	}
}

// TestMerger_ThreeWayMerge_unknownChunk: a chunk other than PATH, OBJS and
// REFS may index into PATH, which every merge rewrites, so it cannot be
// carried even when all three manifests have the same bytes. The manifest
// still parses; only the merge declines.
func TestMerger_ThreeWayMerge_unknownChunk(t *testing.T) {
	t.Parallel()

	const chunkIDFuture = 0x46555455 // "FUTU"
	future := chunkSpec{chunkIDFuture, []byte{0, 0, 0, 1}}
	for _, tc := range []struct {
		desc     string
		withMeta [3]bool // base, ours, theirs
	}{
		{"in base", [3]bool{true, false, false}},
		{"in ours", [3]bool{false, true, false}},
		{"in theirs", [3]bool{false, false, true}},
		{"unchanged in all three", [3]bool{true, true, true}},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			build := func(withChunk bool, packs ...string) (*Manifest, []byte) {
				var extra []chunkSpec
				if withChunk {
					extra = append(extra, future)
				}
				return gitManifest(t, expandPacks(packs...), nil, nil, extra...)
			}
			base, _ := build(tc.withMeta[0], "p0")
			ours, oursBody := build(tc.withMeta[1], "p0", "s1")
			theirs, _ := build(tc.withMeta[2], "p0", "t1")

			_, err := mergeGitManifests(t, base, ours, theirs, oursBody)
			require.ErrorIs(t, err, ErrOverlap)
		})
	}
}

// TestMerger_ThreeWayMerge_objsNotContiguous: ours added "pack/p1.keep"
// outside OBJS, and theirs added p2 inside it. The merged PATH sorts the
// keep file between p0 and p2, so OBJS cannot be written as one range and
// the merge must decline rather than write a different OBJS.
func TestMerger_ThreeWayMerge_objsNotContiguous(t *testing.T) {
	t.Parallel()

	base, _ := gitManifest(t, expandPacks("p0"), nil, nil)
	ours, oursBody := gitManifest(t, expandPacks("p0"), nil, []string{"pack/p1.keep"})
	theirs, _ := gitManifest(t, expandPacks("p0", "p2"), nil, nil)

	_, err := mergeGitManifests(t, base, ours, theirs, oursBody)
	require.ErrorIs(t, err, ErrOverlap)
}
