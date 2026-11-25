package mvcc

// This file implements the three-way manifest merge that reconciles two MVCC
// writers that raced for the same repository.
//
// # Background: refs are a stack of reftables
//
// In the MVCC backend a repository's references are not one file but a stack of
// content-addressed reftable files, each stored at "refs/<hash>.ref" and
// carrying an update-index range [min, max]. Higher-index tables sit on top and
// shadow lower ones, so a ref's current value is whatever the topmost table
// that mentions it says. A manifest's REFS chunk is the ordered list of these
// table paths (oldest first). A write appends one new table on top (a higher
// update index) with its ref changes, producing a new manifest with one extra
// entry. Packs, the multi-pack-index and the commit-graph live in the OBJS
// chunk and are likewise content-addressed and immutable.
//
// # The conflict
//
// Two writers both start from a base manifest whose stack is [B]:
//
//	ours   publishes [B, O]   (O written at update-index 2)
//	theirs publishes [B, T]   (T written at update-index 2)
//
// Theirs wins the compare-and-swap, so the durable pointer is now [B, T] and
// ours is stale. Rather than make the client redo the whole push, the merge
// produces "theirs with ours' changes applied on top". O was written at
// index 2 but theirs already occupies 2, so O is rebased above theirs (its
// update-index range shifted, which rewrites its bytes into a new table O')
// and the result is [B, T, O']. If instead both sides changed the same ref to
// different targets, there is nothing to reconcile and the merge returns
// ErrOverlap for the client to redrive.
//
// ThreeWayMerge documents the algorithm in full: the exclusive-set and
// overlap-detection reasoning, the rebase, and why rebasing is split from
// uploading so a merge the format cannot represent orphans nothing.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/sync/errgroup"
)

// reftableInfo holds the update-index range extracted from a single
// reftable during the parallel fetch+parse step of ThreeWayMerge.
// Ranges drive the rebase in step 4.
type reftableInfo struct {
	minIdx uint64
	maxIdx uint64
}

// MergeOpts supplies the reftable callbacks that ThreeWayMerge needs to
// check for ref-level overlap and to rebase update-index ranges. All
// three callbacks are injected to avoid an import cycle with
// internal/git/reftable (see the comments on the callback types in
// resolver.go).
type MergeOpts struct {
	// ReadReftableRefs parses a single reftable and returns its
	// references. When nil, the overlap check is skipped: the merge
	// proceeds as if the ref sets were disjoint. That is the right
	// behaviour for pack-only flows (no refs to conflict on);
	// ref-changing flows must wire a reader (reftable.ReadRefsAt
	// satisfies this).
	ReadReftableRefs ReadReftableRefs

	// ReadReftableUpdateRange + PatchReftableUpdateRange drive the
	// reftable rebase. Both are required: without them the merge
	// cannot resequence staged update-index ranges past the theirs
	// stack. See the comments on the callback types in resolver.go.
	ReadReftableUpdateRange  ReadReftableUpdateRange
	PatchReftableUpdateRange PatchReftableUpdateRange
}

// ThreeWayMerge performs a 3-way manifest merge: given a base manifest
// (the common ancestor), an ours manifest (the staged write that lost
// the CAS race), and a theirs manifest (the concurrent winner), it
// checks whether the two sides modified disjoint refs and, if so,
// produces a merged manifest that combines theirs' state with ours'
// exclusive additions.
//
// Overlap detection combines membership with values, because the
// merge is stack-based while a pure diff is not. We reconstruct each
// side's final ref state by folding its reftable stack, then report
// ErrOverlap when a refname theirs changed relative to base is also
// shadowed by ours and the two sides disagree on its target.
//
// "Shadowed by ours" is membership, not change: the merged stack is
// theirs verbatim with ours' exclusive reftables appended on top, so
// those tables win for every refname they *mention*. A refname an
// ours-exclusive table carries forward at its base value is absent
// from ours' diff yet still overwrites whatever theirs did to it, so
// a diff-only predicate misses the overlap and the merge silently
// drops theirs' write. Compaction on ours produces that shape, and so
// does any write that sets a ref to the value it already had — a
// mirror sync, or a fetch of an unchanged ref.
//
// Values are still compared, so membership alone does not conflict:
// when both sides set the same ref to the same target (two clients
// racing identical `git fetch` operations), the merge succeeds. That
// is also what keeps compaction from producing false positives,
// since a carried-forward ref matches theirs unless theirs moved it.
//
// The I/O cost is essentially that of a membership-only check in the
// no-compaction case: readUniqueReftables detects the triply-shared
// prefix positionally and only reads the non-shared suffixes — which,
// when base is a prefix of both ours and theirs, is exactly the
// exclusive set. Compaction adds reads, but those are required for
// correctness.
//
// The merge then proceeds:
//
//  1. Compute exclusive reftable sets: ours_exclusive = ours \ base,
//     theirs_exclusive = theirs \ base. Only reftables in these sets
//     need their update-index ranges read for the rebase; shared
//     tables are content-addressed and identical on both sides.
//
//  2. Fetch and parse each exclusive reftable once for its update-
//     index range (needed for the rebase).
//
//  3. Run value-level overlap detection by reading every unique
//     non-triply-shared reftable, then check that the merged stack
//     folds to theirs' state with ours' changes applied on top;
//     return ErrOverlap on conflict.
//
//  4. Build the merged manifest from four kinds of path:
//
//     - Packs: mergePacks applies ours' additions and removals on top
//     of theirs, so a pack a concurrent repack retired stays retired
//     instead of being resurrected by a union.
//     - Midx: mergeMidx takes one side's set (single or chain), or drops
//     it when that side's packs are gone; Git rebuilds it from the packs.
//     - Commit-graph: mergeAux takes one side when each side has at most
//     one graph/ path; a graph chain is carried by mergeOtherPaths.
//     - Reftables: theirs' stack verbatim, then ours-exclusive tables
//     with their update-index ranges shifted past theirs' max.
//     - Any other path: taken from one side whole (mergeOtherPaths),
//     keeping its OBJS membership.
//
//     Writes are ordered so a failed merge leaves nothing behind. Each
//     rebased table is patched into the local cache (patchReftable) and
//     the merged manifest is assembled and validated first; only then
//     are the rebased tables uploaded and the pointer file updated. A
//     manifest the format cannot represent is rejected before any
//     durable write, so it never orphans an uploaded artifact.
//
// The manifest format belongs to Git, so the merge enforces none of it:
// no path is rejected for its name, and a path it has no rule for is
// carried, not dropped. It drops only a derived cache it does have a
// rule for, the midx or commit-graph, which Git rebuilds from the packs.
// It merges what it has a rule for and returns ErrOverlap rather than
// guess at the rest.
// Before any I/O it returns ErrOverlap when a manifest carries a chunk
// other than PATH, OBJS and REFS (such a chunk may index into PATH,
// which the merge rewrites) or when both sides changed the other paths
// differently.
//
// Operates on full relative paths throughout (e.g. "refs/abc.ref",
// "pack/def.pack") to match the binary chunk-based manifest's path-
// centric data model.
//
// pointerPath is the per-RPC manifest pointer file. Its current
// contents must name the ours manifest body. On success, the pointer
// is rewritten to the merged manifest sha.
//
// Returns ErrOverlap when the two sides cannot be reconciled. All other
// errors are wrapped with context.
//
// Precondition (not enforced): the merge reconciles which refs and
// artifacts each manifest names, not whether the objects behind them
// still exist. It assumes every object a side's refs point at is still
// in the merged object set.
//
// A pack drop from either side can break that assumption, now that the
// merge applies ours' retirements as well as theirs'. Say one side
// repacks with prune, or deletes a branch so its objects become
// unreachable, and drops a pack the other side's new commits were built
// on. The two sides' refs do not overlap, so the merge succeeds, yet the
// merged manifest names refs whose objects are gone. A repack is meant to
// roll the objects of a pack it retires into the pack it writes, but the
// merge does not check that it did.
//
// Closing this gap needs one of:
//   - a guarantee, enforced elsewhere, that retired objects stay present;
//   - a connectivity check on the merged result; or
//   - ErrOverlap when one side dropped packs while the other added objects.
//
// One of these must be in place before the merge runs on the commit path
// (part of gitaly#7372). Until then, callers must not rely on the merge
// to catch a pruned-base race.
func (m *Merger) ThreeWayMerge(
	ctx context.Context,
	pointerPath string,
	ours, base, theirs *Manifest,
	opts MergeOpts,
) error {
	if opts.ReadReftableUpdateRange == nil || opts.PatchReftableUpdateRange == nil {
		return fmt.Errorf("mvcc: ThreeWayMerge requires both ReadReftableUpdateRange and PatchReftableUpdateRange callbacks")
	}

	for _, side := range []struct {
		name     string
		manifest *Manifest
	}{{"base", base}, {"ours", ours}, {"theirs", theirs}} {
		if ids := side.manifest.unknownChunks; len(ids) > 0 {
			return fmt.Errorf("%w: %s manifest carries chunk %08x, which a merge cannot carry", ErrOverlap, side.name, ids[0])
		}
	}

	// The midx is always merged as a set by mergeMidx, so a midx chain is
	// handled the same way as a single one. The commit-graph gets mergeAux's
	// single-path rule only when every side carries at most one. Gitaly writes
	// split commit-graphs (`git commit-graph write --split`), so a chain lands
	// as several graph/ paths; when any side has more than one, the graph is
	// carried leniently as unknown paths instead (see mergeOtherPaths),
	// matching the pre-removal union.
	graphSingle := auxIsSingle(graphPrefix, base, ours, theirs)

	mergedOther, err := mergeOtherPaths(base, ours, theirs, graphSingle)
	if err != nil {
		return err
	}

	// Step 1: compute exclusive reftable sets relative to base.
	// Operates on full ref paths (e.g. "refs/abc.ref") since that's
	// the binary manifest's native data model.
	oursExclusiveRefPaths := diffSet(ours.RefPaths(), base.RefPaths())
	theirsExclusiveRefPaths := diffSet(theirs.RefPaths(), base.RefPaths())

	// A value-level conflict is only possible when both sides added reftables
	// relative to base: ours' exclusive tables stack on top of theirs, so
	// they can only shadow a ref theirs also changed. When both sets are
	// non-empty, overlap detection must read the tables; a nil reader there
	// would silently merge two conflicting ref writes as if disjoint
	// (checkValueLevelOverlap treats a nil reader as "no overlap", which is
	// only safe for a pack-only side). Require the reader rather than skip.
	if len(oursExclusiveRefPaths) > 0 && len(theirsExclusiveRefPaths) > 0 && opts.ReadReftableRefs == nil {
		return fmt.Errorf("mvcc: ThreeWayMerge requires ReadReftableRefs when both sides changed refs")
	}

	// Step 2: fetch and parse each exclusive reftable's update-index
	// range. Needed for the rebase in step 4.
	oursInfo := make([]reftableInfo, len(oursExclusiveRefPaths))
	theirsInfo := make([]reftableInfo, len(theirsExclusiveRefPaths))

	// The errgroup's derived context propagates the first error to the
	// other goroutines' fetches, but it does not stop them: ReadArtifact
	// is required to run the fetch detached from the caller's context
	// (see its contract on ObjectStorage), so cancelling gctx only makes
	// this goroutine stop waiting. The download finishes regardless and
	// lands in the local cache, which is what makes a retry cheap. Read
	// the idiom as "collect the first error", not as "stop the work".
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(artifactConcurrency)

	parseTable := func(refPath string, info *reftableInfo, side string) func() error {
		return func() error {
			if err := m.fetchArtifact(gctx, refPath); err != nil {
				return fmt.Errorf("mvcc: fetch %s reftable %s: %w", side, refPath, err)
			}
			absPath := filepath.Join(m.cacheDir, refPath)

			min, max, err := opts.ReadReftableUpdateRange(absPath)
			if err != nil {
				return fmt.Errorf("mvcc: read %s reftable %s range: %w", side, refPath, err)
			}
			info.minIdx = min
			info.maxIdx = max
			return nil
		}
	}

	for i, p := range oursExclusiveRefPaths {
		g.Go(parseTable(p, &oursInfo[i], "ours"))
	}
	for i, p := range theirsExclusiveRefPaths {
		g.Go(parseTable(p, &theirsInfo[i], "theirs"))
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Step 3: overlap detection. Reconstruct the ref state on each
	// side by folding its reftable stack, then compare what theirs
	// changed against what ours' exclusive tables shadow. ErrOverlap
	// is returned only when the two sides disagree on a target.
	if err := checkValueLevelOverlap(ctx, m, ours, base, theirs, oursExclusiveRefPaths, opts); err != nil {
		return err
	}

	// Step 4: merge manifests.
	//
	// Build the merged manifest's Paths and refStackOrder fields.
	// Paths is the union of all artifact paths (SerializeManifest
	// sorts it); refStackOrder is the reftable stack: theirs
	// verbatim, then ours-exclusive rebased.

	// Object paths. Packs: apply ours' adds and removes on top of theirs so a
	// pack a concurrent repack retired stays retired (gitaly#7465); a set-union
	// would hand it back. The midx and commit-graph live in OBJS, which the
	// target object store reads them from; each is merged below.
	mergedPacks, oursPacksKept, theirsPacksKept := mergePacks(base, ours, theirs)

	// The midx names packfiles, so mergeMidx keeps a side's midx (single or
	// chain) only while that side's packs survive. The commit-graph names
	// commits, not packs, so mergeAux keeps it regardless of retirements
	// (packs-kept forced true); a multi-path graph chain flows through
	// mergeOtherPaths above.
	mergedMidx := mergeMidx(base, ours, theirs, oursPacksKept, theirsPacksKept)

	var graph string
	if graphSingle {
		if graph, err = mergeAux((*Manifest).GraphPath, base, ours, theirs, true, true); err != nil {
			return err
		}
	}

	mergedObjPaths := make([]string, 0, len(mergedPacks)+len(mergedMidx)+1+len(mergedOther))
	mergedObjPaths = append(mergedObjPaths, mergedPacks...)
	mergedObjPaths = append(mergedObjPaths, mergedMidx...)
	if graph != "" {
		mergedObjPaths = append(mergedObjPaths, graph)
	}

	// Any remaining path the merge has no rule for keeps the OBJS membership it
	// had on the side it came from (see mergeOtherPaths): the manifest format
	// belongs to Git, so an unknown path is carried, not dropped.
	var mergedOutside []string
	for _, p := range slices.Sorted(maps.Keys(mergedOther)) {
		if mergedOther[p] {
			mergedObjPaths = append(mergedObjPaths, p)
		} else {
			mergedOutside = append(mergedOutside, p)
		}
	}

	// Reftable stack: theirs verbatim, then ours-exclusive rebased past
	// theirs' max update index.
	//
	// The rebase is split from the upload: patchReftable writes each
	// rebased table to the local cache only. That lets the merged manifest
	// be assembled and validated below before anything is written to
	// durable storage, so a merge the format cannot represent is rejected
	// with nothing to orphan. Only once it validates are the rebased
	// tables uploaded.
	mergedRefStack := make([]string, 0, len(theirs.RefPaths())+len(oursExclusiveRefPaths))
	mergedRefStack = append(mergedRefStack, theirs.RefPaths()...)

	// rebasedToUpload holds the locally rebased tables that must be made
	// durable once the merge validates. The no-rebase branch adds nothing:
	// ours' own tables are already durable from its original push.
	var rebasedToUpload []string

	if len(oursExclusiveRefPaths) > 0 {
		// See reftableMaxIndex for how theirs' highest update
		// index is found.
		theirsMax, err := m.reftableMaxIndex(ctx, theirs.RefPaths(), theirsExclusiveRefPaths, theirsInfo, opts.ReadReftableUpdateRange)
		if err != nil {
			return err
		}

		// Reftable stacks are ordered by update-index, so the first
		// exclusive table has the lowest MinUpdateIndex. Take the min
		// over every exclusive range rather than trusting that order:
		// overstating oursMin would collide ours with theirs, so the
		// safe direction is the true minimum (mirrors reftableMaxIndex,
		// which maxes over theirs' ranges for the same reason).
		oursMin := oursInfo[0].minIdx
		for _, info := range oursInfo[1:] {
			oursMin = min(oursMin, info.minIdx)
		}

		if oursMin <= theirsMax {
			// Rebasing shifts ours' update indices up by delta. Guard the
			// arithmetic against uint64 overflow: it cannot happen in
			// practice (an index is incremented once per write, so reaching
			// 2^64 is unattainable), but a silent wrap would produce
			// colliding indices, so reject rather than risk it.
			if theirsMax == math.MaxUint64 {
				return fmt.Errorf("mvcc: reftable update index at uint64 max, cannot rebase")
			}
			delta := theirsMax + 1 - oursMin

			oursMax := oursInfo[0].maxIdx
			for _, info := range oursInfo[1:] {
				oursMax = max(oursMax, info.maxIdx)
			}
			if oursMax > math.MaxUint64-delta {
				return fmt.Errorf("mvcc: rebased reftable update index would overflow uint64")
			}

			rebased := make([]string, len(oursExclusiveRefPaths))
			// Patching is local file I/O, so a plain errgroup (no
			// context) bounds the fan-out; uploads happen later.
			var pg errgroup.Group
			pg.SetLimit(artifactConcurrency)
			for i, p := range oursExclusiveRefPaths {
				newMin := oursInfo[i].minIdx + delta
				newMax := oursInfo[i].maxIdx + delta
				pg.Go(func() error {
					newPath, err := m.patchReftable(p, newMin, newMax, opts.PatchReftableUpdateRange)
					if err != nil {
						return fmt.Errorf("mvcc: rebase ours reftable %s: %w", p, err)
					}
					rebased[i] = newPath
					return nil
				})
			}
			if err := pg.Wait(); err != nil {
				return err
			}
			mergedRefStack = append(mergedRefStack, rebased...)
			rebasedToUpload = rebased
		} else {
			// Ours is already strictly above theirs; no rebase needed.
			mergedRefStack = append(mergedRefStack, oursExclusiveRefPaths...)
		}
	}

	// Assemble and validate the merged manifest before anything is
	// uploaded. HashAlgo defaults to SHA-1 inside SerializeManifest when
	// zero, so passing theirs.HashAlgo (the durable winner's algorithm) is
	// sufficient for all valid 3-way merges (all three manifests belong to
	// the same repository). An OBJS that cannot be one run, or a path the
	// two sides gave different roles, is a merge the format cannot
	// represent; a reftable both sides carried is deduped, not rejected.
	merged, err := assembleManifest(theirs.HashAlgo, mergedObjPaths, mergedRefStack, mergedOutside)
	if err != nil {
		return asOverlap(err, "assemble merged manifest")
	}

	// The merge is representable, so make the rebased tables durable, then
	// write the merged body and repoint. Order matters: the pointer must
	// never name a manifest whose artifacts are not yet durable.
	if len(rebasedToUpload) > 0 {
		ug, ugctx := errgroup.WithContext(ctx)
		ug.SetLimit(artifactConcurrency)
		for _, p := range rebasedToUpload {
			ug.Go(func() error {
				if err := m.uploadFromCache(ugctx, p); err != nil {
					return fmt.Errorf("mvcc: upload rebased reftable %s: %w", p, err)
				}
				return nil
			})
		}
		if err := ug.Wait(); err != nil {
			return err
		}
	}

	if _, err := m.writeAndPointManifest(ctx, merged, pointerPath); err != nil {
		return err
	}
	return nil
}

// asOverlap maps the manifest-assembly errors that mean "this merge cannot be
// represented" to ErrOverlap, so the caller surfaces a terminal conflict and
// the client redrives. Any other error is wrapped with context under what.
func asOverlap(err error, what string) error {
	if errors.Is(err, errObjsNotContiguous) || errors.Is(err, errPathRoleConflict) {
		return fmt.Errorf("%w: %w", ErrOverlap, err)
	}
	return fmt.Errorf("mvcc: %s: %w", what, err)
}

// mergePacks applies ours' pack additions and removals on top of theirs, so a
// pack a concurrent repack retired stays retired; a set-union would hand it
// back (gitaly#7465). A repack leaves out objects already in a pack it keeps,
// so if each side retired a pack the other kept, every pack of both sides is
// kept instead. It returns the merged packs and whether each side's packs all
// survived (mergeMidx uses that to drop a midx whose packs are gone).
func mergePacks(base, ours, theirs *Manifest) (merged []string, oursPacksKept, theirsPacksKept bool) {
	basePacks, oursPacks, theirsPacks := base.PackPaths(), ours.PackPaths(), theirs.PackPaths()
	removedByOurs := diffSet(basePacks, oursPacks)
	addedByOurs := diffSet(oursPacks, basePacks)
	merged = append(diffSet(theirsPacks, removedByOurs), diffSet(addedByOurs, theirsPacks)...)

	oursPacksKept = len(diffSet(oursPacks, merged)) == 0
	theirsPacksKept = len(diffSet(theirsPacks, merged)) == 0
	if !oursPacksKept && !theirsPacksKept {
		// Each side retired a pack the other kept; keep both sides' packs.
		merged = append(diffSet(theirsPacks, oursPacks), oursPacks...)
		oursPacksKept, theirsPacksKept = true, true
	}
	return merged, oursPacksKept, theirsPacksKept
}

// mergeAux merges a single aux path that path reads ("" for none): today the
// commit-graph. It takes the side that changed it, else theirs. When both
// changed it, as when two housekeeping runs race and each publishes its own
// rolled-up graph, it keeps theirs if theirs has one and all of theirs' packs
// are kept, else ours on the same test. An identical regeneration (o == t)
// falls out of the both-changed case, kept.
//
// The commit-graph names commits, not packfiles, and the pack merge keeps every
// commit either side holds (under the reachability precondition), so the caller
// forces the packs-kept flags true and the graph is never dropped for a
// retirement. Callers guarantee each side has at most one such path, so path
// never reports more than one. The midx, which names packfiles and can be a
// chain, is merged as a set by mergeMidx instead.
func mergeAux(
	path func(*Manifest) (string, error),
	base, ours, theirs *Manifest,
	oursPacksKept, theirsPacksKept bool,
) (string, error) {
	b, err := path(base)
	if err != nil {
		return "", err
	}
	o, err := path(ours)
	if err != nil {
		return "", err
	}
	t, err := path(theirs)
	if err != nil {
		return "", err
	}

	switch {
	case o != b && t != b:
		// Both changed it. Keep whichever side is still covered by the
		// merged packs, preferring theirs.
		if t != "" && theirsPacksKept {
			return t, nil
		}
		if oursPacksKept {
			return o, nil
		}
		return "", nil
	case o != b && oursPacksKept:
		return o, nil
	case o == b && theirsPacksKept:
		return t, nil
	default:
		return "", nil
	}
}

// mergeMidx merges the midx/ paths as a set, so a single midx and an
// incremental midx chain (multiple midx/ files) are handled alike. A midx names
// packfiles, so it is valid only while every pack it could name survives the
// merge. The merge does not read midx contents, so it uses a sound
// approximation: a side's midx set is valid when all of that side's packs are
// kept, since a side's midx only indexes that side's packs. It takes the side
// that changed the set, else theirs (both unchanged keeps theirs' set), and
// keeps it only when that side's packs survived; otherwise it drops the midx
// and the store falls back to reading the packs. This mirrors mergeAux's rule,
// generalized to a set and always gated on packs.
func mergeMidx(base, ours, theirs *Manifest, oursPacksKept, theirsPacksKept bool) []string {
	b := prefixPaths(midxPrefix, base)
	o := prefixPaths(midxPrefix, ours)
	t := prefixPaths(midxPrefix, theirs)

	oursChanged := !equalPathSet(o, b)
	theirsChanged := !equalPathSet(t, b)

	switch {
	case oursChanged && theirsChanged:
		// Both changed it. Keep whichever side's packs all survived,
		// preferring theirs.
		if len(t) > 0 && theirsPacksKept {
			return t
		}
		if oursPacksKept {
			return o
		}
		return nil
	case oursChanged:
		// Only ours changed it.
		if oursPacksKept {
			return o
		}
		return nil
	default:
		// Ours left it as base; take theirs, changed or not.
		if theirsPacksKept {
			return t
		}
		return nil
	}
}

// equalPathSet reports whether a and b hold the same paths. Manifest path lists
// have no duplicates, so equal length with no left-only element means equal.
func equalPathSet(a, b []string) bool {
	return len(a) == len(b) && len(diffSet(a, b)) == 0
}

// prefixPaths returns m's OBJS paths that carry prefix.
func prefixPaths(prefix string, m *Manifest) []string {
	var out []string
	for _, p := range m.ObjPaths() {
		if strings.HasPrefix(p, prefix) {
			out = append(out, p)
		}
	}
	return out
}

// auxIsSingle reports whether base, ours, and theirs each carry at most one
// OBJS path with prefix, so mergeAux's single-path rule applies. Gitaly writes
// split commit-graphs, so a chain is several graph/ paths; when any side has
// more than one the prefix is carried leniently as unknown paths instead.
func auxIsSingle(prefix string, base, ours, theirs *Manifest) bool {
	return auxCount(prefix, base) <= 1 &&
		auxCount(prefix, ours) <= 1 &&
		auxCount(prefix, theirs) <= 1
}

// auxCount returns how many of m's OBJS paths carry prefix.
func auxCount(prefix string, m *Manifest) int {
	n := 0
	for _, p := range m.ObjPaths() {
		if strings.HasPrefix(p, prefix) {
			n++
		}
	}
	return n
}

// otherPaths returns m's paths the merge has no rule for -- a pack in OBJS and
// a reftable in REFS always have a rule, and so does a midx (mergeMidx owns it,
// single or chain); a commit-graph has one only when it is single on every side
// (graphSingle), since mergeAux handles just that case. Each returned path maps
// to whether it is in OBJS. The manifest format belongs to Git, so an unknown
// path is carried (lenient), not dropped, with its OBJS membership.
func otherPaths(m *Manifest, graphSingle bool) map[string]bool {
	known := make(map[string]struct{}, len(m.refStackOrder))
	for _, p := range m.RefPaths() {
		known[p] = struct{}{}
	}
	for _, p := range m.ObjPaths() {
		switch {
		case strings.HasPrefix(p, packPrefix):
			known[p] = struct{}{}
		case strings.HasPrefix(p, midxPrefix):
			known[p] = struct{}{}
		case graphSingle && strings.HasPrefix(p, graphPrefix):
			known[p] = struct{}{}
		}
	}
	inObjs := make(map[string]struct{}, m.objCount)
	for _, p := range m.ObjPaths() {
		inObjs[p] = struct{}{}
	}

	out := make(map[string]bool)
	for _, p := range m.Paths {
		if _, ok := known[p]; ok {
			continue
		}
		_, objs := inObjs[p]
		out[p] = objs
	}
	return out
}

// mergeOtherPaths merges the paths otherPaths covers by taking one side's
// set whole, membership included, the way a merge treats a file it cannot
// read: theirs when ours left them as they were in base, ours when theirs
// did or when both made the same change. Removals carry with the set.
// When both sides changed them differently there is no answer that does
// not depend on what the paths mean, so it returns ErrOverlap.
func mergeOtherPaths(base, ours, theirs *Manifest, graphSingle bool) (map[string]bool, error) {
	b := otherPaths(base, graphSingle)
	o := otherPaths(ours, graphSingle)
	t := otherPaths(theirs, graphSingle)
	switch {
	case maps.Equal(o, b):
		return t, nil
	case maps.Equal(t, b), maps.Equal(o, t):
		return o, nil
	}
	return nil, fmt.Errorf("%w: both sides changed paths outside packs and reftables", ErrOverlap)
}

// checkValueLevelOverlap reconstructs the ref state on each of base,
// ours, and theirs by reading every unique reftable across the three
// stacks once, and reports overlap when the two sides disagree on the
// target of a refname both of them determine in the merged manifest.
// Refs set identically on both sides do not conflict (a pair of
// clients that ran identical `git fetch` operations race their
// pointer-advance commits but do not actually conflict).
//
// oursExclusiveRefPaths is ours' reftable stack minus base's, i.e.
// exactly the tables the merge appends on top of theirs.
//
// Reftables are content-addressed by their filename hash, so a path
// appearing in more than one stack carries byte-identical contents.
// We exploit this by reading each unique path at most once and then
// folding the per-side stacks from the shared cache. In the common
// case of partial compaction — where only a few tables at the top of
// the stack are exclusive to one side — this avoids re-reading the
// long tail of shared older tables.
//
// Returns ErrOverlap on real value-level conflict, nil otherwise.
//
// When ReadReftableRefs is not wired (pack-only flows), reftable
// contents are unknown so the conservative outcome is to assume no
// overlap. This is safe because pack-only writes never touch refs.
func checkValueLevelOverlap(
	ctx context.Context,
	m *Merger,
	ours, base, theirs *Manifest,
	oursExclusiveRefPaths []string,
	opts MergeOpts,
) error {
	if opts.ReadReftableRefs == nil {
		return nil
	}

	tableRefs, err := m.readUniqueReftables(
		ctx,
		opts.ReadReftableRefs,
		base.RefPaths(), ours.RefPaths(), theirs.RefPaths(),
	)
	// Note: readUniqueReftables drops triply-shared paths from the
	// read set as a hot-path optimisation. foldStack tolerates the
	// missing entries (range over a nil map is a no-op), and since
	// those tables contribute identically to every fold, the checks
	// below still reach the same answer (see readUniqueReftables).
	if err != nil {
		return err
	}

	baseRefs := foldStack(base.RefPaths(), tableRefs)
	oursRefs := foldStack(ours.RefPaths(), tableRefs)
	theirsRefs := foldStack(theirs.RefPaths(), tableRefs)

	// Two kinds of conflict can emerge from a merge:
	//
	//  1. Value-level: ours and theirs end up disagreeing about the
	//     target of a refname. Refs set identically on both sides
	//     do not conflict (a pair of clients that ran identical
	//     `git fetch` operations race their pointer-advance commits
	//     but do not actually conflict).
	//
	//  2. Directory/file: ours and theirs introduce refnames N and
	//     M where M = N + "/..." . Git's ref namespace forbids this
	//     (one would have to be both a file and a directory in the
	//     loose-ref layout). Reftables physically permit such pairs,
	//     but Gitaly's per-transaction conflict.Manager rejects them
	//     at write time for parity with the files backend.
	//
	// The per-transaction check is run against base on each side
	// individually, so the only D/F violation that can emerge at
	// merge time is between an ours-created/updated name and a
	// theirs-created/updated name. All other dangerous combinations
	// (ours-create vs theirs-delete-then-create on a path base still
	// holds, etc.) would have been caught when one of the two
	// transactions was individually validated against base.
	oursDiff := refDiff(baseRefs, oursRefs)
	theirsDiff := refDiff(baseRefs, theirsRefs)

	// Value-level check.
	//
	// The ours side is membership-based, not diff-based, because the
	// merged stack is theirs verbatim with ours' exclusive tables
	// appended on top: those tables decide the merged value of every
	// refname they mention, whether or not ours changed it. Keying
	// off oursDiff instead lets a ref that an ours-exclusive table
	// carries forward unchanged shadow theirs' write without the
	// overlap ever being reported.
	//
	// oursDiff is unioned in for the converse shape, where ours
	// removed a refname from its stack entirely (a compaction that
	// absorbed the tombstone) so no exclusive table mentions it.
	oursShadowed := make(map[string]struct{}, len(oursDiff))
	for _, refPath := range oursExclusiveRefPaths {
		for name := range tableRefs[refPath] {
			oursShadowed[name] = struct{}{}
		}
	}
	for name := range oursDiff {
		oursShadowed[name] = struct{}{}
	}

	// theirsDiff is the side worth walking: it holds only what theirs
	// changed relative to base, whereas oursShadowed also contains
	// every refname ours merely carried forward.
	for name := range theirsDiff {
		if _, shadowed := oursShadowed[name]; !shadowed {
			continue
		}
		if !sameRefTarget(oursRefs[name], theirsRefs[name]) {
			return ErrOverlap
		}
	}

	// Directory/file check. This one is diff-based on both sides: a
	// refname both sides already held cannot introduce a new D/F
	// pair, since base would have had to hold the pair already.
	//
	// Deletions are excluded too, for the same reason — they only
	// remove names from the post-merge live set. Delete-vs-modify is
	// a value-level conflict and is caught above.
	//
	// Walk the smaller side once to populate the trie, then probe it
	// with the larger.
	smaller, larger := oursDiff, theirsDiff
	if len(theirsDiff) < len(oursDiff) {
		smaller, larger = theirsDiff, oursDiff
	}

	trie := newPathTrie()
	for name, target := range smaller {
		if isLiveTarget(target) {
			trie.insert(name)
		}
	}

	for name, target := range larger {
		if !isLiveTarget(target) {
			continue
		}
		if trie.conflicts(name) {
			return ErrOverlap
		}
	}

	// Merged-stack check. The checks above reason about the two diffs,
	// but the merge does not apply diffs: it appends ours' exclusive
	// tables to theirs' stack. When ours compacted tables theirs still
	// carries, that append cannot always represent ours' state. A
	// compaction from the bottom of the stack drops tombstones, so an
	// ours delete never reaches the merged stack, and a partial
	// compaction places an older value above newer tables theirs still
	// holds. Neither shows up in either diff, so fold the stack the
	// merge will produce and require it to equal theirs with ours'
	// changes applied on top.
	//
	// Rebasing only changes update indices, not ref values, so the
	// pre-rebase paths fold to the same refs. The merged stack starts
	// with theirs, so it skips the same triply-shared prefix as the
	// other folds.
	mergedRefs := foldStack(append(slices.Clone(theirs.RefPaths()), oursExclusiveRefPaths...), tableRefs)
	expectedRefs := maps.Clone(theirsRefs)
	maps.Copy(expectedRefs, oursDiff)

	for name, target := range mergedRefs {
		if !sameRefTarget(target, expectedRefs[name]) {
			return ErrOverlap
		}
	}
	for name, target := range expectedRefs {
		if !sameRefTarget(target, mergedRefs[name]) {
			return ErrOverlap
		}
	}
	return nil
}

// sameRefTarget reports whether two diff entries describe the same
// post-merge state for a refname. Byte equality is not sufficient
// because "the ref does not exist" has two encodings that can reach
// the comparison from opposite sides of the merge: refDiff's empty
// string, recorded when the ref is in base but absent from the folded
// stack, and the zero OID that foldStack preserves for a reftable
// tombstone.
//
// Both encodings mean the ref is gone, so a side that deleted a ref
// with a tombstone and a side that deleted the same ref and then
// compacted the tombstone away agree. Comparing the raw strings
// instead reports delete-vs-delete as a conflict, which forces a
// client redrive on exactly the case the merge exists to absorb: the
// same operation racing itself.
func sameRefTarget(a, b string) bool {
	if a == b {
		return true
	}
	return !isLiveTarget(a) && !isLiveTarget(b)
}

// isLiveTarget reports whether target represents an actually-present
// ref value (creation or update) rather than a deletion. Two encodings
// of "ref does not exist" can flow into a diff:
//
//   - "" — refDiff records a deletion as the empty-string sentinel
//     when the ref is present in base but absent in after.
//   - "0000...0" — foldStack preserves reftable deletion records as
//     zero-OID entries (40 zeros for SHA-1, 64 for SHA-256); refDiff
//     then carries that sentinel through if the value differs from
//     base. The check is "any non-'0' character" rather than a
//     length comparison so the same predicate works for both hash
//     algorithms without importing the git package.
//
// Real OIDs are hex strings with at least one non-'0' character
// (collisions on the all-zeroes hash are not produced by any real
// Git operation). Symbolic refs ("ref: refs/...") obviously contain
// non-'0' characters too.
func isLiveTarget(target string) bool {
	if target == "" {
		return false
	}
	for i := 0; i < len(target); i++ {
		if target[i] != '0' {
			return true
		}
	}
	return false
}

// readUniqueReftables reads the unique reftable paths needed to detect
// value-level overlap across base, ours, and theirs. Tables that appear
// in *all three* stacks are skipped entirely, since reading them would
// be wasted work.
//
// The triply-shared tables form a common prefix of every stack, and of
// the merged stack too, which starts with theirs. They therefore
// contribute identically to every fold checkValueLevelOverlap
// computes. Every table that can put a refname into oursShadowed or
// theirsDiff lies outside that prefix, so skipping the prefix changes
// none of the answers the overlap checks give.
//
// Skipping is *only* safe at the "triply shared" level. A doubly-
// shared table (in two of the three stacks) contributes asymmetrically
// to the folded maps and must be read.
//
// The triply-shared set is detected by a single positional walk:
// reftable stacks are append-only and compaction only replaces a
// contiguous range with one new table (never reorders), so once
// base[k], ours[k], theirs[k] disagree at some k, no later position
// can be triply shared either. The triply-shared set is exactly
// base[:prefixLen] where prefixLen is the first divergence.
//
// Tables are fetched and parsed in parallel up to artifactConcurrency
// goroutines.
func (m *Merger) readUniqueReftables(
	ctx context.Context,
	readRefs ReadReftableRefs,
	base, ours, theirs []string,
) (map[string]map[string]string, error) {
	// Identify the triply-shared prefix by positional walk.
	//
	// Reftable stacks are append-only and ordered oldest-first.
	// Compaction replaces a contiguous range of older tables with one
	// new content-addressed table; it never reorders. So if base[i],
	// ours[i], and theirs[i] are all the same path, that table sits
	// at the same stack position on every side and contributes
	// identically to every fold (a triply-shared table).
	//
	// Crucially, once any of the three sides diverges at position k
	// (different path, or one side has run out of entries), no later
	// position can be triply shared: the divergence at k is the
	// signature of a compaction or an append that shifts every
	// subsequent index on at least one side. So the triply-shared
	// set is exactly base[:prefixLen] for some prefixLen, found by a
	// single linear scan.
	minLen := min(len(theirs), min(len(ours), len(base)))

	prefixLen := 0
	for prefixLen < minLen &&
		base[prefixLen] == ours[prefixLen] &&
		base[prefixLen] == theirs[prefixLen] {
		prefixLen++
	}

	// Collect the paths we must read: every unique path in the
	// non-triply-shared suffix of each stack. Starting each sub-scan
	// from prefixLen skips the triply-shared prefix directly — no
	// explicit set lookup needed.
	//
	// Safety: a content-addressed reftable's hash depends on its
	// predecessor's max_update_index (through its own header), so the
	// same hash cannot appear at two different stack positions, nor
	// can it appear in the triply-shared prefix of one stack and the
	// suffix of another. So nothing in any [prefixLen:] suffix
	// duplicates anything in [:prefixLen].
	//
	// However, the same path CAN appear in two of the three suffixes:
	// e.g. with base=[B0,B1], ours=[B0,OC,T] (ours compacted B1 into
	// OC then appended T), theirs=[B0,B1,T] (theirs just appended T).
	// prefixLen=1, and T appears in both ours[1:] and theirs[1:].
	// This is legitimate because T's header min_update_index depends
	// only on its predecessor's max_update_index, which can match
	// across stacks even after a compaction earlier in the stack.
	// The seen map dedupes such cross-suffix duplicates so we read T
	// once instead of twice.
	//
	// Deterministic iteration order keeps the goroutine fan-out
	// reproducible across runs.
	seen := make(map[string]struct{})
	var uniq []string
	// prefixLen <= min(len(base), len(ours), len(theirs)), so the
	// stack[prefixLen:] slice is always valid (possibly empty).
	for _, stack := range [][]string{base, ours, theirs} {
		for _, p := range stack[prefixLen:] {
			if _, ok := seen[p]; ok {
				continue
			}
			seen[p] = struct{}{}
			uniq = append(uniq, p)
		}
	}
	if len(uniq) == 0 {
		return map[string]map[string]string{}, nil
	}

	results := make([]map[string]string, len(uniq))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(artifactConcurrency)
	for i, p := range uniq {
		g.Go(func() error {
			if err := m.fetchArtifact(gctx, p); err != nil {
				return fmt.Errorf("mvcc: fetch reftable %s: %w", p, err)
			}
			refs, err := readRefs(filepath.Join(m.cacheDir, p))
			if err != nil {
				return fmt.Errorf("mvcc: read reftable %s refs: %w", p, err)
			}
			results[i] = refs
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	out := make(map[string]map[string]string, len(uniq))
	for i, p := range uniq {
		out[p] = results[i]
	}
	return out, nil
}

// foldStack reconstructs the final ref state of a reftable stack by
// folding each table's contents from tableRefs in stack order. Later
// tables (higher in the stack) shadow earlier ones, matching how the
// reftable stack is resolved at read time by Git: a top-down search
// returns the first hit, which is equivalent to bottom-up folding
// because later entries overwrite earlier ones.
//
// Tombstones (deletion records) are preserved through the fold as
// entries whose target is the zero OID. This relies on the
// ReadReftableRefs callback emitting deletions as {name: zeroOID}
// rather than omitting them: a top-of-stack tombstone for ref r must
// shadow an earlier table's real value for r, and an omitted entry
// would let the earlier value leak through. reftable.ReadRefsAt
// satisfies this contract. The resulting map therefore conflates "r
// has been deleted" with "r currently points at the zero OID" -- the
// latter is unrepresentable in a real Git ref, so the conflation is
// harmless. refDiff treats both `""` (key absent on the side it's
// diffing) and the zero-OID string as distinguishable from any real
// target, so deletions still participate correctly in the overlap
// predicate (e.g. ours-deletes vs theirs-updates is flagged as a
// real conflict because the zero-OID string differs from theirs's
// non-zero target).
func foldStack(stackPaths []string, tableRefs map[string]map[string]string) map[string]string {
	folded := make(map[string]string)
	for _, p := range stackPaths {
		maps.Copy(folded, tableRefs[p])
	}
	return folded
}

// refDiff returns the subset of after that differs from before. A ref
// present in before but absent in after counts as a deletion and is
// recorded with the empty-string sentinel; a ref present in after but
// absent in before counts as a creation; a ref whose target changed
// counts as an update. Refs whose targets are identical (or which are
// absent on both sides) are omitted.
func refDiff(before, after map[string]string) map[string]string {
	out := make(map[string]string)
	for name, t := range after {
		if before[name] != t {
			out[name] = t
		}
	}
	for name := range before {
		if _, ok := after[name]; !ok {
			out[name] = ""
		}
	}
	return out
}

// reftableMaxIndex returns the highest MaxUpdateIndex in an ordered
// reftable stack. Ours is rebased above this value, so overstating it
// is harmless but understating it would interleave ours' update
// indices with theirs'.
//
// The maximum is taken over every range already in parsedInfo (the
// exclusive tables) and over the last table in the stack. It does NOT
// read shared tables in the middle of the stack, so it relies on the
// reftable invariant that update indices increase up the stack: a
// shared middle table's MaxUpdateIndex is then never higher than the
// last table's, which is read. If that invariant were ever violated a
// middle table could hold the true maximum and this would understate
// theirsMax. We accept that dependency rather than fetch and read every
// table on the merge path; a cheap pre-rebase guard that rejects a
// stack ours does not extend is tracked in gitaly#7512.
//
// Ranges the exclusive set already parsed are reused from parsedInfo.
// The last table is fetched into the local cache and read when it is
// not among them: the stack's last entry predates the exclusive set
// whenever that side appended no reftables, and nothing else in the
// merge has pulled it down.
//
// An empty stack has no update indices at all, so it reports 0: a
// manifest without a REFS chunk (a pack-only write against a refless
// base) leaves refStackOrder nil, and ours then rebases onto nothing.
func (m *Merger) reftableMaxIndex(
	ctx context.Context,
	allRefPaths []string,
	parsedRefPaths []string,
	parsedInfo []reftableInfo,
	readRange ReadReftableUpdateRange,
) (uint64, error) {
	if len(allRefPaths) == 0 {
		return 0, nil
	}

	lastPath := allRefPaths[len(allRefPaths)-1]

	var maxIdx uint64
	lastParsed := false
	for i, p := range parsedRefPaths {
		maxIdx = max(maxIdx, parsedInfo[i].maxIdx)
		if p == lastPath {
			lastParsed = true
		}
	}
	if lastParsed {
		return maxIdx, nil
	}

	// Not in the parsed set — fetch and read its range directly.
	if err := m.fetchArtifact(ctx, lastPath); err != nil {
		return 0, fmt.Errorf("mvcc: fetch last reftable %s: %w", lastPath, err)
	}
	_, lastMax, err := readRange(filepath.Join(m.cacheDir, lastPath))
	if err != nil {
		return 0, fmt.Errorf("mvcc: read last reftable %s range: %w", lastPath, err)
	}
	return max(maxIdx, lastMax), nil
}

// pathTrie is a prefix tree keyed on "/"-delimited refname components.
// Each node carries an "isRef" bit that is set when a complete refname
// terminates at that node. The trie is built from one merge side's
// live-ref creations/updates so the other side can probe for D/F
// (directory/file) conflicts in O(depth) per query.
//
// This is the same shape as Gitaly's existing refdb.History trie in
// internal/gitaly/storage/storagemgr/partition/conflict/refdb/tree.go,
// just scoped to the merge-time check rather than per-transaction
// staging.
type pathTrie struct {
	root *trieNode
}

type trieNode struct {
	isRef    bool
	children map[string]*trieNode
}

func newPathTrie() *pathTrie {
	return &pathTrie{root: &trieNode{}}
}

// insert records refname as a live ref in the trie. Components are
// split on "/", so the boundary check is anchored at "/" -- inserting
// "refs/heads/nest" does not introduce any common state with a later
// insert of "refs/heads/nested".
func (t *pathTrie) insert(refname string) {
	n := t.root
	for _, comp := range strings.Split(refname, "/") {
		child, ok := n.children[comp]
		if !ok {
			if n.children == nil {
				n.children = make(map[string]*trieNode)
			}
			child = &trieNode{}
			n.children[comp] = child
		}
		n = child
	}
	n.isRef = true
}

// conflicts reports whether inserting refname would collide with any
// refname already in the trie on a "/" boundary. Two collision shapes
// are detected:
//
//   - Walking down to refname's target node, any intermediate node
//     with isRef = true means a previously-inserted refname is a
//     strict prefix of refname (the "ancestor exists" case;
//     e.g. trie has refs/heads/nested, query is refs/heads/nested/a).
//
//   - At refname's target node, any children means a previously-
//     inserted refname extends refname (the "descendant exists" case;
//     e.g. trie has refs/heads/nested/a, query is refs/heads/nested).
//
// The exact-match case (trie has exactly refname, query is refname)
// is intentionally not flagged here. By the time the D/F check in
// checkValueLevelOverlap runs, the value-level check has already
// passed, which means both sides staged the same target for refname
// -- an idempotent write, not a conflict. Two clients running an
// identical "git fetch" land here and must merge cleanly.
func (t *pathTrie) conflicts(refname string) bool {
	n := t.root
	comps := strings.Split(refname, "/")
	for i, comp := range comps {
		child, ok := n.children[comp]
		if !ok {
			return false
		}
		// Walking down: an ancestor-as-ref on the path to refname
		// is a D/F violation (intermediate components only; the
		// final component is handled by the post-loop check).
		if i < len(comps)-1 && child.isRef {
			return true
		}
		n = child
	}
	// At the target node: a descendant-as-ref is a D/F violation.
	// Exact match (n.isRef && no children) is not flagged; see the
	// doc comment for why.
	return len(n.children) > 0
}
