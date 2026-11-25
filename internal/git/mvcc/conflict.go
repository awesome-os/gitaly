package mvcc

import "fmt"

// ErrOverlap is returned when a three-way manifest merge cannot reconcile ours
// and theirs: their changes overlap, both changed an artifact the merge has no
// rule for, or the manifest carries a chunk the merge cannot carry. The caller
// surfaces it as a terminal conflict (Aborted with kind=overlap) so the client
// redrives the RPC.
//
// The three-way merge that returns this lives in a follow-up (!9348); the
// sentinel lives here with the rest of the conflict vocabulary so the
// classification and the middleware mapping can reference it independently.
var ErrOverlap = fmt.Errorf("mvcc: ours and theirs changes overlap")

// ConflictKind names the reason a write was rejected after losing the race for
// the manifest pointer. The MVCC middleware attaches it as the
// "mvcc_conflict_kind" gRPC error metadata key so the two failure modes can be
// told apart in logs without parsing an error message.
//
// This is classification only. The errors themselves stay the package's
// existing sentinels — ErrManifestPointerConflict for a failed
// compare-and-swap, ErrOverlap for a three-way merge that could not reconcile
// the two sides — so a conflict has one vocabulary throughout rather than one
// per layer.
type ConflictKind string

// ConflictKindMetadataKey is the error metadata key that carries a ConflictKind.
const ConflictKindMetadataKey = "mvcc_conflict_kind"

const (
	// ConflictKindCASExhausted indicates the manifest pointer raced against
	// this write and either no retry path was available or the retry budget was
	// exhausted. It accompanies ErrManifestPointerConflict. The client should
	// redrive the RPC.
	ConflictKindCASExhausted ConflictKind = "cas_exhausted"

	// ConflictKindOverlap indicates that the ours and theirs ref sets
	// (relative to the shared base) are not disjoint, so a manifest
	// rebase would silently invalidate the staged work. It accompanies
	// ErrOverlap. The client must redrive the RPC so Git can
	// re-evaluate against the new ref state.
	ConflictKindOverlap ConflictKind = "overlap"
)

// ConflictError is returned by an ObjectStorage's Commit when the manifest
// pointer has advanced beyond the base the new write was built on. Unlike the
// bare ErrManifestPointerConflict sentinel, it names the concurrent winner so a
// retry loop can rebase onto it (read the "theirs" manifest, 3-way merge, and
// re-CAS) without re-listing the log.
//
// It unwraps to ErrManifestPointerConflict so existing callers using
// errors.Is(err, ErrManifestPointerConflict) keep working unchanged.
type ConflictError struct {
	// Base is the manifest pointer the losing write was built on. Its
	// generation is known only for a backend that tracks generations (the EE
	// object-storage backend); LocalStorage leaves HasGeneration false.
	Base ManifestPointer
	// Winner is the concurrent write that won the race, read back from the head,
	// so a retry loop can rebase onto it without re-listing the log.
	//
	// Winner can equal Base. On object stores where a conditional PUT can
	// conflict while another PUT to the same slot is still in flight (S3 returns
	// 409 ConditionalRequestConflict), the head read-back may still see Base
	// because the winning write has not landed yet. A retry loop must treat
	// Winner == Base as a transient conflict and re-read the head rather than
	// 3-way merge ours against itself.
	Winner ManifestPointer
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("mvcc: manifest pointer conflict: expected %s, got %s",
		describePointer(e.Base), describePointer(e.Winner))
}

// describePointer renders a ManifestPointer for an error message. It includes
// the generation only when it is known, so a hash-only (LocalStorage) pointer
// does not misleadingly claim "generation 0".
func describePointer(p ManifestPointer) string {
	switch {
	case p.Hash == "":
		return "(none)"
	case p.HasGeneration:
		return fmt.Sprintf("%s (generation %d)", p.Hash, p.Generation)
	default:
		return p.Hash
	}
}

// Unwrap lets errors.Is(err, ErrManifestPointerConflict) match a *ConflictError,
// preserving the behavior of callers written against the bare sentinel.
func (e *ConflictError) Unwrap() error {
	return ErrManifestPointerConflict
}
