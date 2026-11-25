package mvcc

// ReadReftableRefs reads a single reftable file and returns its
// references as a name->target map. Direct references store their
// object id in the target; symbolic references store the target ref
// name. Tombstones are encoded as map entries whose target is the zero
// OID; ThreeWayMerge includes tombstones in the overlap check so a
// same-name delete-vs-write race is detected.
//
// Injected into ThreeWayMerge via MergeOpts to avoid an import cycle
// with internal/git/reftable (which transitively pulls in gittest,
// which itself depends on this package). reftable.ReadRefsAt
// satisfies this; tests can supply lighter-weight fakes.
type ReadReftableRefs func(path string) (map[string]string, error)

// ReadReftableUpdateRange reads the inclusive [min, max] update-index
// range stored in the header / footer of a reftable. Injected into the
// rebase so the mvcc package can sequence staged reftables
// onto a theirs stack without importing the reftable package directly
// (which would close a test-only import cycle through gittest).
//
// reftable.ReadUpdateRangeAt satisfies this; tests can supply
// lighter-weight fakes.
type ReadReftableUpdateRange func(path string) (min, max uint64, err error)

// PatchReftableUpdateRange opens the reftable at path and rewrites its
// header + footer to claim the supplied [min, max] update-index range.
// The file is patched in place: callers operating on content-addressed
// storage must copy the file to a temporary destination before invoking
// this callback so the original bytes (and hence the original content
// address) are preserved.
//
// reftable.PatchUpdateRangeAt satisfies this; tests can supply
// lighter-weight fakes.
type PatchReftableUpdateRange func(path string, min, max uint64) error
