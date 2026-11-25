package mvcc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
)

// ErrManifestPointerConflict is returned by an ObjectStorage's Commit when the
// manifest pointer has diverged from the base hash that was resolved at the
// start of the RPC, i.e. another writer advanced the pointer concurrently.
// Merger.ThreeWayMerge can resolve such a conflict but is not wired into Commit
// yet, so the RPC fails; clients may retry against the new base.
var ErrManifestPointerConflict = errors.New("mvcc: manifest pointer conflict")

// NotInitializedError is returned by an ObjectStorage's ReadManifestPointer when
// the backend holds no MVCC state for the repository at all, i.e. no manifest was
// ever committed for it. Consumers that probe durable storage for a repository's
// existence — node hydration, packfile-URI offload — match it with errors.As to
// tell "not in this backend" apart from a read failure. It also satisfies
// errors.Is(err, fs.ErrNotExist), the contract ReadManifestPointer documented
// before this type existed, so callers that only check for a missing pointer
// keep working.
type NotInitializedError struct {
	// RepositoryID identifies the repository that has no committed manifest.
	RepositoryID string
}

// Error describes which repository has no MVCC state.
func (e *NotInitializedError) Error() string {
	return fmt.Sprintf("mvcc: repository %q has no committed manifest", e.RepositoryID)
}

// Is reports fs.ErrNotExist as a match, preserving the missing-pointer contract
// for callers that check the sentinel rather than this type.
func (e *NotInitializedError) Is(target error) bool { return target == fs.ErrNotExist }

// ManifestPointer identifies the base state a mutator RPC builds on: the manifest
// hash resolved at the start of the RPC and, when the backend tracks it, the
// generation that hash occupies in the log. Carrying the generation lets Commit
// target the next slot without re-reading the head. HasGeneration is false when
// only a hash is known (a read pin, or a backend without generations such as
// LocalStorage), which makes Commit resolve the head itself.
type ManifestPointer struct {
	// Hash is the content-addressed manifest hash. It is empty when the
	// repository has no committed manifest yet (a genesis base).
	Hash string
	// Generation is the position of Hash in the append-only generation log.
	// Only meaningful when HasGeneration is true.
	Generation uint64
	// HasGeneration reports whether Generation is known and can be used to skip
	// the head read at commit time.
	HasGeneration bool
}

// ObjectStorage is the interface used for a component to communicate with object storage.
type ObjectStorage interface {
	// ReadManifestPointer reads the manifest pointer for the given repository ID.
	// Backends that track generations populate ManifestPointer.Generation so a
	// later Commit can advance without re-reading the head.
	ReadManifestPointer(ctx context.Context, repositoryID string) (ManifestPointer, error)

	// ReadManifest reads a specific manifest for the repository, keyed on the hash.
	ReadManifest(ctx context.Context, repositoryID string, hash string) (io.ReadCloser, error)

	// ReadArtifact ensures the artifact identified by `key` is fetched for the given repository.
	// The consume callback is invoked with an io.ReadCloser that must be used to read the artifact.
	//
	// Implementations must satisfy two properties beyond fetching the bytes, because
	// callers depend on them rather than coordinating fetches themselves:
	//
	//   - Concurrent calls for the same repositoryID and key are coalesced onto a
	//     single fetch, and consume is invoked at most once for it. Merger.fetchArtifact
	//     fans out across a manifest's reftables, and LocalCache.prepareArtifacts warms
	//     the same set, so without coalescing the two would download and write the same
	//     artifact at once.
	//
	//   - The fetch survives cancellation of the caller that started it, so that the
	//     remaining callers still observe its result. A caller that gives up gets its
	//     own ctx error back; it does not take the fetch down with it. This matters for
	//     large artifacts whose download outlasts a single request deadline, and for the
	//     merge's retries, which rely on a previous attempt's fetch having completed.
	//
	// A straight read of the backing store that honours ctx would satisfy the first
	// sentence of this comment and silently drop both properties.
	ReadArtifact(ctx context.Context, repositoryID, key string, consume func(ctx context.Context, r io.ReadCloser) error) error

	// PutArtifact writes `content` at `key` for the given repository.
	PutArtifact(ctx context.Context, repositoryID, key string, content io.ReadCloser) error

	// Commit commits the new manifest pointer for the given repository. This operation
	// must be atomic, by comparing base (the state resolved at the start of the RPC)
	// with the newHash. If a conflict occurs, some implementations can introduce a conflict
	// resolution mechanism (ex: three-way merge).
	Commit(ctx context.Context, repositoryID string, base ManifestPointer, newHash string) error

	// TearDown releases all resources opened by the Storage. It is good practice to
	// run when the Storage is no longer needed.
	TearDown(ctx context.Context) error
}

// ReadHeadManifest resolves the current head of the repository's manifest log and
// parses the manifest it names. It is the read entry point for consumers that
// serve straight from durable storage without a local cache, such as packfile-URI
// offload and node hydration. The returned pointer carries the generation when
// the backend tracks one, so a caller can detect later that the head moved. A
// repository with no committed manifest surfaces as a *NotInitializedError.
func ReadHeadManifest(ctx context.Context, storage ObjectStorage, repositoryID string) (ManifestPointer, *Manifest, error) {
	pointer, err := storage.ReadManifestPointer(ctx, repositoryID)
	if err != nil {
		return ManifestPointer{}, nil, fmt.Errorf("read manifest pointer: %w", err)
	}

	reader, err := storage.ReadManifest(ctx, repositoryID, pointer.Hash)
	if err != nil {
		return ManifestPointer{}, nil, fmt.Errorf("read manifest %q: %w", pointer.Hash, err)
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		return ManifestPointer{}, nil, fmt.Errorf("read manifest %q: %w", pointer.Hash, err)
	}

	manifest, err := ParseManifest(data)
	if err != nil {
		return ManifestPointer{}, nil, fmt.Errorf("parse manifest %q: %w", pointer.Hash, err)
	}
	return pointer, manifest, nil
}
