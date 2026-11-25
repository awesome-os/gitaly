package mvcc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage/mode"
)

// artifactConcurrency caps the number of in-flight artifact transfers
// a single merge performs. Bounded to avoid spawning hundreds of
// goroutines and connections for repositories with many reftables while
// still meaningfully overlapping latency.
const artifactConcurrency = 8

// Merger resolves a manifest pointer conflict by three-way merging the
// manifest that lost the compare-and-swap with the one that won it. See
// ThreeWayMerge for the algorithm.
//
// It is deliberately decoupled from any particular ObjectStorage
// implementation: the merge only needs to fetch artifacts into the local
// cache, store rebased reftables back, and write the merged manifest body,
// all of which the ObjectStorage interface already expresses.
type Merger struct {
	// storage is durable storage the merge fetches conflicting artifacts
	// from and writes rebased reftables back to.
	storage ObjectStorage

	// repositoryID identifies the repository in storage.
	repositoryID string

	// cacheDir is the absolute path of the repository's local MVCC cache,
	// i.e. where manifest paths resolve to files on disk.
	cacheDir string
}

// NewMerger returns a Merger for the repository identified by repositoryID,
// whose local MVCC cache lives at cacheDir.
func NewMerger(storage ObjectStorage, repositoryID, cacheDir string) *Merger {
	return &Merger{
		storage:      storage,
		repositoryID: repositoryID,
		cacheDir:     cacheDir,
	}
}

// fetchArtifact downloads the artifact at relPath from durable storage into
// the local cache if it is not already present. Artifacts are immutable, so
// any local file at the same path is trusted.
//
// The exists check races other fetches of the same artifact, including the
// detached ones ReadArtifact keeps running past their caller's cancellation.
// What makes that safe is createFileAtomic's rename, not immutability:
// immutable bytes written straight to dst would still let a reader observe a
// partial file. Anything replacing createFileAtomic here has to keep the
// rename.
func (m *Merger) fetchArtifact(ctx context.Context, relPath string) error {
	dst := filepath.Join(m.cacheDir, relPath)
	exists, err := fileExist(dst)
	if err != nil {
		return fmt.Errorf("stat %s: %w", dst, err)
	}
	if exists {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(dst), mode.Directory); err != nil {
		return fmt.Errorf("mkdir for %s: %w", dst, err)
	}

	return m.storage.ReadArtifact(ctx, m.repositoryID, relPath, func(ctx context.Context, r io.ReadCloser) error {
		return createFileAtomic(ctx, dst, r)
	})
}

// uploadFromCache writes the locally cached artifact at relPath back to
// durable storage.
func (m *Merger) uploadFromCache(ctx context.Context, relPath string) error {
	f, err := os.Open(filepath.Join(m.cacheDir, relPath))
	if err != nil {
		return fmt.Errorf("open cached %s: %w", relPath, err)
	}
	defer func() { _ = f.Close() }()

	if err := m.storage.PutArtifact(ctx, m.repositoryID, relPath, f); err != nil {
		return fmt.Errorf("put %s: %w", relPath, err)
	}
	return nil
}

// writeAndPointManifest serialises manifest, writes the body into the
// local cache at its fanout path under <cacheDir>/manifests/ (atomically),
// and rewrites pointerPath to point at <newSha>. The returned sha is the
// SHA-256 of the serialised body, computed by SerializeManifest and
// embedded in the trailer.
//
// Used by ThreeWayMerge to install the merged manifest body and
// update the per-RPC pointer file in a single primitive.
func (m *Merger) writeAndPointManifest(ctx context.Context, manifest *Manifest, pointerPath string) (string, error) {
	body, newSha, err := SerializeManifest(manifest)
	if err != nil {
		return "", fmt.Errorf("serialize merged manifest: %w", err)
	}

	// The manifest is sharded into a fanout directory by hash, matching the rest of the
	// MVCC cache; createFileAtomic creates the fanout directory before writing.
	manifestRelPath, err := ManifestRelPath(newSha)
	if err != nil {
		return "", fmt.Errorf("manifest path: %w", err)
	}
	if err := createFileAtomic(ctx, filepath.Join(m.cacheDir, manifestRelPath), bytes.NewReader(body)); err != nil {
		return "", fmt.Errorf("write manifest body: %w", err)
	}

	// createFileAtomic stages its tempfile next to the target, so the pointer's
	// directory has to exist.
	if err := os.MkdirAll(filepath.Dir(pointerPath), mode.Directory); err != nil {
		return "", fmt.Errorf("mkdir pointer dir: %w", err)
	}
	if err := createFileAtomic(ctx, pointerPath, strings.NewReader(newSha)); err != nil {
		return "", fmt.Errorf("update pointer: %w", err)
	}
	return newSha, nil
}

// patchReftable copies <cacheDir>/<srcRefPath> to a temporary file in the
// same directory, applies the supplied update-index range to the copy,
// content-addresses the result, and renames the temp file to the resulting
// reftable sharded into the fanout directory named by its own hash. Returns the
// new content-addressed relative path (e.g. "refs/<newHash[:2]>/<newHash[2:]>.ref").
//
// The patched table is written to the local cache only; it is NOT uploaded to
// durable storage. The caller uploads it (see uploadFromCache) after it has
// validated the merged manifest, so a merge the format cannot represent never
// leaves an orphaned artifact in durable storage.
//
// srcRefPath is a full relative path (e.g. "refs/ab/cdef.ref") drawn from a
// manifest's REFS chunk. Only its kind directory (refs) is taken from it; the
// rebased table is sharded into the fanout directory named by its own content
// hash, like every other MVCC artifact, not under the source table's prefix.
func (m *Merger) patchReftable(
	srcRefPath string,
	newMin, newMax uint64,
	patchRange PatchReftableUpdateRange,
) (string, error) {
	dir, _ := filepath.Split(srcRefPath)
	srcAbs := filepath.Join(m.cacheDir, srcRefPath)
	dirAbs := filepath.Join(m.cacheDir, dir)
	if err := os.MkdirAll(dirAbs, mode.Directory); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", dirAbs, err)
	}

	tmp, err := os.CreateTemp(dirAbs, "rebase-*.ref.tmp")
	if err != nil {
		return "", fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()
	tmpClosed := false
	// tmpRenamed guards the cleanup below: it is set once the temp
	// file has been renamed into place, which is the only path on
	// which tmpName no longer names a file of ours to remove. Note
	// that it says nothing about the upload — a rename that is never
	// followed by a successful PutArtifact leaves the cache holding
	// an artifact durable storage does not, which is fine because the
	// error aborts the merge and the next attempt re-uploads from the
	// cache.
	tmpRenamed := false
	defer func() {
		if !tmpClosed {
			_ = tmp.Close()
		}
		if !tmpRenamed {
			_ = os.Remove(tmpName)
		}
	}()

	src, err := os.Open(srcAbs)
	if err != nil {
		return "", fmt.Errorf("open src %s: %w", srcAbs, err)
	}
	defer func() { _ = src.Close() }()

	if _, err := io.Copy(tmp, src); err != nil {
		return "", fmt.Errorf("copy: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close temp: %w", err)
	}
	tmpClosed = true

	if err := patchRange(tmpName, newMin, newMax); err != nil {
		return "", fmt.Errorf("patch: %w", err)
	}

	// Content-address the patched bytes. Use SHA-256 over the file
	// contents so the new filename matches the rest of the MVCC
	// content-addressing scheme.
	patched, err := os.Open(tmpName)
	if err != nil {
		return "", fmt.Errorf("hash patched: open: %w", err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, patched); err != nil {
		_ = patched.Close()
		return "", fmt.Errorf("hash patched: copy: %w", err)
	}
	if err := patched.Close(); err != nil {
		return "", fmt.Errorf("hash patched: close: %w", err)
	}
	newHash := fmt.Sprintf("%x", h.Sum(nil))

	// The rebased table is content-addressed by its own hash, so it lands in the fanout
	// directory named by that hash rather than inheriting the source table's prefix.
	newRelPath, err := fanoutRelPath(refsDirectory, newHash+".ref")
	if err != nil {
		return "", fmt.Errorf("reftable path: %w", err)
	}
	finalAbs := filepath.Join(m.cacheDir, newRelPath)
	if err := os.MkdirAll(filepath.Dir(finalAbs), mode.Directory); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", filepath.Dir(finalAbs), err)
	}

	// If the final path already exists, the rebase landed on the
	// same bytes a previous run produced (idempotent re-CAS). Discard
	// the temp; the existing file is good as-is.
	exists, err := fileExist(finalAbs)
	if err != nil {
		return "", fmt.Errorf("stat %s: %w", finalAbs, err)
	}
	if exists {
		// The rebase landed on bytes a previous run already produced.
		// The existing cached file is good as-is; tmpRenamed stays false
		// so the deferred cleanup removes our temp. The caller uploads
		// newRelPath regardless, which also covers a previous attempt
		// that crashed between rename and upload.
		return newRelPath, nil
	}

	if err := os.Rename(tmpName, finalAbs); err != nil {
		return "", fmt.Errorf("rename to %s: %w", finalAbs, err)
	}
	tmpRenamed = true

	return newRelPath, nil
}

// diffSet returns the elements of have that are not in exclude, preserving
// have's order.
func diffSet(have, exclude []string) []string {
	skip := make(map[string]struct{}, len(exclude))
	for _, s := range exclude {
		skip[s] = struct{}{}
	}
	var out []string
	for _, s := range have {
		if _, ok := skip[s]; !ok {
			out = append(out, s)
		}
	}
	return out
}
