// Package objectstorage provides a cloud-object-storage implementation of
// mvcc.ObjectStorage for the MVCC backend. It is licensed under the GitLab
// Enterprise Edition license (see ee/LICENSE), not the MIT license that covers
// the rest of the repository.
//
// The implementation talks to object storage through the generic
// gocloud.dev/blob interface, opened from a URL (s3://, gs://, azblob://, or
// mem:// for tests). The one provider-specific quirk it handles: during a
// create-if-absent conflict, S3 may emit an error that gocloud.dev/blob classes
// as "unknown", so we use the smithy error interface to catch it in
// isCreateConflict.
//
// # Manifest generations
//
// Rather than overwriting a single manifest-pointer object on every commit
// (which caps throughput at object storage's per-object update limit, e.g.
// ~1 write/second on GCS), the canonical pointer is an append-only log of
// immutable "generation" objects:
//
//	<repo-prefix>/generations/<inverted-generation>          # content: the manifest hash
//	<repo-prefix>/generations/<inverted-generation>.<hash>   # empty; hash is in the key
//	<repo-prefix>/manifests/<hash[:2]>/<hash[2:]>             # content: the manifest body
//	<repo-prefix>/<artifact key>                              # content: a pack/reftable/...
//
// Manifest bodies and artifacts are sharded into a two-character fanout directory
// by hash, the same layout the MVCC backend uses on disk, so a hash resolves to the
// same relative key whether it is read from local or object storage.
//
// generation counts up from 0; the object key stores MaxUint64-generation as a
// fixed-width 16-digit hex string, so the newest generation is the
// lexicographically first key, the only ordering object stores can list
// efficiently. Generation 0 is "generations/ffffffffffffffff".
//
// A commit is made by writing the next generation's file using
// WriterOptions.IfNotExist. This gives a race-free compare-and-swap operation
// that is able to eliminate conflicts from concurrent writers.
//
// The commit then writes a "<generation>.<hash>" sibling key that carries the
// manifest hash in its name. It sorts right after the generation object, so head
// can resolve the current generation and its hash from one two-result List,
// skipping the body read when the sibling is already visible (see head).
package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/smithy-go"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	// Register the object-storage drivers gocloud opens by URL scheme. These are
	// blank imports so no provider SDK type is referenced directly.
	_ "gocloud.dev/blob/azureblob" // azblob://
	_ "gocloud.dev/blob/gcsblob"   // gs://
	_ "gocloud.dev/blob/memblob"   // mem:// (tests)
	_ "gocloud.dev/blob/s3blob"    // s3://
)

const (
	// generationKeyDir is the key prefix, relative to a repository prefix, under
	// which the generation log is stored.
	generationKeyDir = "generations"
)

// defaultReadOptions lets Download fetch a large artifact as concurrent ranged
// reads when it writes into an io.WriterAt. An artifact smaller than one part
// still takes a single request. The sizes match what poc/scaling-git uses with
// the AWS SDK downloader.
var defaultReadOptions = blob.ReaderOptions{
	AllowParallelDownload: true,
	BufferSize:            32 << 20,
	MaxConcurrency:        8,
}

// artifactKey identifies a single artifact fetch for the runner cache.
type artifactKey struct {
	prefix string
	key    string
}

// Storage is a gocloud.dev/blob-backed implementation of mvcc.ObjectStorage.
type Storage struct {
	bucket *blob.Bucket
	// resolve maps a repository ID to the opaque key prefix its objects live
	// under. It is the single indirection point between a repository and its
	// storage location; see PrefixResolver.
	resolve PrefixResolver
	// runnerCache coalesces concurrent fetches of the same artifact and keeps a
	// download running past the canceling caller, matching LocalStorage.
	runnerCache *mvcc.KeyedRunnerCache[artifactKey]
	// readOptions are the options artifacts are read with; see
	// defaultReadOptions.
	readOptions blob.ReaderOptions
	// ownsBucket records whether this Storage opened the bucket itself (via
	// OpenStorage) and must therefore close it in TearDown. A bucket passed to
	// NewStorage is owned by the caller and left open.
	ownsBucket bool
	// closeOnce guards the owned bucket's Close so TearDown is safe to call more
	// than once, which happens when one Storage backs several storage paths.
	closeOnce sync.Once
}

var _ mvcc.ObjectStorage = (*Storage)(nil)

// PrefixResolver maps a repository ID to the opaque key prefix its objects live
// under. Callers of a Storage must treat the returned prefix as opaque: nothing
// in this package assumes it mirrors the on-disk hashed path. It must return a
// stable prefix per repository, and an error for an unusable repository ID.
//
// Injecting the resolver keeps the storage layout independent of the repository
// naming scheme. Object-pool deduplication, where forks share one artifact
// prefix while keeping distinct manifest-pointer prefixes, will supply a
// mapping-backed resolver in place of HashedPathPrefix without changing this
// package. See https://gitlab.com/gitlab-org/gitaly/-/work_items/7250.
type PrefixResolver func(repositoryID string) (string, error)

// HashedPathPrefix is the v1 resolver: it strips Gitaly's leading "@" marker so
// the bucket layout mirrors the on-disk hashed path (e.g. "@hashed/aa/bb/repo.git"
// becomes "hashed/aa/bb/repo.git"). Dropping the "@" also avoids the SigV4 quirks
// it exposes on some S3-compatible servers. An empty result is rejected so a bad
// repository ID cannot collapse the prefix.
func HashedPathPrefix(repositoryID string) (string, error) {
	prefix := strings.TrimPrefix(repositoryID, "@")
	if prefix == "" {
		return "", fmt.Errorf("mvcc: empty repository id")
	}
	return prefix, nil
}

// NewStorage returns a Storage backed by an already-open bucket. The caller opens
// the bucket (for example with blob.OpenBucket for an s3://, gs://, or azblob://
// URL) and owns its lifecycle; TearDown does not close it. resolve maps
// repository IDs to their opaque storage prefixes; pass HashedPathPrefix for the
// default hashed-path layout.
func NewStorage(bucket *blob.Bucket, resolve PrefixResolver) *Storage {
	return &Storage{
		bucket:      bucket,
		resolve:     resolve,
		runnerCache: mvcc.NewKeyedRunnerCache[artifactKey](),
		readOptions: defaultReadOptions,
	}
}

// OpenStorage opens the bucket at url and returns a Storage that owns it: the
// returned Storage's TearDown closes the bucket. url is a gocloud blob URL
// (s3://, gs://, or azblob://); credentials come from the provider's default
// chain. resolve maps repository IDs to their opaque storage prefixes; pass
// HashedPathPrefix for the default hashed-path layout. Use NewStorage instead
// when the caller already holds the bucket and owns its lifecycle.
func OpenStorage(ctx context.Context, url string, resolve PrefixResolver) (*Storage, error) {
	bucket, err := blob.OpenBucket(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("open bucket: %w", err)
	}

	s := NewStorage(bucket, resolve)
	s.ownsBucket = true
	return s, nil
}

// TearDown drains the fetch runner cache and, when this Storage opened the
// bucket itself (via OpenStorage), closes it. A bucket passed to NewStorage is
// left open for its caller to close.
func (s *Storage) TearDown(_ context.Context) error {
	s.runnerCache.Drain()

	if !s.ownsBucket {
		return nil
	}

	var err error
	s.closeOnce.Do(func() { err = s.bucket.Close() })
	return err
}

// validateKey rejects object keys that could escape the repository's prefix once
// joined into the shared bucket. Keys and manifest hashes may come from manifests
// written on disk, so a ".." segment or a leading "/" must not reach path.Join,
// which cleans "..".
func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("mvcc: empty object key")
	}
	if strings.HasPrefix(key, "/") {
		return fmt.Errorf("mvcc: invalid object key %q: leading slash", key)
	}
	if slices.Contains(strings.Split(key, "/"), "..") {
		return fmt.Errorf("mvcc: invalid object key %q: contains %q", key, "..")
	}
	return nil
}

func artifactObjectKey(prefix, key string) string {
	return path.Join(prefix, key)
}

// formatGenerationKey returns the generation's key name relative to the
// generation directory. The number is inverted (MaxUint64-gen) and zero-padded
// to 16 hex digits so the newest generation sorts lexicographically first.
func formatGenerationKey(generation uint64) string {
	return fmt.Sprintf("%016x", math.MaxUint64-generation)
}

// parseGenerationKey recovers the generation number from a full object key.
func parseGenerationKey(key string) (uint64, error) {
	hexPart := path.Base(key)
	if len(hexPart) != 16 {
		return 0, fmt.Errorf("invalid generation key %q", key)
	}
	inverted, err := strconv.ParseUint(hexPart, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("parse generation %q: %w", key, err)
	}
	return math.MaxUint64 - inverted, nil
}

// nextGeneration returns generation+1, erroring if the log is exhausted.
func nextGeneration(generation uint64) (uint64, error) {
	if generation == math.MaxUint64 {
		return 0, fmt.Errorf("mvcc: manifest generations exhausted")
	}
	return generation + 1, nil
}

func generationObjectKey(prefix string, generation uint64) string {
	return path.Join(prefix, generationKeyDir, formatGenerationKey(generation))
}

// generationHashKey returns the sibling "<generation>.<hash>" key that embeds the
// manifest hash in its name. It sorts immediately after the bare generation
// object (a "." extends the shared prefix), so a List(limit=2) that starts at the
// current generation returns this key as its second entry once written. See head
// for how the hash is recovered from it.
func generationHashKey(prefix string, generation uint64, hash string) string {
	return generationObjectKey(prefix, generation) + "." + hash
}

// generationListPrefix is the listing prefix for a repository's generation log.
// It is built from the same pieces as generationObjectKey via path.Join so the
// two cannot drift, with a trailing slash so the list matches only entries in
// the directory.
func generationListPrefix(prefix string) string {
	return path.Join(prefix, generationKeyDir) + "/"
}

// ReadManifestPointer reads the latest committed manifest as a ManifestPointer,
// including its generation so a later Commit can advance without re-reading the
// head. It returns a *mvcc.NotInitializedError (which satisfies
// errors.Is(err, fs.ErrNotExist)) when the repository has no manifest yet.
func (s *Storage) ReadManifestPointer(ctx context.Context, repositoryID string) (mvcc.ManifestPointer, error) {
	prefix, err := s.resolve(repositoryID)
	if err != nil {
		return mvcc.ManifestPointer{}, err
	}
	generation, hash, err := s.head(ctx, prefix)
	if err != nil {
		// head reports fs.ErrNotExist only for an empty generation log, i.e. the
		// repository was never committed to this bucket.
		if errors.Is(err, fs.ErrNotExist) {
			return mvcc.ManifestPointer{}, &mvcc.NotInitializedError{RepositoryID: repositoryID}
		}
		return mvcc.ManifestPointer{}, err
	}
	return mvcc.ManifestPointer{Hash: hash, Generation: generation, HasGeneration: true}, nil
}

// ArtifactObjectKey returns the bucket key at which the repository stores the
// artifact identified by key, e.g. a "pack/<name>.pack" path taken from a
// manifest. It exists for read-side consumers that must address the bucket
// directly rather than through ReadArtifact — signing download URLs for
// packfile-URI offload is the motivating case. The prefix is resolved through
// the same PrefixResolver as every other operation, so the returned key names
// the same object the write path stored.
func (s *Storage) ArtifactObjectKey(repositoryID, key string) (string, error) {
	prefix, err := s.resolve(repositoryID)
	if err != nil {
		return "", err
	}
	if err := validateKey(key); err != nil {
		return "", err
	}
	return artifactObjectKey(prefix, key), nil
}

// SignedURL returns a pre-signed download URL for the object at objectKey,
// valid for expiry. The key is a full bucket key, typically obtained from
// ArtifactObjectKey; the method is the companion read-side primitive for
// consumers that hand artifact downloads to the client instead of streaming
// them, such as packfile-URI offload.
func (s *Storage) SignedURL(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	url, err := s.bucket.SignedURL(ctx, objectKey, &blob.SignedURLOptions{Expiry: expiry})
	if err != nil {
		return "", fmt.Errorf("sign URL for %q: %w", objectKey, translateErr(err))
	}
	return url, nil
}

// ObjectMetadata returns the custom metadata stored with the object at
// objectKey. A missing object satisfies errors.Is(err, fs.ErrNotExist), which
// is how packfile-URI offload's dynamic pack cache tells a miss from a bucket
// failure.
func (s *Storage) ObjectMetadata(ctx context.Context, objectKey string) (map[string]string, error) {
	attrs, err := s.bucket.Attributes(ctx, objectKey)
	if err != nil {
		return nil, fmt.Errorf("attributes for %q: %w", objectKey, translateErr(err))
	}
	return attrs.Metadata, nil
}

// PutObjectWithMetadata writes content at objectKey with the given content
// type and custom metadata, overwriting any existing object. It bypasses
// PutArtifact's artifact-key validation on purpose: the caller supplies a full
// bucket key, and the metadata lets a later ObjectMetadata name the content
// without reading it — packfile-URI offload's dynamic pack cache stores a
// pack's git hash this way.
func (s *Storage) PutObjectWithMetadata(ctx context.Context, objectKey, contentType string, metadata map[string]string, content io.Reader) error {
	// Closing a gocloud writer COMMITS whatever was written; only cancelling
	// its context aborts the upload. The writer therefore runs on its own
	// cancellable context so a failed copy can abort instead of committing a
	// truncated object, which a later read would mistake for the real thing.
	writerCtx, cancelWrite := context.WithCancel(ctx)
	defer cancelWrite()

	w, err := s.bucket.NewWriter(writerCtx, objectKey, &blob.WriterOptions{
		ContentType: contentType,
		Metadata:    metadata,
	})
	if err != nil {
		return fmt.Errorf("open writer for %q: %w", objectKey, err)
	}

	if _, err := io.Copy(w, content); err != nil {
		cancelWrite()
		_ = w.Close()
		return fmt.Errorf("upload to %q: %w", objectKey, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("finalize upload %q: %w", objectKey, err)
	}
	return nil
}

// head resolves the tip of a repository's generation log: the highest committed
// generation and the manifest hash it names. The newest generation is the
// lexicographically first key, and createGeneration writes a "<generation>.<hash>"
// sibling right after it, so a two-result List returns both the current
// generation object and, usually, that sibling. When the sibling is present the
// hash comes straight from its key name; otherwise (a reader racing a writer that
// has not written the sibling yet) head falls back to reading the generation
// body. It returns fs.ErrNotExist when the log is empty.
func (s *Storage) head(ctx context.Context, prefix string) (uint64, string, error) {
	objs, _, err := s.bucket.ListPage(ctx, blob.FirstPageToken, 2, &blob.ListOptions{
		Prefix: generationListPrefix(prefix),
	})
	if err != nil {
		return 0, "", fmt.Errorf("list manifest generations: %w", err)
	}
	if len(objs) == 0 {
		return 0, "", fmt.Errorf("no manifest generation: %w", fs.ErrNotExist)
	}

	// objs[0] is the bare generation object: it sorts before its "<generation>.<hash>"
	// sibling and before any older generation, so it is always the current one.
	generation, err := parseGenerationKey(objs[0].Key)
	if err != nil {
		return 0, "", err
	}

	// If the sibling for this generation is the second entry, the hash is in its
	// name and no body read is needed. objs[1] may instead be an older generation
	// (sibling not yet written), which this prefix check rejects.
	if len(objs) > 1 {
		siblingPrefix := formatGenerationKey(generation) + "."
		if hash, ok := strings.CutPrefix(path.Base(objs[1].Key), siblingPrefix); ok && hash != "" {
			return generation, hash, nil
		}
	}

	// Fall back to the generation body, the source of truth. The hash can't live
	// in the generation object's own key: that key is the compare-and-swap token
	// in createGeneration, so it must stay deterministic per generation.
	data, err := s.bucket.ReadAll(ctx, objs[0].Key)
	if err != nil {
		return 0, "", fmt.Errorf("read manifest generation: %w", err)
	}
	return generation, strings.TrimSpace(string(data)), nil
}

// ReadManifest reads the manifest identified by hash.
func (s *Storage) ReadManifest(ctx context.Context, repositoryID, hash string) (io.ReadCloser, error) {
	prefix, err := s.resolve(repositoryID)
	if err != nil {
		return nil, err
	}
	if err := validateKey(hash); err != nil {
		return nil, err
	}
	manifestPath, err := mvcc.ManifestRelPath(hash)
	if err != nil {
		return nil, err
	}
	reader, err := s.bucket.NewReader(ctx, artifactObjectKey(prefix, manifestPath), nil)
	if err != nil {
		return nil, translateErr(err)
	}
	return reader, nil
}

// ReadArtifact opens the artifact identified by key and passes it to consume,
// closing the reader once consume returns. Concurrent fetches of the same
// artifact are coalesced onto a single download that keeps running past a
// canceled caller, matching LocalStorage; consume must persist the artifact so
// coalesced callers see it.
//
// The reader opens the object lazily, so a missing artifact surfaces as
// fs.ErrNotExist from the reader's first Read or WriteTo inside consume.
func (s *Storage) ReadArtifact(ctx context.Context, repositoryID, key string, consume func(ctx context.Context, r io.ReadCloser) error) error {
	prefix, err := s.resolve(repositoryID)
	if err != nil {
		return err
	}
	if err := validateKey(key); err != nil {
		return err
	}

	do := func(ctx context.Context) (returnedErr error) {
		reader := &artifactReader{ctx: ctx, bucket: s.bucket, key: artifactObjectKey(prefix, key), opts: s.readOptions}
		defer func() {
			// Some drivers only report a truncated download or checksum mismatch
			// on Close, so surface it unless consume already failed.
			if closeErr := reader.Close(); closeErr != nil && returnedErr == nil {
				returnedErr = fmt.Errorf("close reader: %w", closeErr)
			}
		}()
		return consume(ctx, reader)
	}
	return s.runnerCache.Run(ctx, artifactKey{prefix: prefix, key: key}, do)
}

// artifactReader is the reader ReadArtifact hands to consume. It opens the
// object on the first Read, so that io.Copy can call WriteTo instead and let
// Download fetch the artifact in parallel without a wasted GET.
type artifactReader struct {
	ctx    context.Context
	bucket *blob.Bucket
	key    string
	opts   blob.ReaderOptions
	reader *blob.Reader
}

func (a *artifactReader) Read(p []byte) (int, error) {
	if a.reader == nil {
		reader, err := a.bucket.NewReader(a.ctx, a.key, &a.opts)
		if err != nil {
			return 0, translateErr(err)
		}
		a.reader = reader
	}
	return a.reader.Read(p)
}

// WriteTo downloads the whole artifact into w. If w is an io.WriterAt, such as
// the safe.FileWriter the cache writes artifacts with, the driver may split it
// into concurrent ranged reads.
func (a *artifactReader) WriteTo(w io.Writer) (int64, error) {
	if a.reader != nil {
		return 0, errors.New("artifact already partly read")
	}

	cw := &countingWriter{w: w}
	var dst io.Writer = cw
	if wa, ok := w.(io.WriterAt); ok {
		dst = &countingWriterAt{countingWriter: cw, wa: wa}
	}
	err := a.bucket.Download(a.ctx, a.key, dst, &a.opts)
	return cw.n, translateErr(err)
}

func (a *artifactReader) Close() error {
	if a.reader == nil {
		return nil
	}
	return a.reader.Close()
}

// countingWriter counts the bytes written so WriteTo can report them, since
// Download does not.
type countingWriter struct {
	w  io.Writer
	mu sync.Mutex
	n  int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.mu.Lock()
	c.n += int64(n)
	c.mu.Unlock()
	return n, err
}

// countingWriterAt adds WriteAt to a countingWriter whose destination supports
// it. Parallel parts land in any order, so it counts the furthest byte written.
type countingWriterAt struct {
	*countingWriter
	wa io.WriterAt
}

func (c *countingWriterAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := c.wa.WriteAt(p, off)
	c.mu.Lock()
	c.n = max(c.n, off+int64(n))
	c.mu.Unlock()
	return n, err
}

// PutArtifact writes content at key. The caller retains ownership of content
// and is responsible for closing it.
//
// Artifacts are content-addressable, so a key that already exists must already
// hold the same bytes. The write is create-if-absent and a conflict is treated
// as success: a concurrent or earlier writer already stored identical content,
// and re-uploading it would only waste bandwidth.
func (s *Storage) PutArtifact(ctx context.Context, repositoryID, key string, content io.ReadCloser) (returnedErr error) {
	prefix, err := s.resolve(repositoryID)
	if err != nil {
		return err
	}
	if err := validateKey(key); err != nil {
		return err
	}

	writer, err := s.bucket.NewWriter(ctx, artifactObjectKey(prefix, key), &blob.WriterOptions{
		IfNotExist: true,
	})
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}
	defer func() {
		// Close finalizes the upload; drivers evaluate the IfNotExist precondition
		// here, so an already-present artifact surfaces as a conflict on Close.
		// Surface any other close error unless we already have one.
		if closeErr := writer.Close(); closeErr != nil && returnedErr == nil {
			if s.isCreateConflict(closeErr) {
				return
			}
			returnedErr = fmt.Errorf("close writer: %w", closeErr)
		}
	}()

	if _, err := io.Copy(writer, content); err != nil {
		return fmt.Errorf("copy artifact: %w", err)
	}
	return nil
}

// Commit advances the canonical manifest pointer to newHash by appending the
// next generation slot. base is the manifest pointer the cache resolved at the
// start of the RPC. On a lost race — whether a concurrent commit or a stale
// base — it returns a *mvcc.ConflictError naming the concurrent winner (read
// back from the head) so the retry loop can rebase onto it. ConflictError
// unwraps to mvcc.ErrManifestPointerConflict for callers that only check the
// sentinel.
//
// Correctness rests on always targeting exactly base.Generation+1: the
// create-if-absent write to that slot is atomic, so a concurrent commit or a
// stale base is always detected and history can never fork undetected.
//
// An empty base is a genesis commit at generation 0. A non-empty base must carry
// its generation (ManifestPointer.HasGeneration); the cache always resolves it
// via ReadManifestPointer, so a non-empty base without one is a programming
// error. Garbage collection of superseded generations is out of scope here; it
// is safe from the tail (older, larger inverted keys) given a window for
// in-flight readers.
func (s *Storage) Commit(ctx context.Context, repositoryID string, base mvcc.ManifestPointer, newHash string) error {
	prefix, err := s.resolve(repositoryID)
	if err != nil {
		return err
	}

	// newHash is embedded into the generation sibling key, so validate it here as
	// the read/write paths validate their keys: a public caller must not be able to
	// slip a ".." or leading "/" into a key in the shared bucket.
	if err := validateKey(newHash); err != nil {
		return err
	}

	// A genesis commit (empty base) creates generation 0. The create-if-absent
	// rejects it if a generation already exists (a stale genesis). A non-empty
	// base advances to exactly base.Generation+1.
	var targetGeneration uint64
	if base.Hash != "" {
		if !base.HasGeneration {
			return fmt.Errorf("mvcc: commit requires the base generation")
		}
		targetGeneration, err = nextGeneration(base.Generation)
		if err != nil {
			return err
		}
	}

	if err := s.createGeneration(ctx, prefix, targetGeneration, newHash); err != nil {
		if errors.Is(err, mvcc.ErrManifestPointerConflict) {
			// Read the concurrent winner so the retry loop can 3-way merge onto
			// it without re-listing the log.
			generation, hash, headErr := s.head(ctx, prefix)
			return conflictFromHead(base, generation, hash, headErr)
		}
		return err
	}
	return nil
}

// conflictFromHead builds the error a commit returns after losing the CAS race.
// When the head read-back succeeded it is a *mvcc.ConflictError naming the
// winner (hash and generation) so the retry loop can rebase onto it. When the
// read-back failed (e.g. a transient list error or a lagging bucket) the
// conflict sentinel is kept in the chain so the lost race stays a retryable
// conflict rather than becoming a hard failure, matching the behavior before
// the winner was read back.
func conflictFromHead(base mvcc.ManifestPointer, generation uint64, hash string, headErr error) error {
	if headErr != nil {
		return fmt.Errorf("resolve conflict head: %w",
			errors.Join(mvcc.ErrManifestPointerConflict, headErr))
	}
	return &mvcc.ConflictError{
		Base: base,
		Winner: mvcc.ManifestPointer{
			Hash:          hash,
			Generation:    generation,
			HasGeneration: true,
		},
	}
}

// createGeneration writes newHash into the generation slot only if the slot does
// not already exist, mapping a create-if-absent conflict to
// mvcc.ErrManifestPointerConflict. Once the slot is claimed it also writes the
// "<generation>.<hash>" sibling key so a reader can resolve the hash from a
// listing without a body read (see head).
func (s *Storage) createGeneration(ctx context.Context, prefix string, generation uint64, newHash string) error {
	key := generationObjectKey(prefix, generation)
	if err := s.bucket.WriteAll(ctx, key, []byte(newHash), &blob.WriterOptions{
		IfNotExist: true,
	}); err != nil {
		if s.isCreateConflict(err) {
			return mvcc.ErrManifestPointerConflict
		}
		return fmt.Errorf("create manifest generation: %w", err)
	}

	// The generation object above is the source of truth and its create-only
	// write already decided the commit. The sibling key is only a read-side
	// optimization, so a failure here must not fail the commit: readers simply
	// fall back to reading the generation body.
	_ = s.bucket.WriteAll(ctx, generationHashKey(prefix, generation, newHash), nil, nil)
	return nil
}

// isCreateConflict reports whether a create-if-absent (IfNotExist) write failed
// because another writer created the object first. gocloud maps GCS/S3 412
// precondition failures and Azure's BlobAlreadyExists/ConditionNotMet to
// FailedPrecondition; AWS S3 can additionally answer concurrent in-flight
// conditional PUTs on the same key with 409 ConditionalRequestConflict, which
// s3blob leaves as Unknown, so that code is matched explicitly via the generic
// gocloud ErrorAs escape hatch.
func (s *Storage) isCreateConflict(err error) bool {
	if gcerrors.Code(err) == gcerrors.FailedPrecondition {
		return true
	}
	var apiErr smithy.APIError
	if s.bucket.ErrorAs(err, &apiErr) && apiErr.ErrorCode() == "ConditionalRequestConflict" {
		return true
	}
	return false
}

// translateErr maps a gocloud "not found" into fs.ErrNotExist so callers can
// detect a missing object with errors.Is, matching the local-storage backend.
func translateErr(err error) error {
	if err != nil && gcerrors.Code(err) == gcerrors.NotFound {
		return fmt.Errorf("%w: %w", fs.ErrNotExist, err)
	}
	return err
}
