package packfileuri

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
)

// defaultExpiry bounds how long a fetch may keep downloading. The client
// works through the URI list sequentially, one http-fetch child at a
// time, so a busy repository's list can take tens of minutes end to end;
// a URL that expires mid-fetch surfaces as a 403 on a late pack and
// reads like a server defect.
const defaultExpiry = time.Hour

// ObjectStore is the slice of the MVCC object storage the sink needs: the
// generic read interface for resolving the head manifest, plus direct bucket
// addressing and signing so pack downloads can be handed to the client. The
// EE objectstorage.Storage satisfies it; the local-filesystem backend cannot
// sign URLs, so no sink is constructed for it.
type ObjectStore interface {
	mvcc.ObjectStorage

	// ArtifactObjectKey returns the bucket key at which the repository stores
	// the artifact identified by a manifest-relative key such as
	// "pack/<hash>.pack".
	ArtifactObjectKey(repositoryID, key string) (string, error)

	// SignedURL returns a pre-signed download URL for the object at the given
	// bucket key, valid for expiry.
	SignedURL(ctx context.Context, objectKey string, expiry time.Duration) (string, error)

	// ObjectMetadata returns the custom metadata of the object at the given
	// bucket key; a missing object satisfies errors.Is(err, fs.ErrNotExist).
	ObjectMetadata(ctx context.Context, objectKey string) (map[string]string, error)

	// PutObjectWithMetadata writes content at the given bucket key with custom
	// metadata, overwriting any existing object.
	PutObjectWithMetadata(ctx context.Context, objectKey, contentType string, metadata map[string]string, content io.Reader) error
}

// ObjectFormat identifies a repository's object hash algorithm where the
// packfile-URI path needs it: the empty pack written after the URI lines
// carries a trailing checksum in the repository's format, and serving a
// SHA-1 trailer to a SHA-256 repository makes the client reject the fetch.
type ObjectFormat string

const (
	// ObjectFormatSHA1 is the SHA-1 object format.
	ObjectFormatSHA1 ObjectFormat = "sha1"
	// ObjectFormatSHA256 is the SHA-256 object format.
	ObjectFormatSHA256 ObjectFormat = "sha256"
)

// objectFormatFromManifest maps mvcc.Manifest.HashAlgo (1 = SHA-1,
// 2 = SHA-256) to an ObjectFormat.
func objectFormatFromManifest(hashAlgo byte) (ObjectFormat, bool) {
	switch hashAlgo {
	case 1:
		return ObjectFormatSHA1, true
	case 2:
		return ObjectFormatSHA256, true
	default:
		return "", false
	}
}

// ObjectFormatFromHash derives the object format from a hex pack hash: git
// names a pack by its trailing checksum, so the hash length is the format.
func ObjectFormatFromHash(hash string) (ObjectFormat, bool) {
	switch len(hash) {
	case 40:
		return ObjectFormatSHA1, true
	case 64:
		return ObjectFormatSHA256, true
	default:
		return "", false
	}
}

// Pack identifies one pack the head manifest names: the hash a packfile-uri
// line advertises and the bucket key its signed URL must address.
type Pack struct {
	// Hash is the pack's full hex name. The manifest shards artifacts into
	// two-character fanout directories ("pack/ce/c0880….pack"), so the hash
	// is rebuilt from the whole path, not its base name.
	Hash string
	// ObjectKey is the full bucket key of the ".pack" artifact.
	ObjectKey string
}

// Sink reads packfile manifests from the MVCC object storage and signs
// download URLs for the packs they name.
type Sink struct {
	store ObjectStore
}

// NewSink creates a Sink backed by the given object store.
func NewSink(store ObjectStore) *Sink {
	return &Sink{store: store}
}

// ManifestPacks resolves the repository's generation-log head and returns one
// entry per pack the head manifest names, along with the repository's object
// format from the manifest header. A repository with no generation log in the
// store yields an empty list, so the caller falls back to an inline pack
// without treating absence as a failure; an object format the URI path cannot
// serve is an error, because the empty pack's trailer cannot be built for it.
func (s *Sink) ManifestPacks(ctx context.Context, repositoryID string) ([]Pack, ObjectFormat, error) {
	manifest, err := s.baseManifest(ctx, repositoryID)
	if err != nil {
		if errors.As(err, new(*mvcc.NotInitializedError)) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("read base manifest: %w", err)
	}

	// ParseManifest already rejects any hash algo other than SHA-1 or
	// SHA-256, so this branch is exhaustiveness against a future algo being
	// added to the manifest format, not reachable validation.
	format, ok := objectFormatFromManifest(manifest.HashAlgo)
	if !ok {
		return nil, "", fmt.Errorf("unsupported manifest object hash algo %d", manifest.HashAlgo)
	}

	var packs []Pack
	for _, p := range manifest.PackPaths() {
		// PackPaths returns the pack .idx/.pack/.rev triples; only the ".pack"
		// artifact is downloadable pack data.
		if !strings.HasSuffix(p, ".pack") {
			continue
		}
		key, err := s.store.ArtifactObjectKey(repositoryID, p)
		if err != nil {
			return nil, "", fmt.Errorf("resolve object key for %q: %w", p, err)
		}
		// The artifact path shards the hash into a fanout directory
		// ("pack/ce/c0880….pack"), so the full hash is the path with the
		// "pack/" prefix, the ".pack" suffix, and the fanout separator
		// stripped; taking the base name would lose the two-character prefix
		// and git-fetch rejects the short hash on the URI line.
		packs = append(packs, Pack{
			Hash:      strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(p, "pack/"), ".pack"), "/", ""),
			ObjectKey: key,
		})
	}
	return packs, format, nil
}

// baseManifest loads the manifest this RPC's refs were advertised from. The
// spawning RPC's cache pins a base manifest pointer at RPC start; re-resolving
// the head here instead would race concurrent pushes — a force-push plus
// compaction landing between ref advertisement and pack-objects could drop
// packs the client's wants need, failing its connectivity check with no
// fallback. Only a hook no RPC spawned (no cache in the context) reads the
// head directly.
func (s *Sink) baseManifest(ctx context.Context, repositoryID string) (*mvcc.Manifest, error) {
	cache := mvcc.CacheFromContext(ctx)
	if cache == nil {
		_, manifest, err := mvcc.ReadHeadManifest(ctx, s.store, repositoryID)
		return manifest, err
	}

	if err := cache.WaitUntilReady(ctx); err != nil {
		return nil, fmt.Errorf("wait for RPC cache: %w", err)
	}
	hash := cache.BaseManifestPointer()
	if hash == "" {
		return nil, &mvcc.NotInitializedError{RepositoryID: repositoryID}
	}

	reader, err := s.store.ReadManifest(ctx, repositoryID, hash)
	if err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", hash, err)
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", hash, err)
	}
	manifest, err := mvcc.ParseManifest(data)
	if err != nil {
		return nil, fmt.Errorf("parse manifest %q: %w", hash, err)
	}
	return manifest, nil
}

// SignedURL returns a pre-signed URL for the object at the given bucket key.
func (s *Sink) SignedURL(ctx context.Context, objectKey string) (string, error) {
	url, err := s.store.SignedURL(ctx, objectKey, defaultExpiry)
	if err != nil {
		return "", fmt.Errorf("signed URL for %q: %w", objectKey, err)
	}
	return url, nil
}

// --- Dynamic pack cache (PoC: serve a materialized shallow pack by URI) ---
//
// These let a caller store a freshly assembled packfile in the bucket and
// serve its signed URL, so a shallow clone's pack can be fetched from object
// storage directly instead of streamed through the serving node. The stored
// pack's git hash is kept in object metadata so a cache hit can name the pack
// without re-reading it.

const (
	dynPackHashMeta   = "git-pack-hash"
	dynPackFormatMeta = "git-object-format"
)

// DynamicPackObjectKey returns the bucket key under which a dynamic pack for
// the given negotiation cache key is stored, inside the repository's prefix so
// it lives and dies with the repository's other artifacts.
func (s *Sink) DynamicPackObjectKey(repositoryID, cacheKey string) (string, error) {
	return s.store.ArtifactObjectKey(repositoryID, "shallow-cache/"+cacheKey+".pack")
}

// CachedPackExists reports whether a dynamic pack is already stored at the
// bucket key and, if so, the git pack hash and object format recorded in its
// metadata. An entry without a format tag falls back to deriving the format
// from the hash length.
func (s *Sink) CachedPackExists(ctx context.Context, objectKey string) (bool, string, ObjectFormat, error) {
	metadata, err := s.store.ObjectMetadata(ctx, objectKey)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, "", "", nil
		}
		return false, "", "", err
	}

	hash := metadata[dynPackHashMeta]
	// Metadata is bucket content, not trusted input: only the two formats the
	// URI path can serve pass through; anything else (missing, corrupt, or a
	// format this code does not know) falls back to deriving the format from
	// the hash length, and an undeterminable format is reported as empty so
	// the caller serves inline instead of guessing a trailer.
	format := ObjectFormat(metadata[dynPackFormatMeta])
	if format != ObjectFormatSHA1 && format != ObjectFormatSHA256 {
		format, _ = ObjectFormatFromHash(hash)
	}
	return true, hash, format, nil
}

// UploadPack stores the packfile at srcPath under the bucket key, tagging it
// with its git pack hash and object format. Overwrites any existing object
// (content is a pure function of the negotiation key, so a rewrite is
// harmless).
func (s *Sink) UploadPack(ctx context.Context, objectKey, srcPath, packHash string, format ObjectFormat) error {
	f, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open pack %q: %w", srcPath, err)
	}
	defer func() { _ = f.Close() }()

	if err := s.store.PutObjectWithMetadata(ctx, objectKey,
		"application/x-git-packed-objects",
		map[string]string{
			dynPackHashMeta:   packHash,
			dynPackFormatMeta: string(format),
		},
		f,
	); err != nil {
		return fmt.Errorf("upload pack to %q: %w", objectKey, err)
	}
	return nil
}
