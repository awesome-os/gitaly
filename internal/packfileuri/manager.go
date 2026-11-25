package packfileuri

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"golang.org/x/sync/errgroup"
)

// fallbacks counts requests that could have been served by URI but fell back
// to inline pack-objects, by reason, so operators can see signing or bucket
// degradation that the best-effort fallback would otherwise hide.
var fallbacks = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gitaly_packfileuri_fallback_total",
	Help: "Packfile-URI requests that fell back to inline pack-objects, by reason",
}, []string{"reason"})

// dynPackServed counts dynamic shallow packs served by URI, split by whether
// the pack was already cached in the bucket or had to be materialized.
var dynPackServed = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gitaly_packfileuri_dynamic_pack_total",
	Help: "Dynamic shallow packs served by cache result (hit/miss)",
}, []string{"result"})

// signConcurrency is how many URLs are signed in parallel. Under Workload
// Identity each signature costs an IAM signBlob round trip.
const signConcurrency = 16

// PackfileURI holds a single packfile-uri entry: the pack hash and a signed URL.
type PackfileURI struct {
	PackHash string
	URL      string
}

// manifestSigner is the part of Sink the Manager uses, an interface so
// tests can substitute a fake without a real bucket.
type manifestSigner interface {
	ManifestPacks(ctx context.Context, repositoryID string) ([]Pack, ObjectFormat, error)
	SignedURL(ctx context.Context, objectKey string) (string, error)
	DynamicPackObjectKey(repositoryID, cacheKey string) (string, error)
	CachedPackExists(ctx context.Context, objectKey string) (bool, string, ObjectFormat, error)
	UploadPack(ctx context.Context, objectKey, srcPath, packHash string, format ObjectFormat) error
}

// Manager reads packfile manifests from the MVCC object storage and produces
// packfile-uri data for serving fetches.
type Manager struct {
	sink         manifestSigner
	logger       log.Logger
	dynamicPacks bool
}

// ManagerOption configures a Manager.
type ManagerOption func(*Manager)

// WithDynamicPacks enables the dynamic shallow-pack cache; see
// ServeDynamicShallowPack for what it serves and its caveats. Off by default
// because cached packs are never invalidated or deleted.
func WithDynamicPacks() ManagerOption {
	return func(m *Manager) { m.dynamicPacks = true }
}

// NewManager creates a new packfile-URI Manager.
func NewManager(sink *Sink, logger log.Logger, opts ...ManagerOption) *Manager {
	m := &Manager{
		sink:   sink,
		logger: logger,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// DynamicPacksEnabled reports whether the dynamic shallow-pack cache is on, so
// the pack-objects hook can skip its materialization setup entirely when it is
// not.
func (m *Manager) DynamicPacksEnabled() bool { return m.dynamicPacks }

// AdvertiseGitConfig returns a minimal git config that causes upload-pack to
// advertise the packfile-uris capability. The dummy entry uses a zero OID so
// it will never match a real object.
//
// The keepalive must be off wherever packfile URIs can be served: until
// upload-pack has seen pack data it emits a raw "0005\x01" keepalive
// packet, which the client's pkt-line reader decodes as an empty line
// right where it expects the next section header, and the fetch dies
// with fetch-pack's "expected 'packfile'" error. Signing a large
// manifest's URLs can outlast the 5s keepalive default, so with
// keepalives on, serving URIs is a stream corruption race.
//
// The tradeoff: this config applies to every upload-pack spawned by a
// server that enables the manager, including fetches that fall back to
// inline pack-objects, and those lose their idle-timeout protection
// against impatient intermediaries. The decision whether a fetch is
// URI-servable is only made later, inside the pack-objects hook, which
// is too late to restore the keepalive. Scoping it per repository would
// cost a manifest read on every advertisement; packfile_uri_offload is
// therefore an explicit opt-in config.
func (m *Manager) AdvertiseGitConfig() []gitcmd.ConfigPair {
	return []gitcmd.ConfigPair{
		{
			Key:   "uploadpack.blobPackfileUri",
			Value: "0000000000000000000000000000000000000000 0000000000000000000000000000000000000000 http://dummy",
		},
		{
			Key:   "uploadpack.keepalive",
			Value: "0",
		},
	}
}

// ServePackfileURIs signs a URL per manifest pack in parallel batches and
// hands them to write in order, returning the repository's object format so
// the caller can build the empty pack's trailer. Zero served with a nil error
// allows fallback.
func (m *Manager) ServePackfileURIs(ctx context.Context, repo storage.Repository, write func(PackfileURI) error) (int, ObjectFormat, error) {
	relativePath := repoRelativePath(ctx, repo)

	packs, format, err := m.sink.ManifestPacks(ctx, relativePath)
	if err != nil {
		// The request logger already carries the target repository's relative
		// path (requestinfohandler attaches it to the context), so no explicit
		// field here.
		m.logger.WithError(err).ErrorContext(ctx, "packfile-uri: failed to read manifest")
		fallbacks.WithLabelValues("read_manifest").Inc()
		return 0, "", nil
	}

	written := 0
	for start := 0; start < len(packs); start += signConcurrency {
		batch := packs[start:min(start+signConcurrency, len(packs))]
		urls := make([]string, len(batch))

		g, gctx := errgroup.WithContext(ctx)
		for i, pack := range batch {
			g.Go(func() error {
				url, err := m.sink.SignedURL(gctx, pack.ObjectKey)
				if err != nil {
					return fmt.Errorf("sign packfile URL for pack %s: %w", pack.Hash, err)
				}
				urls[i] = url
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			m.logger.WithError(err).ErrorContext(ctx, "packfile-uri: failed to sign URL")
			if written == 0 {
				fallbacks.WithLabelValues("sign").Inc()
				return 0, "", nil
			}
			return written, format, err
		}

		for i, url := range urls {
			if err := write(PackfileURI{PackHash: batch[i].Hash, URL: url}); err != nil {
				return written, format, err
			}
			written++
		}
	}
	return written, format, nil
}

// ServeDynamicShallowPack serves a shallow clone's pack by URI instead of
// inline. It is the read-side counterpart to the static ServePackfileURIs: the
// manifest has no pre-stored pack that is exactly a depth-limited slice, so the
// pack is MATERIALIZED on demand, cached in object storage keyed by the
// negotiation, and served by signed URL. A later identical shallow clone is a
// cache hit and skips assembly entirely.
//
// cacheKey must be a pure function of the negotiation (the pack-objects cache
// key is). produce assembles the pack to a local file and returns its path and
// git pack hash; it is only called on a cache miss. write emits one
// packfile-uri line. Returns true when a URI was served (caller then writes the
// empty inline pack, whose trailer the returned object format picks, and
// returns). Disabled managers always return false so the caller serves inline.
//
// NOTE (PoC): this cache only grows. There is no invalidation — a cached pack
// is keyed only by the negotiation, not by the repository's head, so a push
// that moves HEAD leaves a stale entry — and nothing ever deletes the cached
// objects: every distinct negotiation adds one to the bucket for good. A miss
// also pays for materializing and uploading the pack before the first byte
// reaches the client. Safe for a static-HEAD benchmark; NOT production-safe,
// which is why it sits behind its own packfile_uri_dynamic_packs opt-in.
func (m *Manager) ServeDynamicShallowPack(
	ctx context.Context,
	repo storage.Repository,
	cacheKey string,
	produce func() (packPath string, packHash string, err error),
	write func(PackfileURI) error,
) (bool, ObjectFormat, error) {
	if !m.dynamicPacks {
		return false, "", nil
	}

	objectKey, err := m.sink.DynamicPackObjectKey(repoRelativePath(ctx, repo), cacheKey)
	if err != nil {
		return false, "", fmt.Errorf("resolve dynamic pack key: %w", err)
	}

	exists, hash, format, err := m.sink.CachedPackExists(ctx, objectKey)
	if err != nil {
		m.logger.WithError(err).WarnContext(ctx, "dynamic-pack: cache probe failed, serving inline")
		fallbacks.WithLabelValues("dynamic_probe").Inc()
		return false, "", nil
	}
	if !exists {
		packPath, packHash, perr := produce()
		if perr != nil {
			// Nothing has been written to the response yet, and the caller
			// rewinds stdin and serves the inline pack on a false return, so a
			// failed assembly degrades the same way the probe and upload
			// branches do instead of failing the client's fetch.
			m.logger.WithError(perr).WarnContext(ctx, "dynamic-pack: assembling the pack failed, serving inline")
			fallbacks.WithLabelValues("dynamic_produce").Inc()
			return false, "", nil
		}
		// index-pack printed the hash in the repository's object format, so
		// its length names the format; anything else means produce handed us
		// garbage and serving it would hand the client a broken pack.
		producedFormat, ok := ObjectFormatFromHash(packHash)
		if !ok {
			m.logger.WarnContext(ctx, fmt.Sprintf("dynamic-pack: produced pack hash %q has no known object format, serving inline", packHash))
			fallbacks.WithLabelValues("dynamic_format").Inc()
			return false, "", nil
		}
		if err := m.sink.UploadPack(ctx, objectKey, packPath, packHash, producedFormat); err != nil {
			m.logger.WithError(err).WarnContext(ctx, "dynamic-pack: upload failed, serving inline")
			fallbacks.WithLabelValues("dynamic_upload").Inc()
			return false, "", nil
		}
		hash, format = packHash, producedFormat
		dynPackServed.WithLabelValues("miss").Inc()
	} else {
		if format != ObjectFormatSHA1 && format != ObjectFormatSHA256 {
			// A cached entry whose metadata and hash name no servable format
			// cannot be used: writing its URI line and then failing to build
			// the empty pack's trailer would corrupt a half-flushed response.
			m.logger.WarnContext(ctx, "dynamic-pack: cached entry has unknown object format, serving inline")
			fallbacks.WithLabelValues("dynamic_format").Inc()
			return false, "", nil
		}
		dynPackServed.WithLabelValues("hit").Inc()
	}

	url, err := m.sink.SignedURL(ctx, objectKey)
	if err != nil {
		// Still nothing on the wire: fall back like the static path does when
		// signing fails. Only a write failure below, after a URI line may have
		// been flushed to the client, is unrecoverable.
		m.logger.WithError(err).WarnContext(ctx, "dynamic-pack: signing the URL failed, serving inline")
		fallbacks.WithLabelValues("dynamic_sign").Inc()
		return false, "", nil
	}
	if err := write(PackfileURI{PackHash: hash, URL: url}); err != nil {
		return false, "", err
	}
	return true, format, nil
}

// repoRelativePath resolves the repository's storage-relative path for
// manifest lookups. When running inside a transaction, the original
// repository path is used since the transaction rewrites the path to a
// snapshot. The relative path is the repository ID the MVCC object storage
// namespaces by.
func repoRelativePath(ctx context.Context, repo storage.Repository) string {
	relativePath := repo.GetRelativePath()

	if repoProto, ok := repo.(*gitalypb.Repository); ok {
		if tx := storage.ExtractTransaction(ctx); tx != nil {
			relativePath = tx.OriginalRepository(repoProto).GetRelativePath()
		}
	}

	return relativePath
}
