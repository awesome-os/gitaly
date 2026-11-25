package packfileuri

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
)

// fakeStore implements ObjectStore over an in-memory manifest, standing in for
// the EE object storage.
type fakeStore struct {
	pointer     mvcc.ManifestPointer
	pointerErr  error
	manifest    []byte
	manifestErr error
	keyErr      error
	signErr     error

	pointerReads  int
	manifestReads []string

	objects map[string]storedObject
	metaErr error
	putErr  error
}

func (f *fakeStore) ReadManifestPointer(ctx context.Context, repositoryID string) (mvcc.ManifestPointer, error) {
	f.pointerReads++
	if f.pointerErr != nil {
		return mvcc.ManifestPointer{}, f.pointerErr
	}
	return f.pointer, nil
}

func (f *fakeStore) ReadManifest(ctx context.Context, repositoryID, hash string) (io.ReadCloser, error) {
	f.manifestReads = append(f.manifestReads, hash)
	if f.manifestErr != nil {
		return nil, f.manifestErr
	}
	return io.NopCloser(bytes.NewReader(f.manifest)), nil
}

func (f *fakeStore) ReadArtifact(ctx context.Context, repositoryID, key string, consume func(ctx context.Context, r io.ReadCloser) error) error {
	return errors.New("not implemented")
}

func (f *fakeStore) PutArtifact(ctx context.Context, repositoryID, key string, content io.ReadCloser) error {
	return errors.New("not implemented")
}

func (f *fakeStore) Commit(ctx context.Context, repositoryID string, base mvcc.ManifestPointer, newHash string) error {
	return errors.New("not implemented")
}

func (f *fakeStore) TearDown(ctx context.Context) error { return nil }

func (f *fakeStore) ArtifactObjectKey(repositoryID, key string) (string, error) {
	if f.keyErr != nil {
		return "", f.keyErr
	}
	return "prefix/" + key, nil
}

func (f *fakeStore) SignedURL(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	if f.signErr != nil {
		return "", f.signErr
	}
	return fmt.Sprintf("https://signed.example/%s?expiry=%d", objectKey, int(expiry.Seconds())), nil
}

type storedObject struct {
	contentType string
	metadata    map[string]string
	data        []byte
}

func (f *fakeStore) ObjectMetadata(ctx context.Context, objectKey string) (map[string]string, error) {
	if f.metaErr != nil {
		return nil, f.metaErr
	}
	obj, ok := f.objects[objectKey]
	if !ok {
		return nil, fmt.Errorf("object %q: %w", objectKey, fs.ErrNotExist)
	}
	return obj.metadata, nil
}

func (f *fakeStore) PutObjectWithMetadata(ctx context.Context, objectKey, contentType string, metadata map[string]string, content io.Reader) error {
	if f.putErr != nil {
		return f.putErr
	}
	data, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	if f.objects == nil {
		f.objects = map[string]storedObject{}
	}
	f.objects[objectKey] = storedObject{contentType: contentType, metadata: metadata, data: data}
	return nil
}

// serializedManifest builds a valid binary manifest naming the given OBJS
// artifacts, as the write path would have stored it.
func serializedManifest(t *testing.T, hashAlgo byte, objPaths ...string) []byte {
	t.Helper()
	manifest, err := mvcc.NewManifest(objPaths, nil, hashAlgo)
	require.NoError(t, err)
	body, _, err := mvcc.SerializeManifest(manifest)
	require.NoError(t, err)
	return body
}

func TestSinkManifestPacks(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	t.Run("returns a pack entry per .pack artifact", func(t *testing.T) {
		t.Parallel()
		sink := NewSink(&fakeStore{
			pointer: mvcc.ManifestPointer{Hash: "abc", Generation: 3, HasGeneration: true},
			manifest: serializedManifest(t, 1,
				"pack/1111.idx", "pack/1111.pack", "pack/1111.rev",
				"pack/2222.idx", "pack/2222.pack", "pack/2222.rev",
			),
		})

		packs, format, err := sink.ManifestPacks(ctx, "@hashed/aa/bb/repo.git")
		require.NoError(t, err)
		require.Equal(t, ObjectFormatSHA1, format)
		require.Equal(t, []Pack{
			{Hash: "1111", ObjectKey: "prefix/pack/1111.pack"},
			{Hash: "2222", ObjectKey: "prefix/pack/2222.pack"},
		}, packs, "only .pack artifacts are downloadable pack data; .idx/.rev must be skipped")
	})

	t.Run("fanout-sharded pack paths rebuild the full hash", func(t *testing.T) {
		t.Parallel()
		// The MVCC backend shards artifacts into two-character fanout
		// directories: the pack named cec0880… lives at pack/ce/c0880….pack.
		// git-fetch dies on a URI line whose hash is short, so the fanout
		// separator must not cost the hash its first two characters.
		sha1Hash := "cec0880cd0d0c86fbce7bf3a45ed9c0e9d2a1b3c"
		sha256Hash := "cec0880cd0d0c86fbce7bf3a45ed9c0e9d2a1b3ccec0880cd0d0c86fbce7bf3a"

		sink := NewSink(&fakeStore{
			pointer:  mvcc.ManifestPointer{Hash: "abc", Generation: 3, HasGeneration: true},
			manifest: serializedManifest(t, 1, "pack/"+sha1Hash[:2]+"/"+sha1Hash[2:]+".pack"),
		})
		packs, format, err := sink.ManifestPacks(ctx, "@hashed/aa/bb/repo.git")
		require.NoError(t, err)
		require.Equal(t, ObjectFormatSHA1, format)
		require.Equal(t, []Pack{
			{Hash: sha1Hash, ObjectKey: "prefix/pack/" + sha1Hash[:2] + "/" + sha1Hash[2:] + ".pack"},
		}, packs)

		sink = NewSink(&fakeStore{
			pointer:  mvcc.ManifestPointer{Hash: "abc", Generation: 3, HasGeneration: true},
			manifest: serializedManifest(t, 2, "pack/"+sha256Hash[:2]+"/"+sha256Hash[2:]+".pack"),
		})
		packs, format, err = sink.ManifestPacks(ctx, "@hashed/aa/bb/repo.git")
		require.NoError(t, err)
		require.Equal(t, ObjectFormatSHA256, format, "a SHA-256 manifest must report its format")
		require.Len(t, packs, 1)
		require.Equal(t, sha256Hash, packs[0].Hash)
	})

	t.Run("parser's unknown-algo rejection propagates through ManifestPacks", func(t *testing.T) {
		t.Parallel()
		// mvcc.ParseManifest is what rejects an unknown hash algo (the sink's
		// own switch default is unreachable exhaustiveness); this pins that
		// the parser's rejection surfaces through ManifestPacks instead of
		// being swallowed. Corrupt the hashAlgo byte (the 8th header byte) of
		// a valid manifest and recompute the SHA-256 trailer so parsing gets
		// past the checksum.
		manifest := serializedManifest(t, 1, "pack/1111.pack")
		manifest[7] = 42
		trailer := sha256.Sum256(manifest[:len(manifest)-sha256.Size])
		copy(manifest[len(manifest)-sha256.Size:], trailer[:])

		sink := NewSink(&fakeStore{
			pointer:  mvcc.ManifestPointer{Hash: "abc"},
			manifest: manifest,
		})
		_, _, err := sink.ManifestPacks(ctx, "@hashed/aa/bb/repo.git")
		require.ErrorContains(t, err, "unsupported hash algorithm")
	})

	t.Run("not-initialized repository yields an empty list, not an error", func(t *testing.T) {
		t.Parallel()
		sink := NewSink(&fakeStore{pointerErr: &mvcc.NotInitializedError{RepositoryID: "@hashed/aa/bb/repo.git"}})

		packs, _, err := sink.ManifestPacks(ctx, "@hashed/aa/bb/repo.git")
		require.NoError(t, err)
		require.Empty(t, packs)
	})

	t.Run("pointer read failure propagates", func(t *testing.T) {
		t.Parallel()
		sink := NewSink(&fakeStore{pointerErr: errors.New("bucket down")})

		_, _, err := sink.ManifestPacks(ctx, "@hashed/aa/bb/repo.git")
		require.ErrorContains(t, err, "bucket down")
	})

	t.Run("corrupt manifest propagates", func(t *testing.T) {
		t.Parallel()
		sink := NewSink(&fakeStore{
			pointer:  mvcc.ManifestPointer{Hash: "abc"},
			manifest: []byte("not a manifest"),
		})

		_, _, err := sink.ManifestPacks(ctx, "@hashed/aa/bb/repo.git")
		require.ErrorContains(t, err, "parse manifest")
	})

	t.Run("key resolution failure propagates", func(t *testing.T) {
		t.Parallel()
		sink := NewSink(&fakeStore{
			pointer:  mvcc.ManifestPointer{Hash: "abc"},
			manifest: serializedManifest(t, 1, "pack/1111.pack"),
			keyErr:   errors.New("bad repository ID"),
		})

		_, _, err := sink.ManifestPacks(ctx, "@hashed/aa/bb/repo.git")
		require.ErrorContains(t, err, "bad repository ID")
	})
}

// fakeCache stands in for the RPC's MVCC cache in the context; only the two
// methods baseManifest uses are real.
type fakeCache struct {
	mvcc.Cache
	hash    string
	waitErr error
}

func (f *fakeCache) WaitUntilReady(context.Context) error { return f.waitErr }
func (f *fakeCache) BaseManifestPointer() string          { return f.hash }

func TestSinkManifestPacks_pinnedBase(t *testing.T) {
	t.Parallel()

	const repoID = "@hashed/aa/bb/repo.git"

	t.Run("reads the RPC's pinned manifest, not the head", func(t *testing.T) {
		t.Parallel()
		// The refs the client negotiated against were advertised from the
		// manifest pinned at RPC start; resolving the head again would race a
		// concurrent force-push plus compaction and could drop packs the
		// client's wants need.
		store := &fakeStore{manifest: serializedManifest(t, 1, "pack/1111.pack")}
		sink := NewSink(store)
		ctx := mvcc.ContextWithCache(testhelper.Context(t), &fakeCache{hash: "pinned123"})

		packs, format, err := sink.ManifestPacks(ctx, repoID)
		require.NoError(t, err)
		require.Equal(t, ObjectFormatSHA1, format)
		require.Len(t, packs, 1)
		require.Equal(t, []string{"pinned123"}, store.manifestReads, "must read the pinned manifest hash")
		require.Zero(t, store.pointerReads, "must not re-resolve the generation-log head")
	})

	t.Run("empty pinned pointer reads as not initialized", func(t *testing.T) {
		t.Parallel()
		store := &fakeStore{manifest: serializedManifest(t, 1, "pack/1111.pack")}
		sink := NewSink(store)
		ctx := mvcc.ContextWithCache(testhelper.Context(t), &fakeCache{hash: ""})

		packs, _, err := sink.ManifestPacks(ctx, repoID)
		require.NoError(t, err)
		require.Empty(t, packs, "an uninitialized pinned base falls back to the inline pack")
		require.Zero(t, store.pointerReads)
	})

	t.Run("cache wait failure propagates", func(t *testing.T) {
		t.Parallel()
		sink := NewSink(&fakeStore{})
		ctx := mvcc.ContextWithCache(testhelper.Context(t), &fakeCache{waitErr: errors.New("cache broken")})

		_, _, err := sink.ManifestPacks(ctx, repoID)
		require.ErrorContains(t, err, "cache broken")
	})

	t.Run("no cache in context reads the head", func(t *testing.T) {
		t.Parallel()
		store := &fakeStore{
			pointer:  mvcc.ManifestPointer{Hash: "headhash", Generation: 3, HasGeneration: true},
			manifest: serializedManifest(t, 1, "pack/1111.pack"),
		}
		sink := NewSink(store)

		packs, _, err := sink.ManifestPacks(testhelper.Context(t), repoID)
		require.NoError(t, err)
		require.Len(t, packs, 1)
		require.Equal(t, 1, store.pointerReads, "a hook no RPC spawned resolves the head")
	})
}

func TestSinkSignedURL(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	t.Run("signs with the default expiry", func(t *testing.T) {
		t.Parallel()
		sink := NewSink(&fakeStore{})

		url, err := sink.SignedURL(ctx, "prefix/pack/1111.pack")
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("https://signed.example/prefix/pack/1111.pack?expiry=%d", int(defaultExpiry.Seconds())), url)
	})

	t.Run("signing failure propagates", func(t *testing.T) {
		t.Parallel()
		sink := NewSink(&fakeStore{signErr: errors.New("no token")})

		_, err := sink.SignedURL(ctx, "prefix/pack/1111.pack")
		require.ErrorContains(t, err, "no token")
	})
}

func TestSinkDynamicPackCache(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	const repoID = "@hashed/aa/bb/repo.git"
	const packHash = "1111111111111111111111111111111111111111"

	t.Run("key lives under the repository's shallow-cache prefix", func(t *testing.T) {
		t.Parallel()
		sink := NewSink(&fakeStore{})
		key, err := sink.DynamicPackObjectKey(repoID, "neg-abc")
		require.NoError(t, err)
		require.Equal(t, "prefix/shallow-cache/neg-abc.pack", key)
	})

	t.Run("upload then probe round-trips the pack hash", func(t *testing.T) {
		t.Parallel()
		store := &fakeStore{}
		sink := NewSink(store)

		packPath := filepath.Join(testhelper.TempDir(t), "x.pack")
		require.NoError(t, os.WriteFile(packPath, []byte("PACKDATA"), 0o644))

		key, err := sink.DynamicPackObjectKey(repoID, "neg-abc")
		require.NoError(t, err)

		exists, _, _, err := sink.CachedPackExists(ctx, key)
		require.NoError(t, err)
		require.False(t, exists, "probe before upload must miss")

		require.NoError(t, sink.UploadPack(ctx, key, packPath, packHash, ObjectFormatSHA1))

		exists, hash, format, err := sink.CachedPackExists(ctx, key)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, packHash, hash, "the stored metadata must name the pack")
		require.Equal(t, ObjectFormatSHA1, format, "the stored metadata must name the object format")
		require.Equal(t, []byte("PACKDATA"), store.objects[key].data)
		require.Equal(t, "application/x-git-packed-objects", store.objects[key].contentType)
	})

	t.Run("probe error that is not a miss propagates", func(t *testing.T) {
		t.Parallel()
		sink := NewSink(&fakeStore{metaErr: errors.New("bucket down")})
		_, _, _, err := sink.CachedPackExists(ctx, "prefix/shallow-cache/k.pack")
		require.ErrorContains(t, err, "bucket down")
	})

	t.Run("missing source pack file fails the upload", func(t *testing.T) {
		t.Parallel()
		sink := NewSink(&fakeStore{})
		err := sink.UploadPack(ctx, "prefix/shallow-cache/k.pack", "/nonexistent/x.pack", packHash, ObjectFormatSHA1)
		require.ErrorContains(t, err, "open pack")
	})
}
