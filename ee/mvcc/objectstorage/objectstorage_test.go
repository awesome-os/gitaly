package objectstorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v19/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/blob/s3blob"

	// fileblob backs the signing test: unlike memblob it can produce signed
	// URLs when given a base URL and a secret key.
	_ "gocloud.dev/blob/fileblob"
)

const (
	testRepo = "@hashed/ab/cd/repo.git"
	// testRepoPrefix is testRepo with Gitaly's leading "@" marker stripped, i.e.
	// the bucket key prefix HashedPathPrefix produces.
	testRepoPrefix = "hashed/ab/cd/repo.git"
)

// genPointer is a base with a known generation, as ReadManifestPointer returns.
func genPointer(hash string, gen uint64) mvcc.ManifestPointer {
	return mvcc.ManifestPointer{Hash: hash, Generation: gen, HasGeneration: true}
}

// hashPointer is a base with only a hash (generation unknown), e.g. a read pin,
// which drives Commit's head-resolving fallback path.
func hashPointer(hash string) mvcc.ManifestPointer {
	return mvcc.ManifestPointer{Hash: hash}
}

func newMemStorage(t *testing.T) *Storage {
	t.Helper()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	return NewStorage(bucket, HashedPathPrefix)
}

func put(t *testing.T, s *Storage, key, content string) {
	t.Helper()
	require.NoError(t, s.PutArtifact(testhelper.Context(t), testRepo, key, io.NopCloser(strings.NewReader(content))))
}

func TestOpenStorage_TearDownClosesOwnedBucket(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	s, err := OpenStorage(ctx, "mem://", HashedPathPrefix)
	require.NoError(t, err)
	require.True(t, s.ownsBucket)

	// The bucket is open before teardown.
	_, err = s.bucket.IsAccessible(ctx)
	require.NoError(t, err)

	require.NoError(t, s.TearDown(ctx))

	// Teardown closed the bucket this Storage opened.
	_, err = s.bucket.IsAccessible(ctx)
	require.Error(t, err)

	// One Storage can back several storage paths, so TearDown runs more than
	// once. The second call must not double-close the bucket.
	require.NoError(t, s.TearDown(ctx))
}

func TestNewStorage_TearDownLeavesCallerBucketOpen(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	s := NewStorage(bucket, HashedPathPrefix)

	require.NoError(t, s.TearDown(ctx))

	// The caller owns the bucket, so TearDown leaves it open.
	_, err := bucket.IsAccessible(ctx)
	require.NoError(t, err)
}

func readAll(t *testing.T, rc io.ReadCloser, err error) string {
	t.Helper()
	require.NoError(t, err)
	defer func() { require.NoError(t, rc.Close()) }()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	return string(data)
}

// readArtifact reads an artifact through the consume-callback interface and
// returns its contents (or the ReadArtifact error, e.g. fs.ErrNotExist).
func readArtifact(t *testing.T, s *Storage, key string) (string, error) {
	t.Helper()
	var got string
	err := s.ReadArtifact(testhelper.Context(t), testRepo, key, func(_ context.Context, r io.ReadCloser) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		got = string(data)
		return nil
	})
	return got, err
}

func TestStorage_ArtifactRoundTrip(t *testing.T) {
	ctx := testhelper.Context(t)
	s := newMemStorage(t)

	put(t, s, "pack/abc.pack", "pack-bytes")
	got, err := readArtifact(t, s, "pack/abc.pack")
	require.NoError(t, err)
	require.Equal(t, "pack-bytes", got)

	// A manifest is just an artifact under the manifests/ prefix, sharded into a fanout
	// directory by hash, so ReadManifest resolves the same key PutArtifact wrote.
	put(t, s, "manifests/ab/cdef", "manifest-body")
	rc, err := s.ReadManifest(ctx, testRepo, "abcdef")
	require.Equal(t, "manifest-body", readAll(t, rc, err))
}

func TestStorage_ReadMissing(t *testing.T) {
	ctx := testhelper.Context(t)
	s := newMemStorage(t)

	_, err := s.ReadManifestPointer(ctx, testRepo)
	require.ErrorIs(t, err, fs.ErrNotExist)

	// An empty generation log is "repository not in this bucket", which probing
	// consumers (hydration, packfile-URI offload) tell apart from a read failure
	// by the typed error.
	var notInit *mvcc.NotInitializedError
	require.ErrorAs(t, err, &notInit)
	require.Equal(t, testRepo, notInit.RepositoryID)

	_, err = readArtifact(t, s, "pack/missing.pack")
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestStorage_ArtifactObjectKey(t *testing.T) {
	t.Parallel()
	s := newMemStorage(t)

	key, err := s.ArtifactObjectKey(testRepo, "pack/abc.pack")
	require.NoError(t, err)
	require.Equal(t, testRepoPrefix+"/pack/abc.pack", key)

	// The key goes through the same validation as the read/write paths, so a
	// manifest-supplied path cannot escape the repository's prefix.
	for _, bad := range []string{"", "/abs/path", "pack/../../escape"} {
		_, err := s.ArtifactObjectKey(testRepo, bad)
		require.Error(t, err, "key %q must be rejected", bad)
	}

	// A repository ID the resolver rejects surfaces the resolver's error.
	_, err = s.ArtifactObjectKey("@", "pack/abc.pack")
	require.Error(t, err)
}

func TestStorage_SignedURL(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	// memblob cannot sign URLs; fileblob can when given a base URL and a
	// signing key, which makes it the lightest backend that exercises the
	// real gocloud signing path.
	dir := testhelper.TempDir(t)
	secret := filepath.Join(dir, "secret.key")
	require.NoError(t, os.WriteFile(secret, []byte("0123456789abcdef"), mode.File))

	bucketDir := filepath.Join(dir, "bucket")
	require.NoError(t, os.Mkdir(bucketDir, mode.Directory))

	s, err := OpenStorage(ctx,
		"file://"+bucketDir+"?base_url=https://signed.example/bucket&secret_key_path="+secret,
		HashedPathPrefix,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.TearDown(ctx)) }()

	put(t, s, "pack/abc.pack", "pack bytes")

	key, err := s.ArtifactObjectKey(testRepo, "pack/abc.pack")
	require.NoError(t, err)

	url, err := s.SignedURL(ctx, key, time.Hour)
	require.NoError(t, err)
	require.Contains(t, url, "https://signed.example/bucket")
	require.Contains(t, url, "pack%2Fabc.pack", "the signed URL must name the artifact's key")
}

func TestStorage_CommitGenerations(t *testing.T) {
	ctx := testhelper.Context(t)
	s := newMemStorage(t)

	// Genesis commit establishes the pointer at generation 0.
	require.NoError(t, s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, "Z1"))
	pointer, err := s.ReadManifestPointer(ctx, testRepo)
	require.NoError(t, err)
	require.Equal(t, genPointer("Z1", 0), pointer)

	// Advancing from the resolved base succeeds and writes a new generation slot.
	require.NoError(t, s.Commit(ctx, testRepo, pointer, "Z2"))
	pointer, err = s.ReadManifestPointer(ctx, testRepo)
	require.NoError(t, err)
	require.Equal(t, genPointer("Z2", 1), pointer)

	// A commit from a stale base (generation 0 again) is rejected and leaves the
	// pointer untouched.
	require.ErrorIs(t, s.Commit(ctx, testRepo, genPointer("Z1", 0), "Z3"), mvcc.ErrManifestPointerConflict)
	pointer, err = s.ReadManifestPointer(ctx, testRepo)
	require.NoError(t, err)
	require.Equal(t, "Z2", pointer.Hash)
}

func TestStorage_CommitConcurrentSameBaseConflicts(t *testing.T) {
	ctx := testhelper.Context(t)
	s := newMemStorage(t)

	require.NoError(t, s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, "Z1"))
	base, err := s.ReadManifestPointer(ctx, testRepo)
	require.NoError(t, err)

	// Two writers from the same base target the same next generation slot. The
	// first create-if-absent wins; the second must see a conflict.
	require.NoError(t, s.Commit(ctx, testRepo, base, "Z2a"))
	err = s.Commit(ctx, testRepo, base, "Z2b")
	require.ErrorIs(t, err, mvcc.ErrManifestPointerConflict)

	// The conflict names the concurrent winner (hash and generation) so the retry
	// loop can rebase onto it without re-listing the log.
	var conflict *mvcc.ConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, "Z1", conflict.Base.Hash)
	require.Equal(t, uint64(0), conflict.Base.Generation)
	require.Equal(t, "Z2a", conflict.Winner.Hash)
	require.Equal(t, uint64(1), conflict.Winner.Generation)

	pointer, err := s.ReadManifestPointer(ctx, testRepo)
	require.NoError(t, err)
	require.Equal(t, "Z2a", pointer.Hash)
}

func TestConflictFromHead(t *testing.T) {
	t.Parallel()

	base := mvcc.ManifestPointer{Hash: "base", Generation: 3, HasGeneration: true}

	t.Run("head read-back failure keeps the conflict sentinel", func(t *testing.T) {
		headErr := errors.New("list failed")
		err := conflictFromHead(base, 0, "", headErr)

		// A failed read-back must stay a retryable conflict, not a hard failure.
		require.ErrorIs(t, err, mvcc.ErrManifestPointerConflict)
		require.ErrorIs(t, err, headErr)
		var conflict *mvcc.ConflictError
		require.False(t, errors.As(err, &conflict),
			"with no winner read back there is nothing to merge onto, so no ConflictError")
	})

	t.Run("head read-back success names the winner", func(t *testing.T) {
		err := conflictFromHead(base, 7, "winner", nil)

		require.ErrorIs(t, err, mvcc.ErrManifestPointerConflict)
		var conflict *mvcc.ConflictError
		require.ErrorAs(t, err, &conflict)
		require.Equal(t, "base", conflict.Base.Hash)
		require.Equal(t, uint64(3), conflict.Base.Generation)
		require.Equal(t, "winner", conflict.Winner.Hash)
		require.Equal(t, uint64(7), conflict.Winner.Generation)
	})
}

func TestStorage_EmptyBaseAfterGenesisConflicts(t *testing.T) {
	ctx := testhelper.Context(t)
	s := newMemStorage(t)

	// Once a genesis commit exists, a later commit that still assumes an empty
	// base is stale and must conflict.
	require.NoError(t, s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, "Z1"))
	require.ErrorIs(t, s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, "Z1-other"), mvcc.ErrManifestPointerConflict)
}

func TestStorage_GenerationKeyEncoding(t *testing.T) {
	ctx := testhelper.Context(t)
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	s := NewStorage(bucket, HashedPathPrefix)

	require.NoError(t, s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, "Z1")) // genesis -> generation 0
	base, err := s.ReadManifestPointer(ctx, testRepo)
	require.NoError(t, err)
	require.NoError(t, s.Commit(ctx, testRepo, base, "Z2")) // generation 1

	// The generation number is stored inverted (MaxUint64-gen) as 16 hex digits,
	// so generation 0 is the max key and the newer generation sorts first. Each
	// generation object is followed by its "<generation>.<hash>" sibling, which
	// sorts right after it and before the next generation. Keys are laid out under
	// the @-stripped repository prefix.
	require.Equal(t, []string{
		testRepoPrefix + "/generations/fffffffffffffffe",    // generation 1
		testRepoPrefix + "/generations/fffffffffffffffe.Z2", // generation 1 hash sibling
		testRepoPrefix + "/generations/ffffffffffffffff",    // generation 0 (genesis)
		testRepoPrefix + "/generations/ffffffffffffffff.Z1", // generation 0 hash sibling
	}, listKeys(t, ctx, bucket, testRepoPrefix+"/generations/"))
}

func TestStorage_StripsHashedPathMarker(t *testing.T) {
	ctx := testhelper.Context(t)
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	s := NewStorage(bucket, HashedPathPrefix)

	put(t, s, "pack/abc.pack", "pack-bytes")
	require.NoError(t, s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, "Z1"))

	// Every key this package writes drops Gitaly's leading "@" hashed-path marker,
	// so nothing in the bucket starts with "@".
	for _, key := range listKeys(t, ctx, bucket, "") {
		require.False(t, strings.HasPrefix(key, "@"), "key %q must not keep the @ marker", key)
		require.True(t, strings.HasPrefix(key, testRepoPrefix+"/"), "unexpected key %q", key)
	}
}

func TestStorage_UsesInjectedPrefixResolver(t *testing.T) {
	ctx := testhelper.Context(t)
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	// The resolver decouples the bucket layout from the repository ID: objects
	// land under the opaque prefix it returns, not under the hashed path. This is
	// the seam an object-pool mapping will replace.
	s := NewStorage(bucket, func(string) (string, error) { return "opaque/token", nil })

	require.NoError(t, s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, "Z1"))

	keys := listKeys(t, ctx, bucket, "")
	require.NotEmpty(t, keys)
	for _, key := range keys {
		require.True(t, strings.HasPrefix(key, "opaque/token/"), "unexpected key %q", key)
	}
}

func TestStorage_PutArtifactIsIdempotent(t *testing.T) {
	s := newMemStorage(t)

	// Artifacts are content-addressable, so re-putting the same key is a no-op
	// that must succeed rather than conflict. The first write wins and its bytes
	// remain: a create-if-absent write leaves the existing object untouched.
	put(t, s, "pack/abc.pack", "first")
	put(t, s, "pack/abc.pack", "second")

	got, err := readArtifact(t, s, "pack/abc.pack")
	require.NoError(t, err)
	require.Equal(t, "first", got)
}

func TestStorage_CommitWritesHashSiblingKey(t *testing.T) {
	ctx := testhelper.Context(t)
	s := newMemStorage(t)

	require.NoError(t, s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, "Z1"))

	// The sibling key carries the manifest hash in its name so head can resolve it
	// from a listing without reading the generation body.
	require.Contains(t,
		listKeys(t, ctx, s.bucket, generationListPrefix(testRepoPrefix)),
		generationHashKey(testRepoPrefix, 0, "Z1"))

	pointer, err := s.ReadManifestPointer(ctx, testRepo)
	require.NoError(t, err)
	require.Equal(t, genPointer("Z1", 0), pointer)
}

func TestStorage_HeadFallsBackToGenerationBody(t *testing.T) {
	ctx := testhelper.Context(t)
	s := newMemStorage(t)

	require.NoError(t, s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, "Z1"))

	// A reader may observe the generation object before its hash sibling is
	// written. Deleting the sibling forces head onto the body-read fallback, which
	// must still resolve the same generation and hash.
	require.NoError(t, s.bucket.Delete(ctx, generationHashKey(testRepoPrefix, 0, "Z1")))

	pointer, err := s.ReadManifestPointer(ctx, testRepo)
	require.NoError(t, err)
	require.Equal(t, genPointer("Z1", 0), pointer)
}

func listKeys(t *testing.T, ctx context.Context, bucket *blob.Bucket, prefix string) []string {
	t.Helper()
	iter := bucket.List(&blob.ListOptions{Prefix: prefix})
	var keys []string
	for {
		obj, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		keys = append(keys, obj.Key)
	}
	return keys
}

func TestStorage_CommitRequiresBaseGeneration(t *testing.T) {
	ctx := testhelper.Context(t)
	s := newMemStorage(t)

	require.NoError(t, s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, "Z1"))

	// A non-empty base without a generation is a contract violation: the cache
	// always resolves the generation via ReadManifestPointer. It is neither a
	// genesis nor a CAS conflict, so it returns a plain error.
	err := s.Commit(ctx, testRepo, hashPointer("Z1"), "Z2")
	require.Error(t, err)
	require.NotErrorIs(t, err, mvcc.ErrManifestPointerConflict)
}

func TestStorage_RejectsUnsafeKeys(t *testing.T) {
	ctx := testhelper.Context(t)
	s := newMemStorage(t)

	// A ".." segment or a leading "/" must be rejected before path.Join can clean
	// it and let a key escape the repository's prefix in the shared bucket.
	for _, key := range []string{"../other/pack", "pack/../../escape", "/abs/pack", ""} {
		_, readErr := readArtifact(t, s, key)
		require.Error(t, readErr, "ReadArtifact must reject %q", key)

		putErr := s.PutArtifact(ctx, testRepo, key, io.NopCloser(strings.NewReader("x")))
		require.Error(t, putErr, "PutArtifact must reject %q", key)

		_, manErr := s.ReadManifest(ctx, testRepo, key)
		require.Error(t, manErr, "ReadManifest must reject %q", key)

		// newHash is embedded into the generation sibling key, so Commit must
		// reject it too rather than rely on callers validating upstream.
		commitErr := s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, key)
		require.Error(t, commitErr, "Commit must reject newHash %q", key)
	}
}

func TestStorage_RejectsEmptyRepositoryID(t *testing.T) {
	ctx := testhelper.Context(t)
	s := newMemStorage(t)

	// "@" strips to an empty prefix, which would collapse keys to the bucket root.
	for _, repo := range []string{"", "@"} {
		_, err := s.ReadManifestPointer(ctx, repo)
		require.Error(t, err, "empty repository id %q must be rejected", repo)
		require.Error(t, s.Commit(ctx, repo, mvcc.ManifestPointer{}, "Z1"))
	}
}

func TestStorage_TearDownKeepsBucketOpen(t *testing.T) {
	ctx := testhelper.Context(t)

	// The caller owns the bucket, so TearDown must not close it: the bucket stays
	// usable afterwards.
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	s := NewStorage(bucket, HashedPathPrefix)

	require.NoError(t, s.TearDown(ctx))
	require.NoError(t, s.Commit(ctx, testRepo, mvcc.ManifestPointer{}, "Z1"), "bucket must stay open after TearDown")
}

// copyArtifact reads an artifact the way the MVCC cache does: io.Copy into a
// safe.FileWriter, which takes the artifact reader's WriteTo path.
func copyArtifact(t *testing.T, s *Storage, key string) (string, int64, error) {
	t.Helper()
	dst := filepath.Join(testhelper.TempDir(t), "artifact")
	var n int64
	err := s.ReadArtifact(testhelper.Context(t), testRepo, key, func(ctx context.Context, r io.ReadCloser) error {
		w, err := safe.NewFileWriter(dst)
		if err != nil {
			return err
		}
		defer func() { _ = w.Close() }()
		if n, err = io.Copy(w, r); err != nil {
			return err
		}
		return w.Commit(ctx)
	})
	if err != nil {
		return "", n, err
	}
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	return string(data), n, nil
}

func TestStorage_ReadArtifactWriteTo(t *testing.T) {
	t.Parallel()
	s := newMemStorage(t)

	put(t, s, "pack/abc.pack", "pack-bytes")
	got, n, err := copyArtifact(t, s, "pack/abc.pack")
	require.NoError(t, err)
	require.Equal(t, "pack-bytes", got)
	require.Equal(t, int64(len("pack-bytes")), n)

	_, _, err = copyArtifact(t, s, "pack/missing.pack")
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestStorage_ReadArtifactParallel(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	const partSize = 1024
	content := make([]byte, 10*partSize+17)
	for i := range content {
		content[i] = byte(i % 251)
	}
	key := testRepoPrefix + "/pack/big.pack"

	var mu sync.Mutex
	var ranges []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/bucket/"+key {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		ranges = append(ranges, r.Header.Get("Range"))
		mu.Unlock()
		w.Header().Set("ETag", `"etag"`)
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(content))
	}))
	t.Cleanup(srv.Close)

	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  aws.AnonymousCredentials{},
	})
	bucket, err := s3blob.OpenBucket(ctx, client, "bucket", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bucket.Close() })

	s := NewStorage(bucket, HashedPathPrefix)
	s.readOptions.BufferSize = partSize
	s.readOptions.MaxConcurrency = 4

	got, n, err := copyArtifact(t, s, "pack/big.pack")
	require.NoError(t, err)
	require.Equal(t, string(content), got)
	require.Equal(t, int64(len(content)), n)

	// One ranged GET per part, not one stream.
	require.Len(t, ranges, 11)
	for _, r := range ranges {
		require.True(t, strings.HasPrefix(r, "bytes="), r)
	}
}

// failingReader errors after yielding a little data, simulating a source file
// truncated mid-upload.
type failingReader struct{ n int }

func (r *failingReader) Read(p []byte) (int, error) {
	if r.n > 0 {
		r.n--
		return copy(p, []byte("data")), nil
	}
	return 0, errors.New("source went away")
}

func TestStorage_PutObjectWithMetadata(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	t.Run("round-trips content type and metadata", func(t *testing.T) {
		t.Parallel()
		s := newMemStorage(t)

		require.NoError(t, s.PutObjectWithMetadata(ctx, "some/key.pack",
			"application/x-git-packed-objects",
			map[string]string{"git-pack-hash": "abc"},
			strings.NewReader("PACK"),
		))

		metadata, err := s.ObjectMetadata(ctx, "some/key.pack")
		require.NoError(t, err)
		require.Equal(t, "abc", metadata["git-pack-hash"])
	})

	t.Run("failed copy aborts the upload instead of committing a truncated object", func(t *testing.T) {
		t.Parallel()
		s := newMemStorage(t)

		err := s.PutObjectWithMetadata(ctx, "some/key.pack",
			"application/x-git-packed-objects",
			map[string]string{"git-pack-hash": "abc"},
			&failingReader{n: 2},
		)
		require.ErrorContains(t, err, "source went away")

		// Closing a gocloud writer commits whatever was written unless its
		// context is cancelled first; a truncated pack left behind here would
		// read as a valid cache hit forever.
		_, err = s.ObjectMetadata(ctx, "some/key.pack")
		require.ErrorIs(t, err, fs.ErrNotExist, "nothing may be committed after a failed copy")
	})

	t.Run("missing object satisfies fs.ErrNotExist", func(t *testing.T) {
		t.Parallel()
		s := newMemStorage(t)
		_, err := s.ObjectMetadata(ctx, "absent/key.pack")
		require.ErrorIs(t, err, fs.ErrNotExist)
	})
}
