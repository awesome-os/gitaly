package packfileuri

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

func TestManagerAdvertiseGitConfig(t *testing.T) {
	t.Parallel()

	mgr := &Manager{sink: &fakeSink{}, logger: testhelper.NewLogger(t)}

	// uploadpack.keepalive=0 is what stops upload-pack's pre-packfile
	// keepalive packet from corrupting the packfile-uris section; see
	// AdvertiseGitConfig for the mechanism. Pin the exact pairs so a
	// cleanup cannot drop it silently.
	require.Equal(t, []gitcmd.ConfigPair{
		{
			Key:   "uploadpack.blobPackfileUri",
			Value: "0000000000000000000000000000000000000000 0000000000000000000000000000000000000000 http://dummy",
		},
		{
			Key:   "uploadpack.keepalive",
			Value: "0",
		},
	}, mgr.AdvertiseGitConfig())
}

type fakeSink struct {
	packs    []Pack
	readErr  error
	signErr  map[string]error
	signWait time.Duration

	mu          sync.Mutex
	signCalls   int
	inFlight    int
	maxInFlight int

	format ObjectFormat // object format ManifestPacks reports; SHA-1 when unset

	cached       map[string]string // objectKey -> stored pack hash (dynamic cache)
	cachedFormat ObjectFormat      // overrides the hash-derived format when set
	uploads      int
	cacheErr     error
	uploadErr    error
}

func (f *fakeSink) DynamicPackObjectKey(repositoryID, cacheKey string) (string, error) {
	return "prefix/" + repositoryID + "/shallow-cache/" + cacheKey + ".pack", nil
}

func (f *fakeSink) CachedPackExists(ctx context.Context, objectKey string) (bool, string, ObjectFormat, error) {
	if f.cacheErr != nil {
		return false, "", "", f.cacheErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	h, ok := f.cached[objectKey]
	if f.cachedFormat != "" {
		return ok, h, f.cachedFormat, nil
	}
	format, _ := ObjectFormatFromHash(h)
	return ok, h, format, nil
}

func (f *fakeSink) UploadPack(ctx context.Context, objectKey, srcPath, packHash string, format ObjectFormat) error {
	if f.uploadErr != nil {
		return f.uploadErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cached == nil {
		f.cached = map[string]string{}
	}
	f.cached[objectKey] = packHash
	f.uploads++
	return nil
}

func (f *fakeSink) ManifestPacks(ctx context.Context, repositoryID string) ([]Pack, ObjectFormat, error) {
	if f.readErr != nil {
		return nil, "", f.readErr
	}
	format := f.format
	if format == "" {
		format = ObjectFormatSHA1
	}
	return f.packs, format, nil
}

func (f *fakeSink) SignedURL(ctx context.Context, objectKey string) (string, error) {
	f.mu.Lock()
	f.signCalls++
	f.inFlight++
	if f.inFlight > f.maxInFlight {
		f.maxInFlight = f.inFlight
	}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()

	time.Sleep(f.signWait)

	if err := f.signErr[objectKey]; err != nil {
		return "", err
	}
	return "https://signed.example/" + objectKey, nil
}

// packFor builds the Pack entry the fake sink would return for a hash, keyed
// the way the real sink keys them.
func packFor(hash string) Pack {
	return Pack{Hash: hash, ObjectKey: fmt.Sprintf("hashed/aa/bb/repo.git/pack/%s.pack", hash)}
}

func TestManagerServePackfileURIs(t *testing.T) {
	t.Parallel()

	repo := &gitalypb.Repository{RelativePath: "@hashed/aa/bb/repo.git"}
	packsFor := func(hashes ...string) []Pack {
		packs := make([]Pack, len(hashes))
		for i, h := range hashes {
			packs[i] = packFor(h)
		}
		return packs
	}

	// manyHashes spans two signing batches, separating failures before any
	// write (first batch) from failures after the response is committed.
	manyHashes := make([]string, signConcurrency+1)
	for i := range manyHashes {
		manyHashes[i] = fmt.Sprintf("h%02d", i+1)
	}
	manyURIs := make([]PackfileURI, signConcurrency)
	for i, hash := range manyHashes[:signConcurrency] {
		manyURIs[i] = PackfileURI{PackHash: hash, URL: "https://signed.example/" + packFor(hash).ObjectKey}
	}

	for _, tc := range []struct {
		desc           string
		sink           *fakeSink
		writeErr       error
		expectedServed int
		expectedErr    string
		expectedURIs   []PackfileURI
	}{
		{
			desc:           "streams every manifest pack in order",
			sink:           &fakeSink{packs: packsFor("h1", "h2")},
			expectedServed: 2,
			expectedURIs: []PackfileURI{
				{PackHash: "h1", URL: "https://signed.example/" + packFor("h1").ObjectKey},
				{PackHash: "h2", URL: "https://signed.example/" + packFor("h2").ObjectKey},
			},
		},
		{
			desc: "manifest read error yields nothing so the caller can fall back",
			sink: &fakeSink{readErr: errors.New("bucket down")},
		},
		{
			desc: "empty manifest yields nothing",
			sink: &fakeSink{},
		},
		{
			desc: "signing failure before any write yields nothing so the caller can fall back",
			sink: &fakeSink{
				packs:   packsFor("h1", "h2"),
				signErr: map[string]error{packFor("h1").ObjectKey: errors.New("no token")},
			},
		},
		{
			desc: "signing failure anywhere in the first batch yields nothing so the caller can fall back",
			sink: &fakeSink{
				packs:   packsFor("h1", "h2"),
				signErr: map[string]error{packFor("h2").ObjectKey: errors.New("no token")},
			},
		},
		{
			desc: "signing failure after a batch is written aborts the committed response",
			sink: &fakeSink{
				packs:   packsFor(manyHashes...),
				signErr: map[string]error{packFor(manyHashes[signConcurrency]).ObjectKey: errors.New("no token")},
			},
			expectedServed: signConcurrency,
			expectedErr:    "sign packfile URL for pack h17: no token",
			expectedURIs:   manyURIs,
		},
		{
			desc:        "write failure propagates",
			sink:        &fakeSink{packs: packsFor("h1")},
			writeErr:    errors.New("client hung up"),
			expectedErr: "client hung up",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := testhelper.Context(t)
			mgr := &Manager{sink: tc.sink, logger: testhelper.NewLogger(t)}

			var written []PackfileURI
			served, format, err := mgr.ServePackfileURIs(ctx, repo, func(uri PackfileURI) error {
				if tc.writeErr != nil {
					return tc.writeErr
				}
				written = append(written, uri)
				return nil
			})

			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectedServed, served)
			require.Equal(t, tc.expectedURIs, written)
			if served > 0 {
				require.Equal(t, ObjectFormatSHA1, format, "the fake sink reports a SHA-1 manifest")
			}
		})
	}
}

func TestManagerServePackfileURIs_signsConcurrently(t *testing.T) {
	t.Parallel()

	repo := &gitalypb.Repository{RelativePath: "@hashed/aa/bb/repo.git"}

	makePacks := func(n int) []Pack {
		packs := make([]Pack, n)
		for i := range packs {
			packs[i] = packFor(fmt.Sprintf("h%02d", i+1))
		}
		return packs
	}

	t.Run("signs batches concurrently within the limit", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)

		packs := makePacks(signConcurrency + 1)
		// The sleep keeps a batch's signers in flight together; sequential
		// signing would show up deterministically as maxInFlight == 1.
		sink := &fakeSink{packs: packs, signWait: time.Millisecond}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t)}

		var written []string
		served, _, err := mgr.ServePackfileURIs(ctx, repo, func(uri PackfileURI) error {
			written = append(written, uri.PackHash)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, len(packs), served)
		hashes := make([]string, len(packs))
		for i, p := range packs {
			hashes[i] = p.Hash
		}
		require.Equal(t, hashes, written, "URIs must be written in manifest order")
		require.Greater(t, sink.maxInFlight, 1, "URLs must be signed concurrently")
		require.LessOrEqual(t, sink.maxInFlight, signConcurrency, "parallelism must not exceed one batch")
	})

	t.Run("write failure stops signing later batches", func(t *testing.T) {
		t.Parallel()
		ctx := testhelper.Context(t)

		packs := makePacks(signConcurrency + 1)
		sink := &fakeSink{packs: packs}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t)}

		served, _, err := mgr.ServePackfileURIs(ctx, repo, func(uri PackfileURI) error {
			return errors.New("client hung up")
		})
		require.ErrorContains(t, err, "client hung up")
		require.Equal(t, 0, served)
		require.Equal(t, signConcurrency, sink.signCalls, "must not sign the next batch after the client is gone")
	})
}

func TestManagerServeDynamicShallowPack(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	repo := &gitalypb.Repository{RelativePath: "@hashed/aa/bb/repo.git"}
	cacheKey := "neg-abc"
	objKey := "prefix/" + repo.GetRelativePath() + "/shallow-cache/" + cacheKey + ".pack"
	const producedHash = "1111111111111111111111111111111111111111"
	const storedHash = "2222222222222222222222222222222222222222"

	t.Run("disabled manager always serves inline", func(t *testing.T) {
		t.Parallel()
		sink := &fakeSink{cached: map[string]string{objKey: storedHash}}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t)}

		require.False(t, mgr.DynamicPacksEnabled())
		served, _, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { t.Fatal("must not assemble"); return "", "", nil },
			func(u PackfileURI) error { t.Fatal("must not write"); return nil },
		)
		require.NoError(t, err)
		require.False(t, served, "a disabled manager must fall back to inline even on a would-be hit")
	})

	t.Run("cache miss materializes, uploads, and serves the produced hash", func(t *testing.T) {
		t.Parallel()
		sink := &fakeSink{}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t), dynamicPacks: true}

		produceCalls := 0
		var got []PackfileURI
		served, format, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { produceCalls++; return "/tmp/ignored.pack", producedHash, nil },
			func(u PackfileURI) error { got = append(got, u); return nil },
		)
		require.NoError(t, err)
		require.True(t, served)
		require.Equal(t, ObjectFormatSHA1, format, "a 40-hex produced hash names SHA-1")
		require.Equal(t, 1, produceCalls, "miss must assemble exactly once")
		require.Equal(t, 1, sink.uploads, "miss must upload the pack")
		require.Len(t, got, 1)
		require.Equal(t, producedHash, got[0].PackHash)
		require.Equal(t, "https://signed.example/"+objKey, got[0].URL)
	})

	t.Run("cache hit skips assembly and serves the stored hash", func(t *testing.T) {
		t.Parallel()
		sink := &fakeSink{cached: map[string]string{objKey: storedHash}}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t), dynamicPacks: true}

		produceCalls := 0
		var got []PackfileURI
		served, format, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { produceCalls++; return "", "", nil },
			func(u PackfileURI) error { got = append(got, u); return nil },
		)
		require.NoError(t, err)
		require.True(t, served)
		require.Equal(t, ObjectFormatSHA1, format)
		require.Equal(t, 0, produceCalls, "a hit MUST NOT re-assemble the pack")
		require.Equal(t, 0, sink.uploads, "a hit MUST NOT re-upload")
		require.Len(t, got, 1)
		require.Equal(t, storedHash, got[0].PackHash, "a hit serves the stored pack's hash")
	})

	t.Run("cached entry with an unservable hash falls back inline", func(t *testing.T) {
		t.Parallel()
		// A stored hash that is neither 40 nor 64 hex characters names no
		// servable object format, so the hit must not be served: writing its
		// URI line and then failing on the empty pack's trailer would corrupt
		// a half-flushed response.
		sink := &fakeSink{cached: map[string]string{objKey: "deadbeef"}}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t), dynamicPacks: true}

		served, _, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { t.Fatal("a hit must not assemble"); return "", "", nil },
			func(u PackfileURI) error { t.Fatal("an unservable hit must not write a URI line"); return nil },
		)
		require.NoError(t, err)
		require.False(t, served, "an unservable cached hash must fall back to inline")
	})

	t.Run("cached entry with a bogus format tag falls back inline", func(t *testing.T) {
		t.Parallel()
		sink := &fakeSink{cached: map[string]string{objKey: storedHash}, cachedFormat: "md5"}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t), dynamicPacks: true}

		served, _, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { t.Fatal("a hit must not assemble"); return "", "", nil },
			func(u PackfileURI) error { t.Fatal("a bogus format must not write a URI line"); return nil },
		)
		require.NoError(t, err)
		require.False(t, served, "a format the URI path cannot serve must fall back to inline")
	})

	t.Run("sha256 produced hash is uploaded and reported as sha256", func(t *testing.T) {
		t.Parallel()
		sha256Hash := strings.Repeat("ab", 32)
		sink := &fakeSink{}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t), dynamicPacks: true}

		served, format, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { return "/tmp/x.pack", sha256Hash, nil },
			func(u PackfileURI) error { return nil },
		)
		require.NoError(t, err)
		require.True(t, served)
		require.Equal(t, ObjectFormatSHA256, format)
	})

	t.Run("cache probe failure falls back to inline (served=false)", func(t *testing.T) {
		t.Parallel()
		sink := &fakeSink{cacheErr: errors.New("s3 down")}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t), dynamicPacks: true}
		served, _, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { return "/tmp/x.pack", producedHash, nil },
			func(u PackfileURI) error { return nil },
		)
		require.NoError(t, err)
		require.False(t, served, "a probe failure must fall back to inline, not error")
	})

	t.Run("upload failure falls back to inline (served=false)", func(t *testing.T) {
		t.Parallel()
		sink := &fakeSink{uploadErr: errors.New("s3 down")}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t), dynamicPacks: true}
		served, _, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { return "/tmp/x.pack", producedHash, nil },
			func(u PackfileURI) error { return nil },
		)
		require.NoError(t, err)
		require.False(t, served, "an upload failure must fall back to inline, not error")
	})

	t.Run("produce failure falls back to inline (served=false)", func(t *testing.T) {
		t.Parallel()
		// Nothing has been written to the response when assembly fails, so the
		// fetch must degrade to the inline pack rather than fail.
		sink := &fakeSink{}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t), dynamicPacks: true}
		served, _, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { return "", "", errors.New("pack-objects died") },
			func(u PackfileURI) error { t.Fatal("a failed assembly must not write a URI line"); return nil },
		)
		require.NoError(t, err)
		require.False(t, served, "a produce failure must fall back to inline, not error")
	})

	t.Run("produced hash with no known format falls back to inline", func(t *testing.T) {
		t.Parallel()
		sink := &fakeSink{}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t), dynamicPacks: true}
		served, _, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { return "/tmp/x.pack", "deadbeef", nil },
			func(u PackfileURI) error { t.Fatal("a malformed hash must not write a URI line"); return nil },
		)
		require.NoError(t, err)
		require.False(t, served, "a malformed produced hash must fall back to inline")
		require.Equal(t, 0, sink.uploads, "a malformed hash must not be uploaded")
	})

	t.Run("signing failure falls back to inline (served=false)", func(t *testing.T) {
		t.Parallel()
		sink := &fakeSink{signErr: map[string]error{objKey: errors.New("no token")}}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t), dynamicPacks: true}
		served, _, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { return "/tmp/x.pack", producedHash, nil },
			func(u PackfileURI) error { t.Fatal("a failed signing must not write a URI line"); return nil },
		)
		require.NoError(t, err)
		require.False(t, served, "a signing failure must fall back to inline, not error")
		require.Equal(t, 1, sink.uploads, "the assembled pack is still cached for the next request")
	})

	t.Run("write failure stays a hard error", func(t *testing.T) {
		t.Parallel()
		// The write callback flushes to the client, so once it fails the
		// response can no longer fall back to an inline pack.
		sink := &fakeSink{}
		mgr := &Manager{sink: sink, logger: testhelper.NewLogger(t), dynamicPacks: true}
		_, _, err := mgr.ServeDynamicShallowPack(ctx, repo, cacheKey,
			func() (string, string, error) { return "/tmp/x.pack", producedHash, nil },
			func(u PackfileURI) error { return errors.New("client hung up") },
		)
		require.ErrorContains(t, err, "client hung up")
	})
}
