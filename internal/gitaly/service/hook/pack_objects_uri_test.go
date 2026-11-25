package hook

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v19/internal/packfileuri"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

func TestScanNegotiation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc     string
		stdin    string
		expected negotiationInfo
	}{
		{desc: "empty input", stdin: ""},
		{desc: "full clone has no --not", stdin: "aaa\nbbb\n"},
		{desc: "full clone with empty --not section", stdin: "aaa\nbbb\n--not\n"},
		{desc: "full clone with blank lines after --not", stdin: "aaa\n--not\n\n\n"},
		{
			desc:     "haves after --not",
			stdin:    "aaa\n--not\nccc\n",
			expected: negotiationInfo{hasExclusions: true},
		},
		{
			desc:     "shallow edge after --not and a blank",
			stdin:    "aaa\n--not\n\nddd\n",
			expected: negotiationInfo{hasExclusions: true},
		},
		{
			// The shape a v2 depth-limited clone of a whole branch produces:
			// a --shallow line and a want, with nothing after --not. The full
			// pack is still wrong for it.
			desc:     "depth-limited clone with empty --not section",
			stdin:    "--shallow aaa\naaa\n--not\n\n",
			expected: negotiationInfo{hasShallow: true},
		},
		{
			desc:     "depth-limited fetch with haves",
			stdin:    "--shallow aaa\naaa\n--not\nccc\n",
			expected: negotiationInfo{hasShallow: true, hasExclusions: true},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			got, err := scanNegotiation(strings.NewReader(tc.stdin))
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestEmptyPack(t *testing.T) {
	t.Parallel()

	t.Run("sha1", func(t *testing.T) {
		t.Parallel()
		pack, err := emptyPack(packfileuri.ObjectFormatSHA1)
		require.NoError(t, err)
		require.Len(t, pack, 12+sha1.Size)
		requireEmptyPackHeader(t, pack)
		sum := sha1.Sum(pack[:12])
		require.Equal(t, sum[:], pack[12:], "trailer must be the SHA-1 of the header")
	})

	t.Run("sha256", func(t *testing.T) {
		t.Parallel()
		pack, err := emptyPack(packfileuri.ObjectFormatSHA256)
		require.NoError(t, err)
		require.Len(t, pack, 12+sha256.Size)
		requireEmptyPackHeader(t, pack)
		sum := sha256.Sum256(pack[:12])
		require.Equal(t, sum[:], pack[12:], "trailer must be the SHA-256 of the header")
	})

	t.Run("unknown format is an error", func(t *testing.T) {
		t.Parallel()
		_, err := emptyPack("")
		require.ErrorContains(t, err, "no object format")
	})
}

func requireEmptyPackHeader(t *testing.T, pack []byte) {
	t.Helper()
	require.Equal(t, "PACK", string(pack[:4]))
	require.EqualValues(t, 2, binary.BigEndian.Uint32(pack[4:8]), "pack version")
	require.EqualValues(t, 0, binary.BigEndian.Uint32(pack[8:12]), "object count")
}

// uriTestStore implements packfileuri.ObjectStore over a serialized in-memory
// manifest, so runPackObjects can be driven through a real packfileuri.Manager
// without a bucket.
type uriTestStore struct {
	pointerErr error
	manifest   []byte

	// objects backs the dynamic-pack cache methods in memory.
	objects map[string]uriStoredObject
}

type uriStoredObject struct {
	metadata map[string]string
	data     []byte
}

func (f *uriTestStore) ReadManifestPointer(ctx context.Context, repositoryID string) (mvcc.ManifestPointer, error) {
	if f.pointerErr != nil {
		return mvcc.ManifestPointer{}, f.pointerErr
	}
	return mvcc.ManifestPointer{Hash: "abc", Generation: 1, HasGeneration: true}, nil
}

func (f *uriTestStore) ReadManifest(ctx context.Context, repositoryID, hash string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.manifest)), nil
}

func (f *uriTestStore) ReadArtifact(ctx context.Context, repositoryID, key string, consume func(ctx context.Context, r io.ReadCloser) error) error {
	return fmt.Errorf("not implemented")
}

func (f *uriTestStore) PutArtifact(ctx context.Context, repositoryID, key string, content io.ReadCloser) error {
	return fmt.Errorf("not implemented")
}

func (f *uriTestStore) Commit(ctx context.Context, repositoryID string, base mvcc.ManifestPointer, newHash string) error {
	return fmt.Errorf("not implemented")
}

func (f *uriTestStore) TearDown(ctx context.Context) error { return nil }

func (f *uriTestStore) ArtifactObjectKey(repositoryID, key string) (string, error) {
	return "prefix/" + key, nil
}

func (f *uriTestStore) SignedURL(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "https://signed.example/" + objectKey, nil
}

func (f *uriTestStore) ObjectMetadata(ctx context.Context, objectKey string) (map[string]string, error) {
	obj, ok := f.objects[objectKey]
	if !ok {
		return nil, fmt.Errorf("object %q: %w", objectKey, fs.ErrNotExist)
	}
	return obj.metadata, nil
}

func (f *uriTestStore) PutObjectWithMetadata(ctx context.Context, objectKey, contentType string, metadata map[string]string, content io.Reader) error {
	data, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	if f.objects == nil {
		f.objects = map[string]uriStoredObject{}
	}
	f.objects[objectKey] = uriStoredObject{metadata: metadata, data: data}
	return nil
}

// collectBand1 demuxes runPackObjects' sideband output and returns the payload
// of band 1 (pack data / URI lines).
func collectBand1(t *testing.T, output []byte) []byte {
	t.Helper()
	var band1 []byte
	scanner := pktline.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		pkt := scanner.Bytes()
		require.Greater(t, len(pkt), 4)
		if pkt[4] == 1 {
			band1 = append(band1, pkt[5:]...)
		}
	}
	require.NoError(t, scanner.Err())
	return band1
}

func TestRunPackObjects_packfileURI(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	parent := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
	head := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithParents(parent))

	req := &gitalypb.PackObjectsHookWithSidechannelRequest{Repository: repo}
	logger := testhelper.NewLogger(t)

	parseArgs := func(t *testing.T) *packObjectsArgs {
		t.Helper()
		args, err := parsePackObjectsArgs([]string{"pack-objects", "--revs", "--thin", "--stdout", "--uri-protocol=https"})
		require.NoError(t, err)
		require.True(t, args.hasURIProtocol())
		return args
	}

	manifestFor := func(t *testing.T, hashAlgo byte, packPaths ...string) []byte {
		t.Helper()
		manifest, err := mvcc.NewManifest(packPaths, nil, hashAlgo)
		require.NoError(t, err)
		body, _, err := mvcc.SerializeManifest(manifest)
		require.NoError(t, err)
		return body
	}

	t.Run("full clone is served by URI with no git spawned", func(t *testing.T) {
		// The fanout-sharded path must surface as the full 40-hex hash on the
		// URI line; git-fetch rejects anything shorter.
		packHash := "cec0880cd0d0c86fbce7bf3a45ed9c0e9d2a1b3c"
		fanoutPath := "pack/" + packHash[:2] + "/" + packHash[2:] + ".pack"
		mgr := packfileuri.NewManager(packfileuri.NewSink(&uriTestStore{
			manifest: manifestFor(t, 1, fanoutPath),
		}), logger)

		before := promtestutil.ToFloat64(packObjectsCommands.WithLabelValues("true", "false", "true"))

		var output bytes.Buffer
		stdin := strings.NewReader(head.String() + "\n--not\n\n")
		require.NoError(t, runPackObjects(ctx, gitCmdFactory, &output, req, parseArgs(t), stdin, "cache-key", mgr, logger))

		band1 := collectBand1(t, output.Bytes())
		expectedPack, err := emptyPack(packfileuri.ObjectFormatSHA1)
		require.NoError(t, err)
		require.Equal(t,
			append([]byte(packHash+" https://signed.example/prefix/"+fanoutPath+"\n"), expectedPack...),
			band1,
			"output must be one URI line with the full pack hash, then the empty pack",
		)

		require.GreaterOrEqual(t,
			promtestutil.ToFloat64(packObjectsCommands.WithLabelValues("true", "false", "true"))-before,
			1.0, "the packfile_uri metric label must count the URI-served request")
	})

	t.Run("sha256 manifest serves a sha256 empty pack", func(t *testing.T) {
		packHash := strings.Repeat("ab", 32)
		fanoutPath := "pack/" + packHash[:2] + "/" + packHash[2:] + ".pack"
		mgr := packfileuri.NewManager(packfileuri.NewSink(&uriTestStore{
			manifest: manifestFor(t, 2, fanoutPath),
		}), logger)

		var output bytes.Buffer
		stdin := strings.NewReader(head.String() + "\n--not\n\n")
		require.NoError(t, runPackObjects(ctx, gitCmdFactory, &output, req, parseArgs(t), stdin, "cache-key", mgr, logger))

		band1 := collectBand1(t, output.Bytes())
		expectedPack, err := emptyPack(packfileuri.ObjectFormatSHA256)
		require.NoError(t, err)
		require.True(t, bytes.HasSuffix(band1, expectedPack), "the empty pack's trailer must use the manifest's object format")
	})

	t.Run("uninitialized repository falls back to inline pack-objects", func(t *testing.T) {
		mgr := packfileuri.NewManager(packfileuri.NewSink(&uriTestStore{
			pointerErr: &mvcc.NotInitializedError{RepositoryID: repo.GetRelativePath()},
		}), logger)

		var output bytes.Buffer
		stdin := strings.NewReader(head.String() + "\n--not\n\n")
		require.NoError(t, runPackObjects(ctx, gitCmdFactory, &output, req, parseArgs(t), stdin, "cache-key", mgr, logger))

		band1 := collectBand1(t, output.Bytes())
		require.Equal(t, "PACK", string(band1[:4]), "fallback must stream a real inline pack")
		verifyPackedObjects(t, cfg, repoPath, band1, head.String())
	})

	t.Run("exclusions with dynamic packs disabled serve inline from rewound stdin", func(t *testing.T) {
		mgr := packfileuri.NewManager(packfileuri.NewSink(&uriTestStore{
			manifest: manifestFor(t, 1, "pack/11/11.pack"),
		}), logger)
		require.False(t, mgr.DynamicPacksEnabled())

		var output bytes.Buffer
		// The exclusion scan consumes stdin before git runs; if the rewind
		// were lost, pack-objects would see no "want" line and pack nothing,
		// so asserting head's objects in the pack pins the rewind.
		stdin := strings.NewReader(head.String() + "\n--not\n" + parent.String() + "\n")
		require.NoError(t, runPackObjects(ctx, gitCmdFactory, &output, req, parseArgs(t), stdin, "cache-key", mgr, logger))

		band1 := collectBand1(t, output.Bytes())
		require.Equal(t, "PACK", string(band1[:4]))
		verifyPackedObjects(t, cfg, repoPath, band1, head.String())
	})

	t.Run("depth-limited clone serves inline, never the manifest's full packs", func(t *testing.T) {
		// A v2 depth-limited clone of a whole branch sends --shallow lines
		// with an EMPTY --not section, so exclusion scanning alone would
		// wrongly pick the static path and hand the client every pack in the
		// manifest, ignoring the requested depth.
		mgr := packfileuri.NewManager(packfileuri.NewSink(&uriTestStore{
			manifest: manifestFor(t, 1, "pack/11/11.pack"),
		}), logger)

		var output bytes.Buffer
		stdin := strings.NewReader("--shallow " + head.String() + "\n" + head.String() + "\n--not\n\n")
		require.NoError(t, runPackObjects(ctx, gitCmdFactory, &output, req, parseArgs(t), stdin, "cache-key", mgr, logger))

		band1 := collectBand1(t, output.Bytes())
		require.Equal(t, "PACK", string(band1[:4]), "a shallow negotiation must be served inline")
		verifyPackedObjects(t, cfg, repoPath, band1, head.String())
	})

	t.Run("filtered clone serves inline, never the manifest's full packs", func(t *testing.T) {
		// A partial clone's pack is a computed subset: the static path would
		// hand out unfiltered packs and ignore the filter.
		mgr := packfileuri.NewManager(packfileuri.NewSink(&uriTestStore{
			manifest: manifestFor(t, 1, "pack/11/11.pack"),
		}), logger)

		filterArgs, err := parsePackObjectsArgs([]string{"pack-objects", "--revs", "--thin", "--stdout", "--filter=blob:none", "--uri-protocol=https"})
		require.NoError(t, err)
		require.True(t, filterArgs.hasFilter())

		var output bytes.Buffer
		stdin := strings.NewReader(head.String() + "\n--not\n\n")
		require.NoError(t, runPackObjects(ctx, gitCmdFactory, &output, req, filterArgs, stdin, "cache-key", mgr, logger))

		band1 := collectBand1(t, output.Bytes())
		require.Equal(t, "PACK", string(band1[:4]), "a filtered negotiation must be served inline")
	})

	t.Run("shallow-file argument serves inline", func(t *testing.T) {
		mgr := packfileuri.NewManager(packfileuri.NewSink(&uriTestStore{
			manifest: manifestFor(t, 1, "pack/11/11.pack"),
		}), logger)

		shallowArgs, err := parsePackObjectsArgs([]string{"--shallow-file", "", "pack-objects", "--revs", "--thin", "--stdout", "--uri-protocol=https"})
		require.NoError(t, err)
		require.True(t, shallowArgs.shallowFile)

		var output bytes.Buffer
		stdin := strings.NewReader(head.String() + "\n--not\n\n")
		require.NoError(t, runPackObjects(ctx, gitCmdFactory, &output, req, shallowArgs, stdin, "cache-key", mgr, logger))

		band1 := collectBand1(t, output.Bytes())
		require.Equal(t, "PACK", string(band1[:4]), "a shallow-file request must be served inline")
	})

	t.Run("exclusions with dynamic packs enabled cache a standalone pack", func(t *testing.T) {
		// A haves-bearing negotiation with --thin would normally let
		// pack-objects delta against the client's objects. The materialized
		// pack is cached and later downloaded by clients with different haves,
		// so it must be self-contained: assembleShallowPack drops --thin, and
		// this pins both that the URI path actually served (no silent inline
		// fallback masking an index-pack failure on unresolved deltas) and
		// that the cached pack indexes into an EMPTY repository.
		//
		// The nearly identical large blobs make a thin delta against the have
		// all but certain if --thin were still in effect.
		thinRepo, thinRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		baseContent := bytes.Repeat([]byte("packfileuri dynamic shallow pack filler line\n"), 400)
		haveBlob := gittest.WriteBlob(t, cfg, thinRepoPath, baseContent)
		haveCommit := gittest.WriteCommit(t, cfg, thinRepoPath, gittest.WithBranch("main"), gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "big.txt", Mode: "100644", OID: haveBlob},
		))
		wantBlob := gittest.WriteBlob(t, cfg, thinRepoPath, append(baseContent, []byte("one changed trailing line\n")...))
		wantCommit := gittest.WriteCommit(t, cfg, thinRepoPath, gittest.WithBranch("main"), gittest.WithParents(haveCommit), gittest.WithTreeEntries(
			gittest.TreeEntry{Path: "big.txt", Mode: "100644", OID: wantBlob},
		))

		store := &uriTestStore{manifest: manifestFor(t, 1, "pack/11/11.pack")}
		mgr := packfileuri.NewManager(packfileuri.NewSink(store), logger, packfileuri.WithDynamicPacks())
		require.True(t, mgr.DynamicPacksEnabled())

		thinReq := &gitalypb.PackObjectsHookWithSidechannelRequest{Repository: thinRepo}
		var output bytes.Buffer
		stdin := strings.NewReader(wantCommit.String() + "\n--not\n" + haveCommit.String() + "\n")
		require.NoError(t, runPackObjects(ctx, gitCmdFactory, &output, thinReq, parseArgs(t), stdin, "dyn-cache-key", mgr, logger))

		band1 := collectBand1(t, output.Bytes())
		require.NotEqual(t, "PACK", string(band1[:4]),
			"the dynamic path must serve a URI line, not fall back inline (a fallback here means assembly failed, e.g. index-pack on a thin pack)")
		require.Len(t, store.objects, 1, "the materialized pack must be cached")

		var cached uriStoredObject
		for _, obj := range store.objects {
			cached = obj
		}
		require.NotEmpty(t, cached.metadata["git-pack-hash"])
		require.Contains(t, string(band1), cached.metadata["git-pack-hash"]+" https://signed.example/",
			"the URI line must name the cached pack's hash")

		// The decisive assertion: index the cached pack into an EMPTY
		// repository without --fix-thin. A thin pack (deltas against the
		// have) fails here with unresolved deltas.
		_, scratchPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
		})
		require.Positive(t, binary.BigEndian.Uint32(cached.data[8:12]), "the cached pack must not be empty")
		gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: bytes.NewReader(cached.data)},
			"-C", scratchPath, "index-pack", "--stdin")
		objects := gittest.Exec(t, cfg, "-C", scratchPath, "cat-file", "--batch-check", "--batch-all-objects")
		require.Contains(t, string(objects), wantCommit.String(), "the cached pack must carry the negotiated commit")
		require.Contains(t, string(objects), wantBlob.String(), "the cached pack must carry the changed blob undeltified against the have")
	})
}

// verifyPackedObjects indexes the streamed pack into the repository and
// asserts the wanted commit is among its objects.
func verifyPackedObjects(t *testing.T, cfg config.Cfg, repoPath string, pack []byte, wantOID string) {
	t.Helper()
	// The repository already holds wantOID as a loose object, so cat-file
	// alone cannot tell a real pack from an empty one: check the pack
	// header's object count first. An empty pack here means pack-objects saw
	// no negotiation, i.e. the stdin rewind was lost.
	require.Positive(t, binary.BigEndian.Uint32(pack[8:12]), "an empty pack means pack-objects saw no negotiation, so the stdin rewind was lost")
	gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: bytes.NewReader(pack)},
		"-C", repoPath, "index-pack", "--stdin", "--fix-thin")
	objects := gittest.Exec(t, cfg, "-C", repoPath, "cat-file", "--batch-check", "--batch-all-objects")
	require.Contains(t, string(objects), wantOID, "the inline pack must contain the negotiated commit")
}
