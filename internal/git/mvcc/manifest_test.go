package mvcc

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// realManifestName is the content-addressed name (hex SHA-256 of the payload)
// of testdata/real-manifest.bin, a manifest body produced by real Git via
// `git init --bare --ref-storage-format=mvcc`.
// It only contains references yet, no objects.
const realManifestName = "3f68130d6fd108b66536bee984ea7f575e3d1f9706982913339f42a9caf11a0b"

// chunkSpec is a single chunk (id plus its raw payload) fed to buildManifest.
type chunkSpec struct {
	id   uint32
	data []byte
}

func TestParseManifest_realBinaryManifest(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile(filepath.Join("testdata", "manifest.bin"))
	require.NoError(t, err)

	m, err := ParseManifest(body)
	require.NoError(t, err)

	expectedRefs := []string{
		"refs/e546fa0090bfdaa4a4d632cf241dfb6f411cd36e5c86d4274fe9dd9f85833a70.ref",
	}

	expectedObjs := []string{
		"pack/cec0880cf4323043aeb2268a3eaefdbd0e006c60.idx",
		"pack/cec0880cf4323043aeb2268a3eaefdbd0e006c60.pack",
		"pack/cec0880cf4323043aeb2268a3eaefdbd0e006c60.rev",
	}

	require.Equal(t, byte(hashAlgoSHA1), m.HashAlgo)
	require.Equal(t, expectedRefs, m.RefPaths())
	require.Equal(t, expectedObjs, m.ObjPaths())

	assertManifestSelfNamed(t, body, realManifestName)
}

func TestParseManifest_Full(t *testing.T) {
	t.Parallel()

	original := &Manifest{
		Paths: []string{
			"pack/aaaa.idx",
			"pack/aaaa.pack",
			"pack/aaaa.rev",
			"refs/bbbb.ref",
			"refs/cccc.ref",
		},
		HashAlgo:      hashAlgoSHA1,
		objStart:      0,
		objCount:      3,
		refStackOrder: []string{"refs/cccc.ref", "refs/bbbb.ref"},
	}

	body, sha, err := SerializeManifest(original)
	require.NoError(t, err)
	require.NotEmpty(t, sha)

	// Verify the trailer is the SHA-256 of the payload.
	payloadEnd := len(body) - trailerSize
	want := sha256.Sum256(body[:payloadEnd])
	require.Equal(t, want[:], body[payloadEnd:])

	parsed, err := ParseManifest(body)
	require.NoError(t, err)
	require.Equal(t, original.Paths, parsed.Paths)
	require.Equal(t, byte(hashAlgoSHA1), parsed.HashAlgo)

	// Verify OBJS range and reftable stack order are preserved.
	require.Equal(t, original.objStart, parsed.objStart)
	require.Equal(t, original.objCount, parsed.objCount)
	require.Equal(t, original.refStackOrder, parsed.refStackOrder)
}

func TestParseManifest_Empty(t *testing.T) {
	t.Parallel()

	body, sha, err := SerializeManifest(&Manifest{HashAlgo: hashAlgoSHA1})
	require.NoError(t, err)
	require.NotEmpty(t, sha)

	parsed, err := ParseManifest(body)
	require.NoError(t, err)
	require.Empty(t, parsed.Paths)
}

func TestParseManifest_NilBody(t *testing.T) {
	t.Parallel()

	_, err := ParseManifest(nil)
	require.Error(t, err)
}

func TestParseManifest_ChecksumMismatch(t *testing.T) {
	t.Parallel()

	body := validManifest(t)
	// Flip a payload byte without re-signing, so the trailer no longer matches.
	body[0] ^= 0xFF
	_, err := ParseManifest(body)
	require.ErrorContains(t, err, "checksum mismatch")
}

func TestParseManifest_HeaderValidations(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		mutate  func([]byte)
		wantErr string
	}{
		{"bad magic", func(b []byte) { b[0] = 0 }, "missing magic number"},
		{"unsupported version", func(b []byte) { b[4] = 99 }, "unsupported manifest version"},
		{"zero path width", func(b []byte) { b[6] = 0 }, "PATH record width"},
		{"path width above cap", func(b []byte) { b[6] = maxPathWidth + 1 }, "PATH record width"},
		{"unsupported hash algo", func(b []byte) { b[7] = 9 }, "unsupported hash algorithm"},
		{"too short for declared TOC", func(b []byte) { b[5] = 255 }, "too short for"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body := validManifest(t)
			tc.mutate(body)
			resign(body)

			_, err := ParseManifest(body)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestParseManifest_zeroChunks(t *testing.T) {
	t.Parallel()

	_, err := ParseManifest(buildManifest(t, manifestVersion, 16, hashAlgoSHA1, nil))
	require.ErrorContains(t, err, "at least one chunk")
}

func TestParseManifest_missingPathChunk(t *testing.T) {
	t.Parallel()

	body := buildManifest(t, manifestVersion, 16, hashAlgoSHA1, []chunkSpec{
		{chunkIDObjs, objsChunkData(0, 0)},
		{chunkIDRefs, refsChunkData()},
	})
	_, err := ParseManifest(body)
	require.ErrorContains(t, err, "no PATH chunks found")
}

func TestParseManifest_optionalChunksAbsent(t *testing.T) {
	t.Parallel()

	t.Run("OBJS absent", func(t *testing.T) {
		t.Parallel()

		body := buildManifest(t, manifestVersion, 16, hashAlgoSHA1, []chunkSpec{
			{chunkIDPath, pathChunkData(16, "refs/aaaa.ref")},
			{chunkIDRefs, refsChunkData(0)},
		})
		m, err := ParseManifest(body)
		require.NoError(t, err)
		require.Empty(t, m.ObjPaths())
		require.Equal(t, []string{"refs/aaaa.ref"}, m.RefPaths())
	})

	t.Run("REFS absent", func(t *testing.T) {
		t.Parallel()

		body := buildManifest(t, manifestVersion, 16, hashAlgoSHA1, []chunkSpec{
			{chunkIDPath, pathChunkData(16, "pack/aaaa.pack")},
			{chunkIDObjs, objsChunkData(0, 1)},
		})
		m, err := ParseManifest(body)
		require.NoError(t, err)
		require.Equal(t, []string{"pack/aaaa.pack"}, m.ObjPaths())
		require.Empty(t, m.RefPaths())
	})

	t.Run("only PATH", func(t *testing.T) {
		t.Parallel()

		body := pathOnlyManifest(t, 16, "extra/thing.dat")
		m, err := ParseManifest(body)
		require.NoError(t, err)
		require.Equal(t, []string{"extra/thing.dat"}, m.Paths)
		require.Empty(t, m.ObjPaths())
		require.Empty(t, m.RefPaths())
	})
}

func TestParseManifest_duplicateChunks(t *testing.T) {
	t.Parallel()

	pathData := pathChunkData(16, "pack/aaaa.pack")
	for _, tc := range []struct {
		name    string
		chunks  []chunkSpec
		wantErr string
	}{
		{
			name:    "duplicate PATH",
			chunks:  []chunkSpec{{chunkIDPath, pathData}, {chunkIDPath, pathData}},
			wantErr: "multiple PATH chunks found",
		},
		{
			name:    "duplicate OBJS",
			chunks:  []chunkSpec{{chunkIDPath, pathData}, {chunkIDObjs, objsChunkData(0, 0)}, {chunkIDObjs, objsChunkData(0, 0)}},
			wantErr: "multiple OBJS chunks found",
		},
		{
			name:    "duplicate REFS",
			chunks:  []chunkSpec{{chunkIDPath, pathData}, {chunkIDRefs, refsChunkData()}, {chunkIDRefs, refsChunkData()}},
			wantErr: "multiple REFS chunks found",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseManifest(buildManifest(t, manifestVersion, 16, hashAlgoSHA1, tc.chunks))
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestParseManifest_invalidPath(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		path string
	}{
		{"empty", ""},
		{"leading slash", "/pack/aaaa.pack"},
		{"trailing slash", "pack/aaaa/"},
		{"empty component", "pack//aaaa.pack"},
		{"dot-dot component", "pack/../aaaa"},
		{"disallowed character", "PACK/aaaa.pack"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseManifest(pathOnlyManifest(t, 16, tc.path))
			require.ErrorContains(t, err, "invalid path")
		})
	}
}

func TestManifest_convenienceMethods(t *testing.T) {
	t.Parallel()

	m := &Manifest{
		Paths: []string{
			"pack/abc.idx",
			"pack/abc.pack",
			"pack/abc.rev",
			"refs/def.ref",
			"refs/ghi.ref",
		},
		objStart:      0,
		objCount:      3,
		refStackOrder: []string{"refs/def.ref", "refs/ghi.ref"},
	}

	require.Equal(t, []string{"pack/abc.idx", "pack/abc.pack", "pack/abc.rev"}, m.ObjPaths())
	require.Equal(t, []string{"refs/def.ref", "refs/ghi.ref"}, m.RefPaths())
}

// buildManifest assembles a manifest body from the given header fields and
// chunks and appends a valid SHA-256 trailer. It computes contiguous chunk
// offsets and a zero-ID sentinel but performs no validation, so it can produce
// deliberately malformed bodies for negative tests.
func buildManifest(tb testing.TB, version, pathWidth, hashAlgo byte, chunks []chunkSpec) []byte {
	tb.Helper()

	numChunks := len(chunks)
	tocSize := (numChunks + 1) * tocChunkSize
	dataStart := headerSize + tocSize

	offsets := make([]int, numChunks+1)
	off := dataStart
	for i, c := range chunks {
		offsets[i] = off
		off += len(c.data)
	}
	offsets[numChunks] = off // sentinel points at the end of chunk data

	buf := make([]byte, off+trailerSize)

	// Header: magic(4) + version(1) + num_chunks(1) + path_width(1) + hash_algo(1).
	binary.BigEndian.PutUint32(buf[0:4], manifestMagic)
	buf[4] = version
	buf[5] = byte(numChunks)
	buf[6] = pathWidth
	buf[7] = hashAlgo

	// Chunk lookup table plus trailing zero-ID sentinel.
	for i, c := range chunks {
		row := headerSize + i*tocChunkSize
		binary.BigEndian.PutUint32(buf[row:row+4], c.id)
		binary.BigEndian.PutUint64(buf[row+4:row+tocChunkSize], uint64(offsets[i]))
	}
	sentinel := headerSize + numChunks*tocChunkSize
	binary.BigEndian.PutUint32(buf[sentinel:sentinel+4], 0)
	binary.BigEndian.PutUint64(buf[sentinel+4:sentinel+tocChunkSize], uint64(offsets[numChunks]))

	// Chunk data in TOC order.
	for i, c := range chunks {
		copy(buf[offsets[i]:], c.data)
	}

	trailer := sha256.Sum256(buf[:off])
	copy(buf[off:], trailer[:])
	return buf
}

// resign recomputes the trailing SHA-256 checksum in place after a body has
// been mutated, so tests can target validation past the checksum gate.
func resign(body []byte) {
	payloadEnd := len(body) - trailerSize
	trailer := sha256.Sum256(body[:payloadEnd])
	copy(body[payloadEnd:], trailer[:])
}

func pathChunkData(width int, paths ...string) []byte {
	data := make([]byte, len(paths)*width)
	for i, p := range paths {
		copy(data[i*width:], p)
	}
	return data
}

func objsChunkData(start, count uint32) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:4], start)
	binary.BigEndian.PutUint32(b[4:8], count)
	return b
}

func refsChunkData(indices ...uint32) []byte {
	b := make([]byte, len(indices)*4)
	for i, idx := range indices {
		binary.BigEndian.PutUint32(b[i*4:], idx)
	}
	return b
}

// validManifest returns a well-formed body with PATH, OBJS and REFS chunks that
// ParseManifest accepts. Individual tests mutate it to exercise one failure.
func validManifest(t *testing.T) []byte {
	t.Helper()

	const width = 16
	paths := []string{
		"pack/aaaa.idx",
		"pack/aaaa.pack",
		"pack/aaaa.rev",
		"refs/bbbb.ref",
		"refs/cccc.ref",
	}
	return buildManifest(t, manifestVersion, width, hashAlgoSHA1, []chunkSpec{
		{chunkIDPath, pathChunkData(width, paths...)},
		{chunkIDObjs, objsChunkData(0, 3)},
		{chunkIDRefs, refsChunkData(4, 3)},
	})
}

// pathOnlyManifest returns a body whose only chunk is a single-record PATH.
func pathOnlyManifest(t *testing.T, width int, path string) []byte {
	t.Helper()
	return buildManifest(t, manifestVersion, byte(width), hashAlgoSHA1, []chunkSpec{
		{chunkIDPath, pathChunkData(width, path)},
	})
}

// assertManifestSelfNamed checks the manifest spec's content-addressing
// invariant: the 32-byte trailer is the SHA-256 of everything preceding it, and
// its hex form is the body's filename under manifests/.
func assertManifestSelfNamed(t *testing.T, body []byte, wantName string) {
	t.Helper()
	payload := body[:len(body)-trailerSize]
	sum := sha256.Sum256(payload)
	require.Equal(t, body[len(body)-trailerSize:], sum[:])
	require.Equal(t, wantName, fmt.Sprintf("%x", sum))
}

// TestSerializeManifest_objsMembership: OBJS is written from the manifest's
// own OBJS membership, never inferred from a "pack/" prefix, so the
// commit-graph and multi-pack-index Git keeps in OBJS stay there
// (gitaly#7465). A set that is not one run once sorted cannot be written.
func TestSerializeManifest_objsMembership(t *testing.T) {
	t.Parallel()

	t.Run("round-trips non-pack paths in OBJS", func(t *testing.T) {
		t.Parallel()

		objs := []string{"graph/g.graph", "midx/m.midx", "pack/aaaa.idx", "pack/aaaa.pack"}
		parsed, _ := gitManifest(t, objs, []string{"refs/bbbb.ref"}, []string{"zz/other"})

		body, _, err := SerializeManifest(parsed)
		require.NoError(t, err)
		again, err := ParseManifest(body)
		require.NoError(t, err)
		require.Equal(t, objs, again.ObjPaths())
		require.Equal(t, []string{"refs/bbbb.ref"}, again.RefPaths())
	})

	t.Run("a set that is not one run is an error", func(t *testing.T) {
		t.Parallel()

		// a/1 and c/1 are OBJS; b/1 sorts between them.
		_, _, err := SerializeManifest(&Manifest{
			Paths:    []string{"a/1", "c/1", "b/1"},
			HashAlgo: hashAlgoSHA1,
			objStart: 0,
			objCount: 2,
		})
		require.ErrorIs(t, err, errObjsNotContiguous)
	})
}

// TestParseManifest_unknownChunk: the format belongs to Git and may grow, so
// a chunk Gitaly does not know is accepted and recorded, not rejected.
func TestParseManifest_unknownChunk(t *testing.T) {
	t.Parallel()

	const chunkIDFuture = 0x46555455 // "FUTU"
	m, _ := gitManifest(t, []string{"pack/aaaa.pack"}, nil, nil, chunkSpec{chunkIDFuture, []byte{1, 2, 3, 4}})
	require.Equal(t, []string{"pack/aaaa.pack"}, m.ObjPaths())
	require.Equal(t, []uint32{chunkIDFuture}, m.unknownChunks)
}

func TestAssembleManifest_dedupesAndRejectsRoleConflicts(t *testing.T) {
	t.Parallel()

	t.Run("a path repeated in the same role is deduped", func(t *testing.T) {
		t.Parallel()

		// A reftable both sides of a merge carried can land in the stack
		// twice; it must collapse to one entry in Paths and in REFS.
		m, err := assembleManifest(hashAlgoSHA1, nil,
			[]string{"refs/a.ref", "refs/b.ref", "refs/a.ref"}, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"refs/a.ref", "refs/b.ref"}, m.Paths)
		require.Equal(t, []string{"refs/a.ref", "refs/b.ref"}, m.RefPaths(),
			"REFS stack keeps first occurrence, deduped")
	})

	t.Run("a path in two roles is a conflict", func(t *testing.T) {
		t.Parallel()

		_, err := assembleManifest(hashAlgoSHA1,
			[]string{"pack/x.pack"}, []string{"pack/x.pack"}, nil)
		require.ErrorIs(t, err, errPathRoleConflict)
	})
}
