// Package mvcc (Multi-Version Concurrency Control) contains the components used within Gitaly
// to create and manage Git repositories using the MVCC backend.
package mvcc

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// The manifest is expressed in the format specified by gitformat-chunk(5). These
// constants are specific to the manifest format. Hex values are big-endian.
//
// See Documentation/technical/mvcc-storage.adoc in Git's gitaly-mvcc-backend branch
// for more information.
const (
	// manifestMagic identifies the file as an MVCC manifest ("MVCC" in ASCII).
	manifestMagic = 0x4D564343
	// manifestVersion is the format version. Readers MUST reject unknown versions.
	manifestVersion = 1
	// maxPathWidth is the upper bound on the fixed-width PATH record size.
	maxPathWidth = 128

	// hashAlgoSHA1 and hashAlgoSHA256 identify the object hash algorithm
	// used by the repository.
	hashAlgoSHA1   = 1
	hashAlgoSHA256 = 2

	// The manifest format declares three kinds of chunks.
	chunkIDPath = 0x50415448
	chunkIDObjs = 0x4F424A53
	chunkIDRefs = 0x52454653

	// trailerSize is the length of the trailing SHA-256 checksum.
	trailerSize = 32

	// headerSize is the size of the manifest header
	headerSize = 8

	tocChunkSize = 12
)

// Manifest is the parsed form of an MVCC manifest body. A manifest file is
// a binary file that contains paths to locations on disk of Git artifacts,
// the sum of which is a repository's persistent state.
//
// The binary format carries three chunks — PATH, OBJS, REFS — that Gitaly
// uses to categorise artifacts for phased downloads: refs first (so
// ref-reading RPCs can proceed quickly), then packs, then anything else.
//
// On-disk layout (gitformat-chunk(5)):
//
//	┌────────────────────────────────────────────────────────────┐
//	│ HEADER  8B   magic "MVCC", version, numChunks, pathWidth,  │
//	│              hashAlgo                                      │
//	├────────────────────────────────────────────────────────────┤
//	│ TOC          PATH -> offset                                │
//	│  (numChunks  OBJS -> offset                                │
//	│   +1 rows,   REFS -> offset                                │
//	│   12B each)  0    -> endOffset   (sentinel; bounds the     │
//	│                                  last chunk)               │
//	├────────────────────────────────────────────────────────────┤
//	│ PATH         every artifact path, sorted, as fixed-width   │
//	│              (pathWidth) records: the one source of truth  │
//	│              for path strings                              │
//	├────────────────────────────────────────────────────────────┤
//	│ OBJS         the contiguous run of PATH records that are   │
//	│              object-storage artifacts (packs, midx, graph) │
//	├────────────────────────────────────────────────────────────┤
//	│ REFS         the reftable records in stack order (oldest   │
//	│              first); newer tables shadow older ones        │
//	├────────────────────────────────────────────────────────────┤
//	│ TRAILER 32B  SHA-256 of everything above                   │
//	└────────────────────────────────────────────────────────────┘
//
// OBJS and REFS do not repeat path strings; they index into PATH, which is
// why OBJS must be a single contiguous run (see objsRange) and why the
// merge rewrites PATH whenever it changes membership.
type Manifest struct {
	// Paths holds every relative artifact path in the snapshot, in
	// lexicographic order. Examples: "pack/abcdef.pack", "refs/012345.ref".
	Paths []string
	// HashAlgo identifies the repository's object hash algorithm
	// (1 = SHA-1, 2 = SHA-256).
	HashAlgo byte

	// objStart and objCount define the contiguous slice
	// Paths[objStart:objStart+objCount] identified by the OBJS chunk
	// as object-storage artifacts.
	objStart int
	objCount int

	// refStackOrder holds reftable paths in stack order (oldest first),
	// as identified by the REFS chunk. Stack order is semantically
	// significant: newer reftables shadow older ones.
	refStackOrder []string

	// unknownChunks holds the IDs of chunks other than PATH, OBJS and
	// REFS, in TOC order. Parsing accepts them, since the format belongs
	// to Git and may grow, but SerializeManifest does not write them.
	unknownChunks []uint32
}

type manifestHeader struct {
	magic     []byte
	version   byte
	numChunks int
	pathWidth int
	hashAlgo  byte
}

type manifestChunk struct {
	id     uint32
	offset int
	size   int
}

type manifestChunks struct {
	chunks  []manifestChunk
	pathIdx int
	objsIdx int
	refsIdx int
}

func (c *manifestChunks) AtIndex(i int) (manifestChunk, error) {
	if i < 0 || i >= len(c.chunks) {
		return manifestChunk{}, errors.New("chunk index out of range")
	}
	return c.chunks[i], nil
}

// NewManifest builds a Manifest naming objPaths as the OBJS artifacts (the
// pack .idx/.pack/.rev triples, midx and commit-graph) and refPaths as the
// reftables in stack order (oldest first). It is a thin public wrapper around
// assembleManifest, which the merge path uses to build manifests with the same
// invariants (lexicographic path order, contiguous OBJS range, deduplicated
// ref stack).
func NewManifest(objPaths, refPaths []string, hashAlgo byte) (*Manifest, error) {
	return assembleManifest(hashAlgo, objPaths, refPaths, nil)
}

// ObjRange returns the start index and count of the contiguous range of Paths
// identified by the OBJS chunk as object-storage artifacts.
func (m *Manifest) ObjRange() (start, count int) {
	return m.objStart, m.objCount
}

// ObjPaths returns the paths identified by the OBJS chunk as object-storage
// artifacts (pack .idx/.pack/.rev triples).
func (m *Manifest) ObjPaths() []string {
	if m.objCount == 0 {
		return nil
	}
	return m.Paths[m.objStart : m.objStart+m.objCount]
}

// RefPaths returns the reftable paths identified by the REFS chunk, in stack
// order (oldest first).
func (m *Manifest) RefPaths() []string {
	return m.refStackOrder
}

// OBJS artifact kinds, distinguished by their path prefix. This describes the
// target MVCC object store, where Git stores a snapshot's multi-pack-index and
// commit-graph in OBJS next to the packs and reads them from there. The Git
// pinned in GIT_VERSION_MVCC only copies OBJS forward; it still reads objects,
// midx and commit-graph from objects/.
const (
	packPrefix  = "pack/"
	midxPrefix  = "midx/"
	graphPrefix = "graph/"
)

// PackPaths returns the OBJS paths under "pack/".
func (m *Manifest) PackPaths() []string {
	var out []string
	for _, p := range m.ObjPaths() {
		if strings.HasPrefix(p, packPrefix) {
			out = append(out, p)
		}
	}
	return out
}

// GraphPath returns the single OBJS path under "graph/", or "" if there is
// none. Like the target object store's reader, it errors if the manifest names
// more than one; the merge calls it only when every side has at most one. The
// midx has no such accessor: mergeMidx reads the midx/ set directly, since a
// midx chain is several paths.
func (m *Manifest) GraphPath() (string, error) {
	return m.objPathWithPrefix(graphPrefix)
}

func (m *Manifest) objPathWithPrefix(prefix string) (string, error) {
	var found string
	for _, p := range m.ObjPaths() {
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		if found != "" {
			return "", fmt.Errorf("mvcc: manifest names more than one %q artifact", prefix)
		}
		found = p
	}
	return found, nil
}

// errObjsNotContiguous is returned when the paths that belong in OBJS do not
// form a single run once PATH is sorted. The OBJS chunk is one range, so such
// a set cannot be written.
var errObjsNotContiguous = errors.New("OBJS paths are not contiguous in PATH")

// errPathRoleConflict is returned when assembleManifest is given the same
// path in more than one role, e.g. both in OBJS and outside it.
var errPathRoleConflict = errors.New("path given in more than one role")

// assembleManifest builds a Manifest from its parts: objPaths become the OBJS
// range, refStack the REFS stack (oldest first), and otherPaths sit in PATH
// outside both. Membership is taken as given, never inferred from a path's
// prefix.
//
// A path repeated within the same role is deduplicated, not rejected: paths are
// content-addressed, so a repeat is the identical artifact. This happens when
// both sides of a merge carried the same reftable and it rebased onto the same
// bytes, which would otherwise land twice in the stack. A path given in two
// different roles is a genuine conflict and returns errPathRoleConflict.
func assembleManifest(hashAlgo byte, objPaths, refStack, otherPaths []string) (*Manifest, error) {
	n := len(objPaths) + len(refStack) + len(otherPaths)
	roles := make(map[string]int, n)
	paths := make([]string, 0, n)
	for role, group := range [][]string{objPaths, refStack, otherPaths} {
		for _, p := range group {
			if r, ok := roles[p]; ok {
				if r != role {
					return nil, fmt.Errorf("%w: %q", errPathRoleConflict, p)
				}
				// Same path, same role: content-addressed, so keep it once.
				continue
			}
			roles[p] = role
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)

	objStart, objCount, err := objsRange(paths, objPaths)
	if err != nil {
		return nil, err
	}

	// Dedup the ref stack too, preserving order (first occurrence wins), so a
	// table both sides carried does not appear twice in REFS.
	refStackOrder := make([]string, 0, len(refStack))
	seen := make(map[string]struct{}, len(refStack))
	for _, p := range refStack {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		refStackOrder = append(refStackOrder, p)
	}

	return &Manifest{
		Paths:         paths,
		HashAlgo:      hashAlgo,
		objStart:      objStart,
		objCount:      objCount,
		refStackOrder: refStackOrder,
	}, nil
}

// objsRange returns the start and length of the run objPaths occupy in the
// sorted paths, or errObjsNotContiguous when they do not form a single run.
func objsRange(sortedPaths, objPaths []string) (int, int, error) {
	if len(objPaths) == 0 {
		return 0, 0, nil
	}
	inObjs := make(map[string]struct{}, len(objPaths))
	for _, p := range objPaths {
		inObjs[p] = struct{}{}
	}
	isObj := func(p string) bool {
		_, ok := inObjs[p]
		return ok
	}

	start := slices.IndexFunc(sortedPaths, isObj)
	if start == -1 {
		return 0, 0, fmt.Errorf("%w: none of them is in PATH", errObjsNotContiguous)
	}
	end := start
	for end < len(sortedPaths) && isObj(sortedPaths[end]) {
		end++
	}
	if rest := slices.IndexFunc(sortedPaths[end:], isObj); rest != -1 {
		return 0, 0, fmt.Errorf("%w: %q sorts inside the run", errObjsNotContiguous, sortedPaths[end])
	}
	return start, end - start, nil
}

// ParseManifest parses an MVCC manifest body in binary chunk-based format
// (starting with "MVCC" magic).
func ParseManifest(data []byte) (*Manifest, error) {
	if len(data) == 0 {
		return &Manifest{}, fmt.Errorf("empty manifest")
	}

	if len(data) < headerSize+trailerSize {
		return &Manifest{}, fmt.Errorf("binary manifest too short (%d bytes)", len(data))
	}

	// Validate trailing SHA-256 checksum.
	payloadEnd := len(data) - trailerSize
	want := sha256.Sum256(data[:payloadEnd])
	if !bytes.Equal(want[:], data[payloadEnd:]) {
		return nil, fmt.Errorf("manifest checksum mismatch")
	}

	header, err := parseManifestHeader(data)
	if err != nil {
		return nil, err
	}

	chunks, err := parseChunks(data, header.numChunks)
	if err != nil {
		return nil, err
	}

	m := &Manifest{HashAlgo: header.hashAlgo}

	// Process PATH chunk
	pathChunk, err := chunks.AtIndex(chunks.pathIdx)
	if err != nil {
		return nil, err
	}

	pw := header.pathWidth
	if pathChunk.size%pw != 0 {
		return nil, fmt.Errorf("PATH chunk size %d not divisible by width %d", pathChunk.size, pw)
	}

	numPath := pathChunk.size / pw
	m.Paths = make([]string, numPath)
	for i := range numPath {
		start := pathChunk.offset + i*pw
		record := data[start : start+pw]
		end := bytes.IndexByte(record, 0)
		if end == -1 {
			return nil, fmt.Errorf("PATH record %d is not NUL-terminated", i)
		}
		p := string(record[:end])
		if !isValidManifestPath(p) {
			return nil, fmt.Errorf("manifest contains invalid path %q", p)
		}
		m.Paths[i] = p
	}

	// Process OBJS chunk if present.
	objsChunk, err := chunks.AtIndex(chunks.objsIdx)
	if err == nil {
		if objsChunk.size != 8 {
			return nil, fmt.Errorf("OBJS chunk has unexpected size %d (want 8)", objsChunk.size)
		}
		m.objStart = int(binary.BigEndian.Uint32(data[objsChunk.offset : objsChunk.offset+4]))
		m.objCount = int(binary.BigEndian.Uint32(data[objsChunk.offset+4 : objsChunk.offset+8]))
		if m.objStart+m.objCount > len(m.Paths) {
			return nil, fmt.Errorf("OBJS range [%d, %d) out of bounds (have %d paths)", m.objStart, m.objStart+m.objCount, len(m.Paths))
		}
	}

	// Process REFS chunk if present.
	refsChunk, err := chunks.AtIndex(chunks.refsIdx)
	if err == nil {
		if refsChunk.size%4 != 0 {
			return nil, fmt.Errorf("REFS chunk size %d not divisible by 4", refsChunk.size)
		}
		numRefs := refsChunk.size / 4
		m.refStackOrder = make([]string, numRefs)
		for i := range numRefs {
			idx := binary.BigEndian.Uint32(data[refsChunk.offset+i*4 : refsChunk.offset+i*4+4])
			if int(idx) >= len(m.Paths) {
				return nil, fmt.Errorf("REFS index %d out of bounds (have %d paths)", idx, len(m.Paths))
			}
			m.refStackOrder[i] = m.Paths[idx]
		}
	}

	for _, c := range chunks.chunks {
		switch c.id {
		case chunkIDPath, chunkIDObjs, chunkIDRefs:
		default:
			m.unknownChunks = append(m.unknownChunks, c.id)
		}
	}

	return m, nil
}

// isValidManifestPath mirrors Git's is_valid_manifest_path(): non-empty,
// no leading slash, no "..", no "//", only [a-z0-9/.].
func isValidManifestPath(p string) bool {
	if p == "" || p[0] == '/' || p[len(p)-1] == '/' {
		return false
	}
	if strings.Contains(p, "..") || strings.Contains(p, "//") {
		return false
	}
	for _, c := range p {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '/' || c == '.') {
			return false
		}
	}
	return true
}

func parseManifestHeader(data []byte) (manifestHeader, error) {
	hdr := manifestHeader{
		magic:     data[:4],
		version:   data[4],
		numChunks: int(data[5]),
		pathWidth: int(data[6]),
		hashAlgo:  data[7],
	}

	if binary.BigEndian.Uint32(hdr.magic) != manifestMagic {
		return manifestHeader{}, errors.New("invalid manifest; missing magic number in header")
	}

	if hdr.version != manifestVersion {
		return manifestHeader{}, fmt.Errorf("unsupported manifest version %d", hdr.version)
	}

	if hdr.pathWidth == 0 || hdr.pathWidth > 128 {
		return manifestHeader{}, fmt.Errorf("manifest PATH record width must be between 0 and 128")
	}
	if hdr.hashAlgo != hashAlgoSHA1 && hdr.hashAlgo != hashAlgoSHA256 {
		return manifestHeader{}, fmt.Errorf("unsupported hash algorithm %d", hdr.hashAlgo)
	}

	tocSize := (hdr.numChunks + 1) * 12
	if len(data) < headerSize+tocSize+trailerSize {
		return manifestHeader{}, fmt.Errorf("manifest too short for %d-chunk TOC (%d bytes)", hdr.numChunks, len(data))
	}

	return hdr, nil
}

func parseChunks(data []byte, numChunks int) (*manifestChunks, error) {
	if numChunks <= 0 {
		return nil, errors.New("there must be at least one chunk besides the null terminator")
	}

	tocTerminator := headerSize + numChunks*12
	if binary.BigEndian.Uint32(data[tocTerminator:tocTerminator+4]) != 0 {
		return nil, fmt.Errorf("expected zero sentinel in TOC")
	}

	dataEnd := len(data) - trailerSize

	result := &manifestChunks{
		chunks:  make([]manifestChunk, numChunks),
		pathIdx: -1,
		objsIdx: -1,
		refsIdx: -1,
	}

	for i := range numChunks {
		// The TOC starts after the header
		chunkOffset := headerSize + i*tocChunkSize
		chunkID := binary.BigEndian.Uint32(data[chunkOffset : chunkOffset+4])
		chunkOffsetInFile := int(binary.BigEndian.Uint64(data[chunkOffset+4 : chunkOffset+tocChunkSize]))

		// Calculate the size of the chunk
		nextChunkStart := headerSize + (i+1)*tocChunkSize
		nextChunkOffsetInFile := int(binary.BigEndian.Uint64(data[nextChunkStart+4 : nextChunkStart+tocChunkSize]))

		if chunkOffsetInFile > dataEnd || nextChunkOffsetInFile > dataEnd || nextChunkOffsetInFile < chunkOffsetInFile {
			return nil, fmt.Errorf("chunk %d has out-of-bounds offsets [%d, %d)", i, chunkOffsetInFile, nextChunkOffsetInFile)
		}

		chunkSize := nextChunkOffsetInFile - chunkOffsetInFile

		result.chunks[i] = manifestChunk{
			id:     chunkID,
			offset: chunkOffsetInFile,
			size:   chunkSize,
		}

		switch chunkID {
		case chunkIDObjs:
			if result.objsIdx != -1 {
				return nil, fmt.Errorf("multiple OBJS chunks found")
			}
			result.objsIdx = i
		case chunkIDRefs:
			if result.refsIdx != -1 {
				return nil, fmt.Errorf("multiple REFS chunks found")
			}
			result.refsIdx = i

		case chunkIDPath:
			if result.pathIdx != -1 {
				return nil, fmt.Errorf("multiple PATH chunks found")
			}
			result.pathIdx = i
		}
	}

	// A PATH chunk is required so we validate here that one exist.
	// OBJS and REFS path are optional, so we don't validate them.
	if result.pathIdx == -1 {
		return nil, fmt.Errorf("no PATH chunks found")
	}

	return result, nil
}

// SerializeManifest writes a binary chunk-based manifest body. Returns the
// complete body (including the 32-byte SHA-256 trailer) and the hex trailer
// hash.
//
// OBJS is written from m's own OBJS membership (ObjPaths), whatever the
// paths' prefixes, and errObjsNotContiguous is returned when those paths do
// not form one run once PATH is sorted. Chunks other than PATH, OBJS and REFS
// are not written. The trailer hash is the content-addressed filename under which Git
// and Gitaly store the manifest body in manifests/hash[:2]/hash[2:].
func SerializeManifest(m *Manifest) ([]byte, string, error) {
	paths := make([]string, len(m.Paths))
	copy(paths, m.Paths)
	sort.Strings(paths)

	// Compute PATH record width: longest path + NUL terminator, rounded up
	// to a 4-byte boundary. Minimum is 4 (even for empty manifests).
	maxLen := 0
	for _, p := range paths {
		if len(p) > maxLen {
			maxLen = len(p)
		}
	}
	pathWidth := (maxLen + 1 + 3) & ^3
	if pathWidth < 4 {
		pathWidth = 4
	}
	if pathWidth > maxPathWidth {
		return nil, "", fmt.Errorf("path width %d exceeds maximum %d", pathWidth, maxPathWidth)
	}

	hashAlgo := m.HashAlgo
	if hashAlgo == 0 {
		hashAlgo = hashAlgoSHA1
	}

	pathIndex := make(map[string]int, len(paths))
	for i, p := range paths {
		pathIndex[p] = i
	}

	// OBJS: m's OBJS paths, located in the sorted path list.
	objStart, objCount, err := objsRange(paths, m.ObjPaths())
	if err != nil {
		return nil, "", err
	}

	// REFS: indices into sorted paths, in reftable stack order.
	var refIndices []int
	for _, rp := range m.refStackOrder {
		if idx, ok := pathIndex[rp]; ok {
			refIndices = append(refIndices, idx)
		}
	}

	// Build chunk payloads.
	pathData := make([]byte, len(paths)*pathWidth)
	for i, p := range paths {
		copy(pathData[i*pathWidth:], p)
	}

	objsData := make([]byte, 8)
	binary.BigEndian.PutUint32(objsData[0:4], uint32(objStart))
	binary.BigEndian.PutUint32(objsData[4:8], uint32(objCount))

	refsData := make([]byte, len(refIndices)*4)
	for i, idx := range refIndices {
		binary.BigEndian.PutUint32(refsData[i*4:], uint32(idx))
	}

	// Compute absolute byte offsets for each chunk.
	const numChunks = 3 // PATH, OBJS, REFS — always emitted in v1
	headerSize := 8
	tocSize := (numChunks + 1) * 12
	dataStart := uint64(headerSize + tocSize)
	pathOff := dataStart
	objsOff := pathOff + uint64(len(pathData))
	refsOff := objsOff + uint64(len(objsData))
	endOff := refsOff + uint64(len(refsData))

	buf := make([]byte, 0, int(endOff)+trailerSize)

	// Header: magic(4) + version(1) + num_chunks(1) + path_width(1) + hash_algo(1).
	hdr := [8]byte{}
	binary.BigEndian.PutUint32(hdr[0:4], manifestMagic)
	hdr[4] = manifestVersion
	hdr[5] = numChunks
	hdr[6] = byte(pathWidth)
	hdr[7] = hashAlgo
	buf = append(buf, hdr[:]...)

	// Chunk lookup table: (C+1) rows of (4-byte ID + 8-byte offset).
	// Final row is the zero-ID sentinel marking the end of chunk data.
	toc := make([]byte, tocSize)
	binary.BigEndian.PutUint32(toc[0:4], chunkIDPath)
	binary.BigEndian.PutUint64(toc[4:12], pathOff)
	binary.BigEndian.PutUint32(toc[12:16], chunkIDObjs)
	binary.BigEndian.PutUint64(toc[16:24], objsOff)
	binary.BigEndian.PutUint32(toc[24:28], chunkIDRefs)
	binary.BigEndian.PutUint64(toc[28:36], refsOff)
	binary.BigEndian.PutUint32(toc[36:40], 0) // sentinel
	binary.BigEndian.PutUint64(toc[40:48], endOff)
	buf = append(buf, toc...)

	// Chunk data in TOC order: PATH, OBJS, REFS.
	buf = append(buf, pathData...)
	buf = append(buf, objsData...)
	buf = append(buf, refsData...)

	// Trailing SHA-256 checksum. Its hex form doubles as the content-addressed
	// filename (manifests/hash[:2]/hash[2:]).
	trailer := sha256.Sum256(buf)
	buf = append(buf, trailer[:]...)

	return buf, fmt.Sprintf("%x", trailer), nil
}
