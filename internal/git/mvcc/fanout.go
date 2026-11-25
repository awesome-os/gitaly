package mvcc

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"time"
)

// The MVCC backend shards its artifacts and manifests into a two-character fanout
// directory named by the first two characters of the entry's content-addressed name.
// An entry named "abcdef..." stored under the "manifests" kind therefore lives at
// "manifests/ab/cdef...", and a pack at "pack/ce/c088...idx". Keeping the kind prefix
// ("pack", "refs", "manifests") means the three artifact kinds never collide.
//
// See Documentation/technical/mvcc-storage.adoc in Git's gitaly-mvcc-backend branch
// for the on-disk layout.

// fanoutPrefixLen is the number of leading characters of an entry's content-addressed
// name that name its fanout directory, under which the MVCC backend shards manifests and
// artifacts both on disk and in object storage.
const fanoutPrefixLen = 2

// fanoutRelPath returns the slash-separated path, relative to the MVCC directory, that
// an entry named name is stored at under the given kind directory. It fails if name is too
// short to have a fanout prefix, which no content-addressed name is.
// scanManifestDir is the inverse of this mapping and must be kept in step with it.
func fanoutRelPath(kind, name string) (string, error) {
	if len(name) < fanoutPrefixLen {
		return "", fmt.Errorf("%s name %q is shorter than the %d-character fanout prefix", kind, name, fanoutPrefixLen)
	}

	return path.Join(kind, name[:fanoutPrefixLen], name[fanoutPrefixLen:]), nil
}

// ManifestRelPath returns the fanout-sharded path a manifest with the given hash is
// stored at, relative to the MVCC directory ("manifests/<hash[:2]>/<hash[2:]>"). It is
// the canonical manifest layout: object-storage backends join it onto their repository
// prefix so a manifest resolves to the same relative location on disk and in the store.
// It fails if hash is too short to shard.
func ManifestRelPath(hash string) (string, error) {
	return fanoutRelPath(manifestDirectory, hash)
}

// manifestFile identifies a manifest found below the fanout manifest directory: its
// content-addressed hash, reconstructed across the fanout, and the modification time
// observed while scanning.
type manifestFile struct {
	hash    string
	modTime time.Time
}

// scanManifestDir lists the manifests stored below manifestsDir. It is the inverse of
// fanoutRelPath: a manifest lives at "<hash[:2]>/<hash[2:]>", so this reads one level of
// fanout directories and rejoins the prefix with the remainder. Anything that is not a file
// inside a fanout directory is not a manifest and is ignored. Entries that disappear
// mid-scan are skipped, as a concurrent pruning run removing one is ordinary.
func scanManifestDir(manifestsDir string) ([]manifestFile, error) {
	fanoutDirs, err := os.ReadDir(manifestsDir)
	if err != nil {
		return nil, err
	}

	var manifests []manifestFile
	for _, fanoutDir := range fanoutDirs {
		// Only a directory named by a full fanout prefix can hold manifests. Any other
		// name would not re-shard to the path the entry was found at.
		if !fanoutDir.IsDir() || len(fanoutDir.Name()) != fanoutPrefixLen {
			continue
		}

		prefix := fanoutDir.Name()
		entries, err := os.ReadDir(filepath.Join(manifestsDir, prefix))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, err
		}

		for _, entry := range entries {
			// A directory nested below the fanout is not a manifest.
			if entry.IsDir() {
				continue
			}

			info, err := entry.Info()
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				return nil, fmt.Errorf("statting %q: %w", entry.Name(), err)
			}

			manifests = append(manifests, manifestFile{hash: prefix + entry.Name(), modTime: info.ModTime()})
		}
	}

	return manifests, nil
}
