package housekeeping

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	gitmvcc "gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/helper/duration"
)

// MVCCConfig specifies MVCC-specific housekeeping configuration.
type MVCCConfig struct {
	// ManifestsToKeep controls how many manifests should be retained, starting from
	// the most recent.
	ManifestsToKeep uint
	// ManifestMinAge specifies how old a manifest must be (in terms of
	// its mtime) to be eligible for pruning.
	ManifestMinAge duration.Duration
	// ArtifactMinAge specifies how old an artifact no manifest references must be to be
	// eligible for removal.
	ArtifactMinAge duration.Duration
	// MaxLockDuration is the budget for what a run does while holding the repository lock.
	// Entries left over when it runs out are collected by the next run.
	MaxLockDuration duration.Duration
	// MaxAcquireWait is how long a run may wait for the repository lock before giving up.
	MaxAcquireWait duration.Duration
}

// DefaultMVCCConfig returns the default MVCC housekeeping configuration. We keep the 50 latest
// manifests, since it's unlikely that clients will be actively operating on a manifest older
// than that. Additionally, we only prune a manifest if it's at least 24 hours old, which is
// far longer than any RPC could be running.
//
// The lock budget only bounds a run that nothing asks to stop, as an arriving write evicts one.
// Ten seconds lets a sizeable backlog drain in a few runs while still bounding the hold when there
// is no write to evict it.
func DefaultMVCCConfig() MVCCConfig {
	return MVCCConfig{
		ManifestsToKeep: 50,
		ManifestMinAge:  duration.Duration(24 * time.Hour),
		ArtifactMinAge:  duration.Duration(24 * time.Hour),
		MaxLockDuration: duration.Duration(10 * time.Second),
		MaxAcquireWait:  duration.Duration(time.Second),
	}
}

// CleanMVCCMigrationLeftovers removes files left by an interrupted migration to
// the MVCC backend.
func CleanMVCCMigrationLeftovers(repoPath string) error {
	// The pre-migration store and refs/, and any staging directories.
	stageMatches, err := filepath.Glob(filepath.Join(repoPath, gitmvcc.MigrationStageGlob))
	if err != nil {
		return fmt.Errorf("globbing for staging directories: %w", err)
	}

	for _, path := range append([]string{
		filepath.Join(repoPath, "packed-refs"),
		filepath.Join(repoPath, "reftable"),
		filepath.Join(repoPath, gitmvcc.MigrationOldRefsDir),
	}, stageMatches...) {
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return fmt.Errorf("stat %q: %w", filepath.Base(path), err)
		}

		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("removing %q: %w", filepath.Base(path), err)
		}
	}

	// refs/ has to stay: Gitaly's validation requires it, and Git keeps a
	// refs/heads stub file in it. Anything else in it is files-backend content
	// restored after an interrupted swap. It's removed in place, as moving refs/
	// itself would briefly fail validation for concurrent RPCs.
	refsPath := filepath.Join(repoPath, "refs")
	entries, err := os.ReadDir(refsPath)
	if err != nil {
		return fmt.Errorf("reading refs: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() == "heads" && entry.Type().IsRegular() {
			continue
		}
		path := filepath.Join(refsPath, entry.Name())

		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("removing stale refs content: %w", err)
		}
	}

	return nil
}
