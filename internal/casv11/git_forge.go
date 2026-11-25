package casv11

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// RepositoryProtocol is the canonical protocol identifier for GitForge tree manifests in CasV11Store.
const RepositoryProtocol = "repository://"

var (
	// ErrInvalidGitForgeURI is returned when a repository:// or git-forge:// URI is malformed or missing required components.
	ErrInvalidGitForgeURI = errors.New("casv11: invalid repository URI")
	// ErrPathTraversal is returned when a URI or manifest entry attempts directory traversal.
	ErrPathTraversal = errors.New("casv11: path traversal rejected")
	// ErrLoopbackForbidden is returned when a URI targets a loopback host (B8G016).
	ErrLoopbackForbidden = errors.New("casv11: loopback host forbidden (B8G016)")
	// ErrInvalidManifest is returned when a stored GitForgeTreeManifest is malformed or has an unexpected ABI magic.
	ErrInvalidManifest = errors.New("casv11: invalid gitforge tree manifest")
)

// GitForgeRef identifies a repository tree (and optional subpath, lifecycle hook, and target)
// addressed via the canonical repository:// URI scheme (or legacy git-forge:// alias).
type GitForgeRef struct {
	Host    string `json:"host"`
	Org     string `json:"org"`
	Repo    string `json:"repo"`
	Ref     string `json:"ref"`
	Subpath string `json:"subpath,omitempty"`
	Hook    string `json:"hook,omitempty"`
	Target  string `json:"target,omitempty"`
}

// GitForgeTreeEntry describes a single content-addressed file in a GitForge tree snapshot.
type GitForgeTreeEntry struct {
	Path       string `json:"path"`
	SHA512     string `json:"sha512"`
	Size       int64  `json:"size"`
	Executable bool   `json:"executable"`
}

// GitForgeTreeManifest is the deterministic JSON manifest stored in CasV11Store for a GitForge tree snapshot.
type GitForgeTreeManifest struct {
	Protocol string              `json:"protocol"`
	ABIMagic uint32              `json:"abi_magic"`
	Ref      GitForgeRef         `json:"ref"`
	Entries  []GitForgeTreeEntry `json:"entries"`
}

// ParseGitForgeURI parses and validates a canonical repository:// URI (or legacy git-forge:// alias):
//
//	repository://<host>/<org>/<repo>[@<ref>][#<subpath>][?hook=<h>&target=<t>]
//
// Both `?#` and `#?` orderings for query parameters and fragment subpaths are supported,
// as well as path-style subpaths (`/<org>/<repo>@<ref>/<subpath>`).
// It rejects directory traversal ("..") and loopback hosts (localhost, 127.0.0.1, ::1, 0.0.0.0) per B8G016.
func ParseGitForgeURI(raw string) (*GitForgeRef, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: empty URI", ErrInvalidGitForgeURI)
	}
	if strings.ContainsRune(trimmed, '\x00') || strings.ContainsRune(trimmed, '\\') {
		return nil, fmt.Errorf("%w: forbidden character in URI", ErrPathTraversal)
	}

	u, err := url.Parse(trimmed)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidGitForgeURI, err)
	}

	switch strings.ToLower(u.Scheme) {
	case "repository", "git-forge":
	default:
		return nil, fmt.Errorf("%w: unsupported scheme %q (expected repository:// or git-forge://)", ErrInvalidGitForgeURI, u.Scheme)
	}

	if u.Host == "" {
		return nil, fmt.Errorf("%w: missing host", ErrInvalidGitForgeURI)
	}

	hostname := strings.ToLower(u.Hostname())
	if isForbiddenLoopbackHost(hostname) {
		return nil, fmt.Errorf("%w: host %q is not allowed", ErrLoopbackForbidden, hostname)
	}

	// Check both raw and decoded path for traversal segments before cleaning.
	if hasTraversalSegment(u.Path) || (u.RawPath != "" && hasTraversalSegment(u.RawPath)) {
		return nil, fmt.Errorf("%w: path %q contains '..'", ErrPathTraversal, u.Path)
	}

	trimmedPath := strings.Trim(u.Path, "/")
	if trimmedPath == "" {
		return nil, fmt.Errorf("%w: missing org and repo", ErrInvalidGitForgeURI)
	}

	parts := strings.Split(trimmedPath, "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: expected /<org>/<repo>", ErrInvalidGitForgeURI)
	}

	org := parts[0]
	repoPart := parts[1]
	if err := validateIdentifierSegment("org", org); err != nil {
		return nil, err
	}

	repo := repoPart
	ref := ""
	if atIdx := strings.IndexByte(repoPart, '@'); atIdx >= 0 {
		repo = repoPart[:atIdx]
		ref = repoPart[atIdx+1:]
	}
	repo = strings.TrimSuffix(repo, ".git")
	if err := validateIdentifierSegment("repo", repo); err != nil {
		return nil, err
	}

	subpath := ""
	if len(parts) > 2 {
		subpath = strings.Join(parts[2:], "/")
	}

	// Support both `repository://...#<subpath>?hook=...&target=...` and
	// `repository://...?hook=...&target=...#<subpath>`.
	q := u.Query()
	fragment := strings.TrimSpace(u.Fragment)
	if fragment != "" {
		if qIdx := strings.IndexByte(fragment, '?'); qIdx >= 0 {
			fragQuery, err := url.ParseQuery(fragment[qIdx+1:])
			if err != nil {
				return nil, fmt.Errorf("%w: invalid query %q: %v", ErrInvalidGitForgeURI, fragment[qIdx+1:], err)
			}
			for k, vals := range fragQuery {
				if _, exists := q[k]; !exists {
					q[k] = vals
				}
			}
			fragment = strings.TrimSpace(fragment[:qIdx])
		}
	}

	if qRef := strings.TrimSpace(q.Get("ref")); qRef != "" && ref == "" {
		ref = qRef
	}
	if ref == "" {
		ref = "HEAD"
	}
	if hasTraversalSegment(ref) || strings.ContainsRune(ref, '\\') {
		return nil, fmt.Errorf("%w: ref %q contains traversal", ErrPathTraversal, ref)
	}

	if fragment != "" {
		fragTrimmed := strings.TrimPrefix(fragment, "/")
		if subpath != "" {
			subpath = subpath + "/" + fragTrimmed
		} else {
			subpath = fragTrimmed
		}
	}
	if qSubpath := strings.TrimSpace(q.Get("subpath")); qSubpath != "" {
		if subpath != "" {
			subpath = subpath + "/" + qSubpath
		} else {
			subpath = qSubpath
		}
	}
	if subpath != "" {
		cleanSub, err := validateRelativeSubpath(subpath)
		if err != nil {
			return nil, err
		}
		subpath = cleanSub
	}

	hook := strings.TrimSpace(q.Get("hook"))
	target := strings.TrimSpace(q.Get("target"))
	if hasTraversalSegment(hook) || hasTraversalSegment(target) {
		return nil, fmt.Errorf("%w: query parameter contains traversal", ErrPathTraversal)
	}

	return &GitForgeRef{
		Host:    u.Host,
		Org:     org,
		Repo:    repo,
		Ref:     ref,
		Subpath: subpath,
		Hook:    hook,
		Target:  target,
	}, nil
}

// SnapshotTreeToCasV11 walks repoRoot (scoped to ref.Subpath if non-empty, skipping .git),
// stores every regular file into CasV11Store, serializes a deterministic sorted JSON
// GitForgeTreeManifest, stores the manifest itself into CasV11Store, and returns
// (manifest, manifestSHA512, nil).
func SnapshotTreeToCasV11(store *CasV11Store, ref *GitForgeRef, repoRoot string) (*GitForgeTreeManifest, string, error) {
	if store == nil {
		return nil, "", errors.New("casv11: store must not be nil")
	}
	if ref == nil {
		return nil, "", fmt.Errorf("%w: ref must not be nil", ErrInvalidGitForgeURI)
	}
	if strings.TrimSpace(repoRoot) == "" {
		return nil, "", errors.New("casv11: repoRoot must not be empty")
	}

	cleanRepoRoot, err := filepath.Abs(filepath.Clean(repoRoot))
	if err != nil {
		return nil, "", fmt.Errorf("casv11: resolve repoRoot: %w", err)
	}

	walkRoot := cleanRepoRoot
	normalizedRef := *ref
	if normalizedRef.Subpath != "" {
		cleanSub, err := validateRelativeSubpath(normalizedRef.Subpath)
		if err != nil {
			return nil, "", err
		}
		normalizedRef.Subpath = cleanSub
		walkRoot = filepath.Join(cleanRepoRoot, filepath.FromSlash(cleanSub))
		if err := ensureWithinBase(cleanRepoRoot, walkRoot); err != nil {
			return nil, "", err
		}
	}

	info, err := os.Stat(walkRoot)
	if err != nil {
		return nil, "", fmt.Errorf("casv11: stat walk root: %w", err)
	}
	if !info.IsDir() {
		return nil, "", fmt.Errorf("casv11: walk root %q is not a directory", walkRoot)
	}

	entries := make([]GitForgeTreeEntry, 0)
	err = filepath.WalkDir(walkRoot, func(currentPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}

		fileInfo, err := d.Info()
		if err != nil {
			return err
		}
		if !fileInfo.Mode().IsRegular() {
			return nil
		}

		relPath, err := filepath.Rel(walkRoot, currentPath)
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(relPath)

		data, err := os.ReadFile(currentPath)
		if err != nil {
			return fmt.Errorf("casv11: read %q: %w", relSlash, err)
		}

		executable := fileInfo.Mode().Perm()&0o111 != 0
		blobRef, err := store.Put(data, executable)
		if err != nil {
			return fmt.Errorf("casv11: put %q: %w", relSlash, err)
		}

		entries = append(entries, GitForgeTreeEntry{
			Path:       relSlash,
			SHA512:     blobRef.SHA512,
			Size:       blobRef.Size,
			Executable: executable,
		})
		return nil
	})
	if err != nil {
		return nil, "", err
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Path < entries[j].Path
	})

	manifest := &GitForgeTreeManifest{
		Protocol: RepositoryProtocol,
		ABIMagic: B8GAbiMagicV3,
		Ref:      normalizedRef,
		Entries:  entries,
	}

	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, "", fmt.Errorf("casv11: marshal gitforge manifest: %w", err)
	}

	manifestRef, err := store.Put(manifestBytes, false)
	if err != nil {
		return nil, "", fmt.Errorf("casv11: store gitforge manifest: %w", err)
	}

	return manifest, manifestRef.SHA512, nil
}

// HydrateTreeFromCasV11 loads the GitForgeTreeManifest addressed by manifestSHA512 from CasV11Store
// and materializes every entry into destDir via store.HydrateHardlink.
func HydrateTreeFromCasV11(store *CasV11Store, manifestSHA512 string, destDir string) (int, error) {
	if store == nil {
		return 0, errors.New("casv11: store must not be nil")
	}
	if strings.TrimSpace(destDir) == "" {
		return 0, errors.New("casv11: destDir must not be empty")
	}

	manifestBytes, err := store.Get(manifestSHA512, false)
	if err != nil {
		return 0, fmt.Errorf("casv11: load manifest %s: %w", manifestSHA512, err)
	}

	var manifest GitForgeTreeManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	if manifest.ABIMagic != B8GAbiMagicV3 {
		return 0, fmt.Errorf("%w: unexpected abi_magic 0x%x, want 0x%x", ErrInvalidManifest, manifest.ABIMagic, B8GAbiMagicV3)
	}

	cleanDest, err := filepath.Abs(filepath.Clean(destDir))
	if err != nil {
		return 0, fmt.Errorf("casv11: resolve destDir: %w", err)
	}
	if err := os.MkdirAll(cleanDest, 0o755); err != nil {
		return 0, fmt.Errorf("casv11: create destDir: %w", err)
	}

	hydrated := 0
	for _, entry := range manifest.Entries {
		cleanEntryPath, err := validateRelativeSubpath(entry.Path)
		if err != nil {
			return hydrated, fmt.Errorf("casv11: manifest entry %q: %w", entry.Path, err)
		}
		targetPath := filepath.Join(cleanDest, filepath.FromSlash(cleanEntryPath))
		if err := ensureWithinBase(cleanDest, targetPath); err != nil {
			return hydrated, fmt.Errorf("casv11: manifest entry %q: %w", entry.Path, err)
		}

		if err := store.HydrateHardlink(entry.SHA512, entry.Executable, targetPath); err != nil {
			return hydrated, fmt.Errorf("casv11: hydrate %q: %w", cleanEntryPath, err)
		}
		hydrated++
	}

	return hydrated, nil
}

func isForbiddenLoopbackHost(hostname string) bool {
	if hostname == "" {
		return true
	}
	if hostname == "localhost" || strings.HasSuffix(hostname, ".localhost") {
		return true
	}
	ip := net.ParseIP(hostname)
	if ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
		return true
	}
	return false
}

func hasTraversalSegment(s string) bool {
	if s == "" {
		return false
	}
	normalized := strings.ReplaceAll(s, "\\", "/")
	for _, seg := range strings.Split(normalized, "/") {
		if seg == ".." {
			return true
		}
	}
	return false
}

func validateIdentifierSegment(name, value string) error {
	if value == "" || value == "." || value == ".." {
		return fmt.Errorf("%w: invalid %s %q", ErrInvalidGitForgeURI, name, value)
	}
	if strings.ContainsAny(value, "/\\\x00") {
		return fmt.Errorf("%w: invalid %s %q", ErrPathTraversal, name, value)
	}
	return nil
}

func validateRelativeSubpath(subpath string) (string, error) {
	if strings.TrimSpace(subpath) == "" {
		return "", fmt.Errorf("%w: empty relative path", ErrPathTraversal)
	}
	if strings.ContainsRune(subpath, '\x00') || strings.ContainsRune(subpath, '\\') {
		return "", fmt.Errorf("%w: forbidden character in %q", ErrPathTraversal, subpath)
	}
	if strings.HasPrefix(subpath, "/") {
		return "", fmt.Errorf("%w: absolute path %q not allowed", ErrPathTraversal, subpath)
	}
	if hasTraversalSegment(subpath) {
		return "", fmt.Errorf("%w: path %q contains '..'", ErrPathTraversal, subpath)
	}
	cleaned := path.Clean(subpath)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.HasPrefix(cleaned, "/") {
		return "", fmt.Errorf("%w: invalid cleaned path %q", ErrPathTraversal, cleaned)
	}
	return cleaned, nil
}

func ensureWithinBase(baseDir, targetPath string) error {
	rel, err := filepath.Rel(baseDir, targetPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPathTraversal, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: %q escapes %q", ErrPathTraversal, targetPath, baseDir)
	}
	return nil
}
