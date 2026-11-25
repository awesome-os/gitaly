package casv11

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestParseGitForgeURI_Valid(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want GitForgeRef
	}{
		{
			name: "canonical repository URI defaults ref to HEAD",
			raw:  "repository://github.com/awesome-os/gitaly",
			want: GitForgeRef{
				Host: "github.com",
				Org:  "awesome-os",
				Repo: "gitaly",
				Ref:  "HEAD",
			},
		},
		{
			name: "canonical repository URI with @ref, #subpath, and ?hook&target",
			raw:  "repository://github.com/awesome-os/gitaly@v1.2.3#internal/casv11?hook=post-hydrate&target=win64",
			want: GitForgeRef{
				Host:    "github.com",
				Org:     "awesome-os",
				Repo:    "gitaly",
				Ref:     "v1.2.3",
				Subpath: "internal/casv11",
				Hook:    "post-hydrate",
				Target:  "win64",
			},
		},
		{
			name: "canonical repository URI with ?hook&target before #subpath",
			raw:  "repository://github.com/awesome-os/gitaly@v1.2.3?hook=post-hydrate&target=win64#internal/casv11",
			want: GitForgeRef{
				Host:    "github.com",
				Org:     "awesome-os",
				Repo:    "gitaly",
				Ref:     "v1.2.3",
				Subpath: "internal/casv11",
				Hook:    "post-hydrate",
				Target:  "win64",
			},
		},
		{
			name: "canonical repository URI with .git suffix and port",
			raw:  "repository://forge.awesome-os.com:8443/awesome-os/b8g-runtime.git@main",
			want: GitForgeRef{
				Host: "forge.awesome-os.com:8443",
				Org:  "awesome-os",
				Repo: "b8g-runtime",
				Ref:  "main",
			},
		},
		{
			name: "legacy git-forge alias accepted",
			raw:  "git-forge://gitlab.com/awesome-os/sectium@deadbeef#sections/b8g-es?hook=build&target=linux-amd64",
			want: GitForgeRef{
				Host:    "gitlab.com",
				Org:     "awesome-os",
				Repo:    "sectium",
				Ref:     "deadbeef",
				Subpath: "sections/b8g-es",
				Hook:    "build",
				Target:  "linux-amd64",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseGitForgeURI(tc.raw)
			if err != nil {
				t.Fatalf("ParseGitForgeURI(%q) unexpected error: %v", tc.raw, err)
			}
			if *got != tc.want {
				t.Fatalf("ParseGitForgeURI(%q) = %+v, want %+v", tc.raw, *got, tc.want)
			}
		})
	}
}

func TestParseGitForgeURI_RejectsTraversalAndLoopback(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr error
	}{
		{
			name:    "reject 127.0.0.1 per B8G016",
			raw:     "repository://127.0.0.1/awesome-os/gitaly",
			wantErr: ErrLoopbackForbidden,
		},
		{
			name:    "reject 127.0.0.1 with port per B8G016",
			raw:     "repository://127.0.0.1:8080/awesome-os/gitaly",
			wantErr: ErrLoopbackForbidden,
		},
		{
			name:    "reject localhost per B8G016",
			raw:     "repository://localhost/awesome-os/gitaly",
			wantErr: ErrLoopbackForbidden,
		},
		{
			name:    "reject subdomain of localhost per B8G016",
			raw:     "repository://api.localhost/awesome-os/gitaly",
			wantErr: ErrLoopbackForbidden,
		},
		{
			name:    "reject IPv6 loopback per B8G016",
			raw:     "repository://[::1]/awesome-os/gitaly",
			wantErr: ErrLoopbackForbidden,
		},
		{
			name:    "reject 0.0.0.0 per B8G016",
			raw:     "repository://0.0.0.0/awesome-os/gitaly",
			wantErr: ErrLoopbackForbidden,
		},
		{
			name:    "reject .. traversal in fragment subpath",
			raw:     "repository://github.com/awesome-os/gitaly@main#../../etc/passwd",
			wantErr: ErrPathTraversal,
		},
		{
			name:    "reject .. traversal in path subpath",
			raw:     "repository://github.com/awesome-os/gitaly/../../etc/passwd",
			wantErr: ErrPathTraversal,
		},
		{
			name:    "reject percent-encoded .. traversal",
			raw:     "repository://github.com/awesome-os/gitaly/%2e%2e/secret",
			wantErr: ErrPathTraversal,
		},
		{
			name:    "reject .. traversal in query subpath",
			raw:     "repository://github.com/awesome-os/gitaly?subpath=foo/../bar",
			wantErr: ErrPathTraversal,
		},
		{
			name:    "reject backslash traversal",
			raw:     `repository://github.com/awesome-os/gitaly@main#foo\..\bar`,
			wantErr: ErrPathTraversal,
		},
		{
			name:    "reject empty URI",
			raw:     "",
			wantErr: ErrInvalidGitForgeURI,
		},
		{
			name:    "reject missing repo",
			raw:     "repository://github.com/awesome-os",
			wantErr: ErrInvalidGitForgeURI,
		},
		{
			name:    "reject unsupported file scheme",
			raw:     "file:///awesome-os/gitaly",
			wantErr: ErrInvalidGitForgeURI,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseGitForgeURI(tc.raw)
			if err == nil {
				t.Fatalf("ParseGitForgeURI(%q) = %+v, expected error %v", tc.raw, got, tc.wantErr)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ParseGitForgeURI(%q) error = %v, want errors.Is(..., %v)", tc.raw, err, tc.wantErr)
			}
		})
	}
}

func TestSnapshotAndHydrateTree_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	storeRoot := filepath.Join(tmpDir, "cas-v11")
	repoRoot := filepath.Join(tmpDir, "zero-pkg-repo")
	destDir := filepath.Join(tmpDir, "hydrated-tree")

	store, err := NewCasV11Store(storeRoot)
	if err != nil {
		t.Fatalf("NewCasV11Store: %v", err)
	}

	// Build a zero-package.json repository tree plus a .git directory that MUST be skipped.
	repoFiles := map[string][]byte{
		"mod.mts":               []byte("export const greet = (name: string): string => `hello ${name}`;\n"),
		"src/runtime/arena.mts": []byte("export const B8G_ABI_MAGIC_V3 = 0x33564742;\n"),
		"src/runtime/index.mts": []byte("export * from './arena.mts';\n"),
		"README.md":             []byte("# Zero package.json Stealify module\n"),
	}
	for rel, content := range repoFiles {
		full := filepath.Join(repoRoot, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("MkdirAll %s: %v", rel, err)
		}
		if err := os.WriteFile(full, content, 0o644); err != nil {
			t.Fatalf("WriteFile %s: %v", rel, err)
		}
	}

	// Populate .git internals that must NOT appear in the snapshot or hydrated tree.
	gitHeadPath := filepath.Join(repoRoot, ".git", "HEAD")
	if err := os.MkdirAll(filepath.Dir(gitHeadPath), 0o755); err != nil {
		t.Fatalf("MkdirAll .git: %v", err)
	}
	if err := os.WriteFile(gitHeadPath, []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatalf("WriteFile .git/HEAD: %v", err)
	}

	ref, err := ParseGitForgeURI("repository://github.com/awesome-os/zero-pkg-repo@main?hook=verify&target=wasi")
	if err != nil {
		t.Fatalf("ParseGitForgeURI: %v", err)
	}

	manifest, manifestSHA512, err := SnapshotTreeToCasV11(store, ref, repoRoot)
	if err != nil {
		t.Fatalf("SnapshotTreeToCasV11: %v", err)
	}

	if manifest.Protocol != RepositoryProtocol {
		t.Fatalf("manifest.Protocol = %q, want %q", manifest.Protocol, RepositoryProtocol)
	}
	if manifest.ABIMagic != B8GAbiMagicV3 {
		t.Fatalf("manifest.ABIMagic = 0x%x, want 0x%x", manifest.ABIMagic, B8GAbiMagicV3)
	}
	if len(manifest.Entries) != len(repoFiles) {
		t.Fatalf("len(manifest.Entries) = %d, want %d", len(manifest.Entries), len(repoFiles))
	}

	// Verify deterministic sorted ordering of manifest entries.
	for i := 1; i < len(manifest.Entries); i++ {
		if manifest.Entries[i-1].Path >= manifest.Entries[i].Path {
			t.Fatalf("manifest entries not strictly sorted: %q >= %q", manifest.Entries[i-1].Path, manifest.Entries[i].Path)
		}
	}

	// Verify deterministic manifest SHA-512 across repeated snapshots.
	_, manifestSHA512Again, err := SnapshotTreeToCasV11(store, ref, repoRoot)
	if err != nil {
		t.Fatalf("second SnapshotTreeToCasV11: %v", err)
	}
	if manifestSHA512Again != manifestSHA512 {
		t.Fatalf("manifest SHA-512 not deterministic: %s != %s", manifestSHA512Again, manifestSHA512)
	}

	// Hydrate tree from CAS v11 via hardlinks.
	hydratedCount, err := HydrateTreeFromCasV11(store, manifestSHA512, destDir)
	if err != nil {
		t.Fatalf("HydrateTreeFromCasV11: %v", err)
	}
	if hydratedCount != len(repoFiles) {
		t.Fatalf("hydratedCount = %d, want %d", hydratedCount, len(repoFiles))
	}

	// Confirm .git was skipped.
	if _, err := os.Stat(filepath.Join(destDir, ".git")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(".git should not be hydrated, stat err = %v", err)
	}

	// Verify exact byte content and os.SameFile hardlink identity against CAS blobs.
	for _, entry := range manifest.Entries {
		wantBytes, ok := repoFiles[entry.Path]
		if !ok {
			t.Fatalf("unexpected entry %q in manifest", entry.Path)
		}

		hydratedPath := filepath.Join(destDir, filepath.FromSlash(entry.Path))
		gotBytes, err := os.ReadFile(hydratedPath)
		if err != nil {
			t.Fatalf("ReadFile(%q): %v", hydratedPath, err)
		}
		if !bytes.Equal(gotBytes, wantBytes) {
			t.Fatalf("content mismatch for %q: got %q, want %q", entry.Path, gotBytes, wantBytes)
		}

		casBlobPath, err := store.BlobPath(entry.SHA512, entry.Executable)
		if err != nil {
			t.Fatalf("BlobPath(%q): %v", entry.Path, err)
		}
		casInfo, err := os.Stat(casBlobPath)
		if err != nil {
			t.Fatalf("Stat CAS blob %q: %v", casBlobPath, err)
		}
		hydratedInfo, err := os.Stat(hydratedPath)
		if err != nil {
			t.Fatalf("Stat hydrated file %q: %v", hydratedPath, err)
		}
		if !os.SameFile(casInfo, hydratedInfo) {
			t.Fatalf("expected %q to be hardlinked to CAS blob %q (os.SameFile == false)", hydratedPath, casBlobPath)
		}
	}
}

func TestSnapshotTreeToCasV11_ScopedSubpath(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewCasV11Store(filepath.Join(tmpDir, "cas-v11"))
	if err != nil {
		t.Fatalf("NewCasV11Store: %v", err)
	}

	repoRoot := filepath.Join(tmpDir, "monorepo")
	files := map[string][]byte{
		"root.txt":                    []byte("outside subpath\n"),
		"packages/core/entry.mts":     []byte("export const ok = true;\n"),
		"packages/core/sub/helper.ts": []byte("export const n = 42;\n"),
	}
	for rel, content := range files {
		full := filepath.Join(repoRoot, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(full, content, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}

	ref, err := ParseGitForgeURI("repository://github.com/awesome-os/monorepo@main#packages/core")
	if err != nil {
		t.Fatalf("ParseGitForgeURI: %v", err)
	}

	manifest, manifestSHA512, err := SnapshotTreeToCasV11(store, ref, repoRoot)
	if err != nil {
		t.Fatalf("SnapshotTreeToCasV11: %v", err)
	}
	if len(manifest.Entries) != 2 {
		t.Fatalf("len(manifest.Entries) = %d, want 2 (%+v)", len(manifest.Entries), manifest.Entries)
	}
	if manifest.Entries[0].Path != "entry.mts" || manifest.Entries[1].Path != "sub/helper.ts" {
		t.Fatalf("unexpected scoped entries: %+v", manifest.Entries)
	}

	destDir := filepath.Join(tmpDir, "hydrated-subpath")
	n, err := HydrateTreeFromCasV11(store, manifestSHA512, destDir)
	if err != nil {
		t.Fatalf("HydrateTreeFromCasV11: %v", err)
	}
	if n != 2 {
		t.Fatalf("hydrated count = %d, want 2", n)
	}
	if _, err := os.Stat(filepath.Join(destDir, "root.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("root.txt outside subpath should not exist, err = %v", err)
	}
}
