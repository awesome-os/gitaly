package casv11

import (
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPutDeduplicationAndLayout(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store, err := NewCasV11Store(root)
	if err != nil {
		t.Fatalf("NewCasV11Store: %v", err)
	}

	payload := []byte("hello awesome-os casv11 unified store")
	expectedSum := sha512.Sum512(payload)
	expectedHex := hex.EncodeToString(expectedSum[:])

	ref1, err := store.Put(payload, false)
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if ref1.Deduplicated {
		t.Fatalf("expected first Put Deduplicated=false, got true")
	}
	if ref1.SHA512 != expectedHex {
		t.Fatalf("digest mismatch: got %s, want %s", ref1.SHA512, expectedHex)
	}
	if ref1.AbiMagic != B8GAbiMagicV3 || B8GAbiMagicV3 != 0x33564742 {
		t.Fatalf("AbiMagic mismatch: got 0x%x, want 0x33564742", ref1.AbiMagic)
	}

	expectedPath := filepath.Join(root, "files", expectedHex[:2], expectedHex)
	if ref1.Path != expectedPath {
		t.Fatalf("layout path mismatch: got %s, want %s", ref1.Path, expectedPath)
	}
	if _, err := os.Stat(expectedPath); err != nil {
		t.Fatalf("expected blob at %s: %v", expectedPath, err)
	}

	ref2, err := store.Put(payload, false)
	if err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if !ref2.Deduplicated {
		t.Fatalf("expected second Put Deduplicated=true, got false")
	}
	if ref2.Path != ref1.Path || ref2.SHA512 != ref1.SHA512 {
		t.Fatalf("second Put ref mismatch: %+v vs %+v", ref2, ref1)
	}

	got, err := store.Get(expectedHex, false)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("Get payload mismatch: got %q, want %q", got, payload)
	}
}

func TestPutExecSuffix(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store, err := NewCasV11Store(root)
	if err != nil {
		t.Fatalf("NewCasV11Store: %v", err)
	}

	script := []byte("#!/bin/sh\necho ok\n")
	sum := sha512.Sum512(script)
	digest := hex.EncodeToString(sum[:])

	nonExecRef, err := store.Put(script, false)
	if err != nil {
		t.Fatalf("Put non-exec: %v", err)
	}
	execRef, err := store.Put(script, true)
	if err != nil {
		t.Fatalf("Put exec: %v", err)
	}
	if execRef.Deduplicated {
		t.Fatalf("expected exec blob to be stored separately from non-exec blob")
	}

	wantExecPath := filepath.Join(root, "files", digest[:2], digest+"-exec")
	if execRef.Path != wantExecPath {
		t.Fatalf("exec path mismatch: got %s, want %s", execRef.Path, wantExecPath)
	}
	if nonExecRef.Path == execRef.Path {
		t.Fatalf("non-exec and exec paths collided: %s", execRef.Path)
	}

	execRef2, err := store.Put(script, true)
	if err != nil {
		t.Fatalf("second Put exec: %v", err)
	}
	if !execRef2.Deduplicated {
		t.Fatalf("expected second Put exec Deduplicated=true")
	}

	gotExec, err := store.Get(digest, true)
	if err != nil {
		t.Fatalf("Get exec: %v", err)
	}
	if !bytes.Equal(gotExec, script) {
		t.Fatalf("Get exec content mismatch: got %q, want %q", gotExec, script)
	}
}

func TestHydrateHardlinkAndFallback(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store, err := NewCasV11Store(root)
	if err != nil {
		t.Fatalf("NewCasV11Store: %v", err)
	}

	payload := []byte("hardlink hydration test payload")
	ref, err := store.Put(payload, false)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// 1. Direct hardlink hydration
	destHardlink := filepath.Join(t.TempDir(), "nested", "blob.txt")
	if err := store.HydrateHardlink(ref.SHA512, false, destHardlink); err != nil {
		t.Fatalf("HydrateHardlink: %v", err)
	}

	srcInfo, err := os.Stat(ref.Path)
	if err != nil {
		t.Fatalf("Stat src: %v", err)
	}
	dstInfo, err := os.Stat(destHardlink)
	if err != nil {
		t.Fatalf("Stat dst: %v", err)
	}
	if !os.SameFile(srcInfo, dstInfo) {
		t.Fatalf("expected %s to be a hardlink to %s", destHardlink, ref.Path)
	}

	gotBytes, err := os.ReadFile(destHardlink)
	if err != nil {
		t.Fatalf("ReadFile hardlink: %v", err)
	}
	if !bytes.Equal(gotBytes, payload) {
		t.Fatalf("hardlink content mismatch: got %q, want %q", gotBytes, payload)
	}

	// 2. Copy fallback when os.Link fails (e.g., cross-device EXDEV)
	store.linkFn = func(oldname, newname string) error {
		return errors.New("simulated EXDEV")
	}
	destCopy := filepath.Join(t.TempDir(), "fallback", "blob.txt")
	if err := store.HydrateHardlink(ref.SHA512, false, destCopy); err != nil {
		t.Fatalf("HydrateHardlink copy fallback: %v", err)
	}
	copiedBytes, err := os.ReadFile(destCopy)
	if err != nil {
		t.Fatalf("ReadFile copy fallback: %v", err)
	}
	if !bytes.Equal(copiedBytes, payload) {
		t.Fatalf("copy fallback content mismatch: got %q, want %q", copiedBytes, payload)
	}
}

func TestGetIntegrityVerification(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store, err := NewCasV11Store(root)
	if err != nil {
		t.Fatalf("NewCasV11Store: %v", err)
	}

	ref, err := store.Put([]byte("authentic data"), false)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Corrupt the blob on disk.
	if err := os.WriteFile(ref.Path, []byte("corrupted data"), 0o644); err != nil {
		t.Fatalf("tamper WriteFile: %v", err)
	}

	_, err = store.Get(ref.SHA512, false)
	if !errors.Is(err, ErrIntegrityMismatch) {
		t.Fatalf("expected ErrIntegrityMismatch, got %v", err)
	}

	// Invalid digest rejection.
	if _, err := store.Get("../escape", false); !errors.Is(err, ErrInvalidDigest) {
		t.Fatalf("expected ErrInvalidDigest for traversal string, got %v", err)
	}
	if _, err := store.Get("not-a-valid-sha512", false); !errors.Is(err, ErrInvalidDigest) {
		t.Fatalf("expected ErrInvalidDigest for short string, got %v", err)
	}
}
