package casv11

import (
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// B8GAbiMagicV3 is the canonical B8G ABI v3 magic constant ("BGV3", 0x33564742).
const B8GAbiMagicV3 = uint32(0x33564742)

const (
	sha512HexLen = 128
	execSuffix   = "-exec"
)

var (
	// ErrInvalidDigest is returned when a SHA-512 hex digest is malformed.
	ErrInvalidDigest = errors.New("casv11: invalid sha512 hex digest")
	// ErrIntegrityMismatch is returned when a stored blob's content does not match its SHA-512 digest.
	ErrIntegrityMismatch = errors.New("casv11: sha512 integrity verification failed")
)

// BlobRef describes a content-addressed blob stored in the unified pnpm-v12/Awesome-OS CAS layout.
type BlobRef struct {
	SHA512       string `json:"sha512"`
	Size         int64  `json:"size"`
	Executable   bool   `json:"executable"`
	Path         string `json:"path"`
	Deduplicated bool   `json:"deduplicated"`
	AbiMagic     uint32 `json:"abi_magic"`
}

// CasV11Store implements the unified pnpm-v12 / Awesome-OS v11 content-addressable store:
//
//	<root>/files/<sha512[:2]>/<sha512>
//	<root>/files/<sha512[:2]>/<sha512>-exec   (when executable)
type CasV11Store struct {
	root   string
	linkFn func(oldname, newname string) error
}

// NewCasV11Store initializes a CasV11Store rooted at root and ensures <root>/files exists.
func NewCasV11Store(root string) (*CasV11Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("casv11: store root must not be empty")
	}
	cleanRoot := filepath.Clean(root)
	if err := os.MkdirAll(filepath.Join(cleanRoot, "files"), 0o755); err != nil {
		return nil, fmt.Errorf("casv11: create files directory: %w", err)
	}
	return &CasV11Store{
		root:   cleanRoot,
		linkFn: os.Link,
	}, nil
}

// Root returns the cleaned root directory of the store.
func (s *CasV11Store) Root() string {
	return s.root
}

// NormalizeDigest validates that sha512Hex is a 128-character hexadecimal SHA-512 digest
// and returns its canonical lowercase representation.
func NormalizeDigest(sha512Hex string) (string, error) {
	if len(sha512Hex) != sha512HexLen {
		return "", fmt.Errorf("%w: got length %d, want %d", ErrInvalidDigest, len(sha512Hex), sha512HexLen)
	}
	lower := strings.ToLower(sha512Hex)
	if _, err := hex.DecodeString(lower); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidDigest, err)
	}
	return lower, nil
}

// BlobPath resolves the on-disk path for a given SHA-512 digest and executable flag.
func (s *CasV11Store) BlobPath(sha512Hex string, executable bool) (string, error) {
	digest, err := NormalizeDigest(sha512Hex)
	if err != nil {
		return "", err
	}
	name := digest
	if executable {
		name += execSuffix
	}
	return filepath.Join(s.root, "files", digest[:2], name), nil
}

// Put computes the SHA-512 digest of data, atomically writes the blob to the CAS via temp file + rename,
// and deduplicates identical blobs (returning Deduplicated: true when already present).
func (s *CasV11Store) Put(data []byte, executable bool) (BlobRef, error) {
	sum := sha512.Sum512(data)
	digest := hex.EncodeToString(sum[:])

	targetPath, err := s.BlobPath(digest, executable)
	if err != nil {
		return BlobRef{}, err
	}

	if info, err := os.Stat(targetPath); err == nil && info.Mode().IsRegular() {
		return BlobRef{
			SHA512:       digest,
			Size:         info.Size(),
			Executable:   executable,
			Path:         targetPath,
			Deduplicated: true,
			AbiMagic:     B8GAbiMagicV3,
		}, nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return BlobRef{}, fmt.Errorf("casv11: stat target blob: %w", err)
	}

	shardDir := filepath.Dir(targetPath)
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return BlobRef{}, fmt.Errorf("casv11: create shard dir: %w", err)
	}

	tmpFile, err := os.CreateTemp(shardDir, ".casv11-tmp-*")
	if err != nil {
		return BlobRef{}, fmt.Errorf("casv11: create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return BlobRef{}, fmt.Errorf("casv11: write temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return BlobRef{}, fmt.Errorf("casv11: close temp file: %w", err)
	}

	mode := os.FileMode(0o644)
	if executable {
		mode = 0o755
	}
	if err := os.Chmod(tmpPath, mode); err != nil {
		return BlobRef{}, fmt.Errorf("casv11: chmod temp file: %w", err)
	}

	if err := os.Rename(tmpPath, targetPath); err != nil {
		// Handle concurrent writer race where targetPath appeared before rename.
		if info, statErr := os.Stat(targetPath); statErr == nil && info.Mode().IsRegular() {
			return BlobRef{
				SHA512:       digest,
				Size:         info.Size(),
				Executable:   executable,
				Path:         targetPath,
				Deduplicated: true,
				AbiMagic:     B8GAbiMagicV3,
			}, nil
		}
		return BlobRef{}, fmt.Errorf("casv11: atomic rename: %w", err)
	}
	cleanupTmp = false

	return BlobRef{
		SHA512:       digest,
		Size:         int64(len(data)),
		Executable:   executable,
		Path:         targetPath,
		Deduplicated: false,
		AbiMagic:     B8GAbiMagicV3,
	}, nil
}

// Get reads the blob for sha512Hex and verifies its SHA-512 integrity before returning the bytes.
func (s *CasV11Store) Get(sha512Hex string, executable bool) ([]byte, error) {
	digest, err := NormalizeDigest(sha512Hex)
	if err != nil {
		return nil, err
	}
	targetPath, err := s.BlobPath(digest, executable)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(targetPath)
	if err != nil {
		return nil, err
	}

	sum := sha512.Sum512(data)
	actual := hex.EncodeToString(sum[:])
	if actual != digest {
		return nil, fmt.Errorf("%w: expected %s, got %s", ErrIntegrityMismatch, digest, actual)
	}
	return data, nil
}

// HydrateHardlink materializes a stored blob at destPath using os.Link, falling back to an atomic
// file copy if hardlinking fails (e.g., cross-device link or unsupported filesystem).
func (s *CasV11Store) HydrateHardlink(sha512Hex string, executable bool, destPath string) error {
	if strings.TrimSpace(destPath) == "" {
		return errors.New("casv11: destination path must not be empty")
	}
	srcPath, err := s.BlobPath(sha512Hex, executable)
	if err != nil {
		return err
	}

	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		return fmt.Errorf("casv11: stat source blob: %w", err)
	}

	destDir := filepath.Dir(destPath)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return fmt.Errorf("casv11: create destination directory: %w", err)
	}

	if err := os.Remove(destPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("casv11: remove existing destination: %w", err)
	}

	linkFn := s.linkFn
	if linkFn == nil {
		linkFn = os.Link
	}
	if err := linkFn(srcPath, destPath); err == nil {
		return nil
	}

	// Copy fallback when hardlink is unavailable (e.g., EXDEV).
	return copyFileAtomic(srcPath, destPath, srcInfo.Mode().Perm())
}

func copyFileAtomic(srcPath, destPath string, perm os.FileMode) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("casv11: open source for copy fallback: %w", err)
	}
	defer src.Close()

	destDir := filepath.Dir(destPath)
	tmpFile, err := os.CreateTemp(destDir, ".casv11-hydrate-*")
	if err != nil {
		return fmt.Errorf("casv11: create temp file for copy fallback: %w", err)
	}
	tmpPath := tmpFile.Name()
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := io.Copy(tmpFile, src); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("casv11: copy blob data: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("casv11: close copied temp file: %w", err)
	}
	if err := os.Chmod(tmpPath, perm); err != nil {
		return fmt.Errorf("casv11: chmod copied temp file: %w", err)
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		return fmt.Errorf("casv11: rename copied temp file: %w", err)
	}
	cleanupTmp = false
	return nil
}
