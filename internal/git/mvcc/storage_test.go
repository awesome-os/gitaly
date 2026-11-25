package mvcc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
)

// fakeStorage is a map-backed ObjectStorage for exercising ReadHeadManifest's
// composition without a filesystem or bucket. The embedded interface is nil, so
// a call outside the two read methods panics, keeping the fake honest about
// what ReadHeadManifest may touch.
type fakeStorage struct {
	ObjectStorage
	pointer    ManifestPointer
	pointerErr error
	manifests  map[string][]byte
}

func (f *fakeStorage) ReadManifestPointer(context.Context, string) (ManifestPointer, error) {
	if f.pointerErr != nil {
		return ManifestPointer{}, f.pointerErr
	}
	return f.pointer, nil
}

func (f *fakeStorage) ReadManifest(_ context.Context, _ string, hash string) (io.ReadCloser, error) {
	body, ok := f.manifests[hash]
	if !ok {
		return nil, fmt.Errorf("manifest %q: %w", hash, fs.ErrNotExist)
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func TestReadHeadManifest(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	body, sha, err := SerializeManifest(&Manifest{
		Paths: []string{
			"pack/aaaa.idx",
			"pack/aaaa.pack",
			"refs/bbbb.ref",
		},
		HashAlgo:      hashAlgoSHA1,
		objStart:      0,
		objCount:      2,
		refStackOrder: []string{"refs/bbbb.ref"},
	})
	require.NoError(t, err)

	head := ManifestPointer{Hash: sha, Generation: 4, HasGeneration: true}

	t.Run("resolves and parses the head manifest", func(t *testing.T) {
		t.Parallel()
		storage := &fakeStorage{
			pointer:   head,
			manifests: map[string][]byte{sha: body},
		}

		pointer, manifest, err := ReadHeadManifest(ctx, storage, "repo")
		require.NoError(t, err)
		require.Equal(t, head, pointer)
		require.Equal(t, []string{"pack/aaaa.idx", "pack/aaaa.pack"}, manifest.ObjPaths())
	})

	t.Run("not initialized surfaces typed", func(t *testing.T) {
		t.Parallel()
		storage := &fakeStorage{pointerErr: &NotInitializedError{RepositoryID: "repo"}}

		_, _, err := ReadHeadManifest(ctx, storage, "repo")
		require.ErrorAs(t, err, new(*NotInitializedError))
		require.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("missing manifest body", func(t *testing.T) {
		t.Parallel()
		storage := &fakeStorage{pointer: head, manifests: map[string][]byte{}}

		_, _, err := ReadHeadManifest(ctx, storage, "repo")
		require.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("corrupt manifest body", func(t *testing.T) {
		t.Parallel()
		storage := &fakeStorage{pointer: head, manifests: map[string][]byte{sha: []byte("junk")}}

		_, _, err := ReadHeadManifest(ctx, storage, "repo")
		require.ErrorContains(t, err, "parse manifest")
	})
}

func TestNotInitializedError(t *testing.T) {
	t.Parallel()

	// Wrapped the way ReadHeadManifest returns it, both the type and the
	// fs.ErrNotExist sentinel stay matchable.
	err := fmt.Errorf("read manifest pointer: %w", &NotInitializedError{RepositoryID: "@hashed/ab/repo.git"})
	require.ErrorIs(t, err, fs.ErrNotExist)

	var notInit *NotInitializedError
	require.ErrorAs(t, err, &notInit)
	require.Equal(t, "@hashed/ab/repo.git", notInit.RepositoryID)
}
