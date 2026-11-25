package mvcc

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConflictError_unwrapsToSentinel(t *testing.T) {
	t.Parallel()

	err := &ConflictError{
		Base:   ManifestPointer{Hash: "aaa", Generation: 1, HasGeneration: true},
		Winner: ManifestPointer{Hash: "bbb", Generation: 2, HasGeneration: true},
	}

	require.ErrorIs(t, err, ErrManifestPointerConflict,
		"a ConflictError must satisfy errors.Is(err, ErrManifestPointerConflict) so legacy callers keep working")

	var target *ConflictError
	require.True(t, errors.As(fmt.Errorf("wrapped: %w", err), &target))
	require.Equal(t, "aaa", target.Base.Hash)
	require.Equal(t, "bbb", target.Winner.Hash)
}
