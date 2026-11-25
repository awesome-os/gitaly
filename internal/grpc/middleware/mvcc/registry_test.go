package mvcc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
)

func TestCacheRegistry(t *testing.T) {
	t.Parallel()

	var registry cacheRegistry

	cacheA, cacheB := &mvcc.LocalCache{}, &mvcc.LocalCache{}
	idA, unregisterA := registry.register(cacheA, "default", "repo-a")
	idB, unregisterB := registry.register(cacheB, "default", "repo-b")
	defer unregisterB()

	require.NotZero(t, idA)
	require.NotEqual(t, idA, idB)

	require.Same(t, cacheA, registry.get(idA, "default", "repo-a"))
	require.Same(t, cacheB, registry.get(idB, "default", "repo-b"))

	// A cache is only returned for its own repository.
	require.Nil(t, registry.get(idA, "default", "repo-b"))
	require.Nil(t, registry.get(idA, "other", "repo-a"))

	require.Nil(t, registry.get(0, "default", "repo-a"))

	unregisterA()
	require.Nil(t, registry.get(idA, "default", "repo-a"))
	require.Same(t, cacheB, registry.get(idB, "default", "repo-b"))
}
