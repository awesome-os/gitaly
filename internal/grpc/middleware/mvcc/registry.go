package mvcc

import (
	"sync"
	"sync/atomic"

	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
)

// cacheRegistry tracks the caches of running RPCs, so that the hook RPCs they spawn can find them.
type cacheRegistry struct {
	lastID atomic.Uint64
	caches sync.Map
}

type registeredCache struct {
	cache        mvcc.Cache
	storageName  string
	relativePath string
}

// register registers the cache of the given repository. It returns the cache's ID and a function
// that unregisters it.
func (r *cacheRegistry) register(c mvcc.Cache, storageName, relativePath string) (mvcc.CacheID, func()) {
	id := mvcc.CacheID(r.lastID.Add(1))
	r.caches.Store(id, registeredCache{cache: c, storageName: storageName, relativePath: relativePath})

	return id, func() { r.caches.Delete(id) }
}

// get returns the cache registered under the ID for the given repository, or nil if there is none.
// The ID comes from the hook request, so the caller supplies it. A cache that is registered for
// another repository is therefore never returned.
func (r *cacheRegistry) get(id mvcc.CacheID, storageName, relativePath string) mvcc.Cache {
	value, ok := r.caches.Load(id)
	if !ok {
		return nil
	}

	registered := value.(registeredCache)
	if registered.storageName != storageName || registered.relativePath != relativePath {
		return nil
	}

	return registered.cache
}
