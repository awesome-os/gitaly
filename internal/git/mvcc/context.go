package mvcc

import (
	"context"
)

// CacheID identifies the Cache of a running RPC. Hooks run in a separate process. They call back
// into Gitaly as new RPCs, so their context doesn't carry the Cache of the RPC that spawned them.
// The hooks payload passes them the ID instead, and the MVCC middleware looks the Cache up by it.
// The zero value identifies no Cache.
type CacheID uint64

type cacheContextKey struct{}

type cacheIDContextKey struct{}

// ContextWithCache returns a new context with the Cache stored in it. A nil Cache removes the
// context's Cache. The Cache counts as unregistered, so hooks spawned with the context don't get it.
func ContextWithCache(ctx context.Context, c Cache) context.Context {
	return ContextWithRegisteredCache(ctx, c, 0)
}

// ContextWithRegisteredCache returns a new context that stores the Cache and the ID it is
// registered under. Hooks spawned with the context find the Cache by that ID.
func ContextWithRegisteredCache(ctx context.Context, c Cache, id CacheID) context.Context {
	ctx = context.WithValue(ctx, cacheIDContextKey{}, id)
	return context.WithValue(ctx, cacheContextKey{}, c)
}

// ContextWithoutCache returns a context that carries no Cache, even if ctx does.
// Work that is detached from the request it originated in must not inherit that
// request's Cache: it outlives the request, so committing through that Cache would
// race with the request's own Commit, and it needs to operate on whichever manifest
// is current when it runs rather than the one the request pinned.
func ContextWithoutCache(ctx context.Context) context.Context {
	if CacheFromContext(ctx) == nil {
		return ctx
	}

	return ContextWithCache(ctx, nil)
}

// CacheIDFromContext returns the ID that the context's Cache is registered under, or zero if the
// Cache isn't registered.
func CacheIDFromContext(ctx context.Context) CacheID {
	id, _ := ctx.Value(cacheIDContextKey{}).(CacheID)
	return id
}

// CacheFromContext returns the Cache in the context, or nil if not found.
func CacheFromContext(ctx context.Context) Cache {
	if c, ok := ctx.Value(cacheContextKey{}).(Cache); ok {
		return c
	}
	return nil
}
