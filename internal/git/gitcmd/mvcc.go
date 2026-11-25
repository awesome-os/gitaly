package gitcmd

import "context"

// mvccCtxKey is the context key under which the MVCC marker is stored. Its value records whether
// the context's repository uses the MVCC backends.
type mvccCtxKey struct{}

// ContextWithMVCC returns a new context marking the repository as using the MVCC backends. The
// command factory then runs Git with the MVCC-enabled binary, the only one able to read the
// repository, and creates new repositories with the MVCC backends.
func ContextWithMVCC(ctx context.Context) context.Context {
	return context.WithValue(ctx, mvccCtxKey{}, true)
}

// ContextWithoutMVCC returns ctx without the MVCC marker, so that Git runs with a general-purpose
// binary and creates repositories with the files backend even if a parent context set the marker.
// ctx is returned as is when it carries no marker.
func ContextWithoutMVCC(ctx context.Context) context.Context {
	if !MVCCFromContext(ctx) {
		return ctx
	}

	return context.WithValue(ctx, mvccCtxKey{}, false)
}

// MVCCFromContext returns whether the context marks the repository as using the MVCC backends.
func MVCCFromContext(ctx context.Context) bool {
	marked, _ := ctx.Value(mvccCtxKey{}).(bool)
	return marked
}
