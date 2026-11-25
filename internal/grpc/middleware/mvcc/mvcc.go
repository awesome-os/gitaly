package mvcc

import (
	"context"
	"errors"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/mvcc"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/reftable"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/grpc/protoregistry"
	logger "gitlab.com/gitlab-org/gitaly/v19/internal/log"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/middleware"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"google.golang.org/grpc"
	grpchealth "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"
)

// cacheBypassMethods lists the RPCs the interceptor skips before it builds a cache.
//
// The list should stay short. An RPC that runs no Git command still benefits from the
// warm-up, which refreshes the local cache from object storage and leaves it warm for
// later requests. Only methods that delete the repository qualify, because there the
// warm-up races with the deletion and nothing is left to read or publish.
var cacheBypassMethods = map[string]struct{}{
	// RemoveRepository renames the repository away and deletes it from disk while the
	// warm-up is still reading the manifest pointer, the manifest and the artifacts.
	gitalypb.RepositoryService_RemoveRepository_FullMethodName: {},
}

// CacheInterceptor is a middleware that injects an mvcc.Cache into the context
// when the repository is using the mvcc reference backend.
type CacheInterceptor struct {
	cacheFactory *mvcc.CacheFactory
	registry     *protoregistry.Registry
	locator      storage.Locator
	logger       logger.Logger
	// caches holds the caches of the running RPCs, so that the hook RPCs they spawn can use them.
	caches    *cacheRegistry
	retryOpts mvcc.RetryOpts
}

// NewMvccInterceptor returns a new MVCC interceptor. cacheFactory should be
// shared with the other MVCC cache consumers of the process so that they all
// use the same ObjectStorage per storage.
func NewMvccInterceptor(cacheFactory *mvcc.CacheFactory, logger logger.Logger, registry *protoregistry.Registry, locator storage.Locator) *CacheInterceptor {
	return &CacheInterceptor{
		cacheFactory: cacheFactory,
		logger:       logger,
		registry:     registry,
		locator:      locator,
		caches:       &cacheRegistry{},
		// Wire the reftable callbacks the 3-way merge needs here, where importing
		// internal/git/reftable is safe. The mvcc package can't import it directly
		// without closing a test-only cycle through gittest.
		retryOpts: mvcc.RetryOpts{
			MergeOpts: mvcc.MergeOpts{
				ReadReftableRefs:         reftable.ReadRefsAt,
				ReadReftableUpdateRange:  reftable.ReadUpdateRangeAt,
				PatchReftableUpdateRange: reftable.PatchUpdateRangeAt,
			},
			Logger: logger,
		},
	}
}

// UnaryInterceptor returns a Unary Interceptor
func (m *CacheInterceptor) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		methodInfo, err := m.registry.LookupMethod(info.FullMethod)
		if err != nil {
			if !recognizedNonGitalyMethod(info.FullMethod) {
				m.logger.WithError(err).Warn("mvcc unary interceptor: unable to lookup method")
			}
			return handler(ctx, req)
		}

		if _, ok := cacheBypassMethods[info.FullMethod]; ok {
			return handler(ctx, req)
		}

		targetRepo, err := methodInfo.TargetRepo(req.(proto.Message))
		if err != nil {
			return handler(ctx, req)
		}

		cache, err := m.buildCache(ctx, targetRepo, methodInfo.Operation)
		if err != nil || cache == nil {
			return handler(ctx, req)
		}

		// Cleaning up is registered before preparing, so that the repository cannot stay
		// locked if anything goes wrong in between.
		defer cache.Cleanup()
		// Unregister before cleaning up, so that no hook RPC finds a cache that is cleaned up.
		id, unregister := m.caches.register(cache, targetRepo.GetStorageName(), targetRepo.GetRelativePath())
		defer unregister()
		cache.Prepare(ctx)
		ctx = mvcc.ContextWithRegisteredCache(ctx, cache, id)
		ctx = gitcmd.ContextWithMVCC(ctx)
		res, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}
		if _, err := mvcc.PublishSerialized(ctx, cache, m.retryOpts); err != nil {
			return nil, mapCommitError(err)
		}

		return res, nil
	}
}

// StreamInterceptor returns a Stream Interceptor
func (m *CacheInterceptor) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()

		methodInfo, err := m.registry.LookupMethod(info.FullMethod)
		if err != nil {
			if !recognizedNonGitalyMethod(info.FullMethod) {
				m.logger.WithError(err).Warn("mvcc unary interceptor: unable to lookup method")
			}
			return handler(srv, stream)
		}

		if _, ok := cacheBypassMethods[info.FullMethod]; ok {
			return handler(srv, stream)
		}

		req := methodInfo.NewRequest()
		if err := stream.RecvMsg(req); err != nil {
			return handler(srv, middleware.NewPeekedStream(ctx, nil, err, stream))
		}

		targetRepo, err := methodInfo.TargetRepo(req)
		if err != nil {
			return handler(srv, middleware.NewPeekedStream(ctx, req, nil, stream))
		}

		cache, err := m.buildCache(ctx, targetRepo, methodInfo.Operation)
		if err != nil || cache == nil {
			return handler(srv, middleware.NewPeekedStream(ctx, req, nil, stream))
		}

		// Cleaning up is registered before preparing, so that the repository cannot stay
		// locked if anything goes wrong in between.
		defer cache.Cleanup()
		// Unregister before cleaning up, so that no hook RPC finds a cache that is cleaned up.
		id, unregister := m.caches.register(cache, targetRepo.GetStorageName(), targetRepo.GetRelativePath())
		defer unregister()
		cache.Prepare(ctx)
		ctx = mvcc.ContextWithRegisteredCache(ctx, cache, id)
		ctx = gitcmd.ContextWithMVCC(ctx)
		if err := handler(srv, middleware.NewPeekedStream(ctx, req, nil, stream)); err != nil {
			return err
		}
		if _, err := mvcc.PublishSerialized(ctx, cache, m.retryOpts); err != nil {
			return mapCommitError(err)
		}

		return nil
	}
}

// InternalUnaryInterceptor returns a unary interceptor for the internal server.
//
// Hook RPCs arrive there as new requests from gitaly-hooks. Their context has neither the cache nor
// the MVCC marker of the RPC that spawned the hook. The interceptor puts that RPC's cache
// into the context. The hook then sees the same manifest as the RPC, including reference updates
// that are still in flight. The interceptor never commits the cache, because the spawning RPC does
// that.
//
// Other RPCs on the internal server get their own cache, as on the external server.
func (m *CacheInterceptor) InternalUnaryInterceptor() grpc.UnaryServerInterceptor {
	cacheInterceptor := m.UnaryInterceptor()

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !isHookMethod(info.FullMethod) {
			return cacheInterceptor(ctx, req, info, handler)
		}

		methodInfo, err := m.registry.LookupMethod(info.FullMethod)
		if err != nil {
			return nil, structerr.NewInternal("mvcc: lookup hook method: %w", err)
		}

		ctx, err = m.hookContext(ctx, methodInfo, req.(proto.Message))
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// InternalStreamInterceptor is the streaming variant of InternalUnaryInterceptor.
func (m *CacheInterceptor) InternalStreamInterceptor() grpc.StreamServerInterceptor {
	cacheInterceptor := m.StreamInterceptor()

	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !isHookMethod(info.FullMethod) {
			return cacheInterceptor(srv, stream, info, handler)
		}

		ctx := stream.Context()

		methodInfo, err := m.registry.LookupMethod(info.FullMethod)
		if err != nil {
			return structerr.NewInternal("mvcc: lookup hook method: %w", err)
		}

		req := methodInfo.NewRequest()
		if err := stream.RecvMsg(req); err != nil {
			return handler(srv, middleware.NewPeekedStream(ctx, nil, err, stream))
		}

		ctx, err = m.hookContext(ctx, methodInfo, req)
		if err != nil {
			return err
		}

		return handler(srv, middleware.NewPeekedStream(ctx, req, nil, stream))
	}
}

// isHookMethod returns whether fullMethod is an RPC of the HookService.
func isHookMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/"+gitalypb.HookService_ServiceDesc.ServiceName+"/")
}

// hooksPayloadRequest is a hook RPC request that carries the hooks payload in its environment
// variables. SignalPostReceiveReadyRequest is the only hook request without them.
type hooksPayloadRequest interface {
	GetEnvironmentVariables() []string
}

// hookContext returns the context to run the hook RPC req in.
//
// The hook may outlive the spawning RPC. When the pack-objects cache misses, git-pack-objects keeps
// running for the other clients after the RPC has cleaned up its cache. This is safe, because the
// spawning RPC is an accessor. Cleaning up a read-only cache removes nothing. Garbage collection
// protects the artifacts of recent manifests by their age, whether an RPC uses them or not.
//
// It returns an error if the hooks payload names a cache that isn't registered for the hook's
// repository. The hook would otherwise read a different manifest than the RPC that spawned it.
func (m *CacheInterceptor) hookContext(ctx context.Context, methodInfo protoregistry.MethodInfo, req proto.Message) (context.Context, error) {
	targetRepo, err := methodInfo.TargetRepo(req)
	if err != nil {
		return ctx, nil
	}

	if req, ok := req.(hooksPayloadRequest); ok {
		payload, err := gitcmd.HooksPayloadFromEnv(req.GetEnvironmentVariables())
		if err != nil {
			m.logger.WithError(err).DebugContext(ctx, "mvcc hook interceptor: no hooks payload")
		} else if payload.MVCCCacheID != 0 {
			cache := m.caches.get(payload.MVCCCacheID, targetRepo.GetStorageName(), targetRepo.GetRelativePath())
			if cache == nil {
				return nil, structerr.NewFailedPrecondition("mvcc: cache of the spawning RPC not found").WithMetadataItems(
					structerr.MetadataItem{Key: "mvcc_cache_id", Value: payload.MVCCCacheID},
					structerr.MetadataItem{Key: "storage_name", Value: targetRepo.GetStorageName()},
					structerr.MetadataItem{Key: "relative_path", Value: targetRepo.GetRelativePath()},
				)
			}

			ctx = mvcc.ContextWithRegisteredCache(ctx, cache, payload.MVCCCacheID)
			return gitcmd.ContextWithMVCC(ctx), nil
		}
	}

	// No RPC with a cache spawned the hook. One example is Git that runs outside of an RPC. The
	// hook still needs the binary that can read the repository. It reads the current manifest.
	if m.isMvccReferenceBackend(ctx, targetRepo) {
		ctx = gitcmd.ContextWithMVCC(ctx)
	}

	return ctx, nil
}

// mapCommitError translates a PublishSerialized error into an appropriate gRPC
// status. The retry loop already classifies conflicts it could not resolve
// (overlap or an exhausted budget) as Aborted carrying the mvcc_conflict_kind
// metadata; those pass through unchanged. A bare conflict with no retry
// classification is reported as Aborted so the client may retry against the new
// base; all other errors are returned as-is.
//
// The Aborted error carries an mvcc_conflict_kind metadata key naming which of the two
// conflicts it was, so a plain lost compare-and-swap can be told apart in logs from a
// merge that could not reconcile the two sides without parsing the message. Overlap is
// checked first so that, should an error ever carry both, the more specific kind wins.
func mapCommitError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := structerr.ExtractMetadata(err)[mvcc.ConflictKindMetadataKey]; ok {
		// Already classified by the retry loop.
		return err
	}
	switch {
	case errors.Is(err, mvcc.ErrOverlap):
		return structerr.NewAborted("mvcc: %w", err).
			WithMetadata(mvcc.ConflictKindMetadataKey, string(mvcc.ConflictKindOverlap))
	case errors.Is(err, mvcc.ErrManifestPointerConflict):
		return structerr.NewAborted("mvcc: %w", err).
			WithMetadata(mvcc.ConflictKindMetadataKey, string(mvcc.ConflictKindCASExhausted))
	default:
		return err
	}
}

func (m *CacheInterceptor) buildCache(ctx context.Context, repo *gitalypb.Repository, op protoregistry.OpType) (*mvcc.LocalCache, error) {
	if !m.isMvccReferenceBackend(ctx, repo) {
		return nil, nil
	}

	if fields := logger.CustomFieldsFromContext(ctx); fields != nil {
		fields.RecordMetadata("mvcc.used", true)
	}

	return m.cacheFactory.NewCache(ctx, repo.GetStorageName(), repo.GetRelativePath(), op == protoregistry.OpAccessor)
}

func (m *CacheInterceptor) isMvccReferenceBackend(ctx context.Context, repo *gitalypb.Repository) bool {
	repoPath, err := m.locator.GetRepoPath(ctx, repo)
	if err != nil {
		return false
	}
	backend, err := gitcmd.DetectReferenceBackend(ctx, repoPath)
	if err != nil {
		return false
	}
	return git.ReferenceBackendMVCC.Name == backend.Name
}

// recognizedNonGitalyMethod returns true if `fullMethod` is a non-Gitaly method
// that the server still accepts.

func recognizedNonGitalyMethod(fullMethod string) bool {
	return strings.Contains(fullMethod, grpchealth.Health_ServiceDesc.ServiceName)
}
