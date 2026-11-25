package repository

import (
	"context"
	"fmt"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v19/internal/git"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage/storagemgr/partition/snapshot"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *server) RepositoryInfo(
	ctx context.Context,
	request *gitalypb.RepositoryInfoRequest,
) (*gitalypb.RepositoryInfoResponse, error) {
	if err := s.locator.ValidateRepository(ctx, request.GetRepository()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(request.GetRepository())

	repoPath, err := repo.Path(ctx)
	if err != nil {
		return nil, err
	}

	filter := snapshot.NewDefaultFilter(ctx)
	repoSize, err := dirSizeInBytes(repoPath, filter)
	if err != nil {
		return nil, fmt.Errorf("calculating repository size: %w", err)
	}

	repoInfo, err := stats.RepositoryInfoForRepository(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("deriving repository info: %w", err)
	}

	// Pool members can't write bitmaps because they lack full object closure, so report the
	// pool's bitmaps, together with the pool's multi-pack-index that the multi-pack-index
	// bitmap belongs to. Sizes and object counts stay the repository's own, because Rails
	// enforces the repository size limit against them.
	if repoInfo.Alternates.Exists && len(repoInfo.Alternates.AbsoluteObjectDirectories()) > 0 {
		poolRepoPath := filepath.Dir(repoInfo.Alternates.AbsoluteObjectDirectories()[0])

		storagePath, err := s.locator.GetStorageByName(ctx, request.GetRepository().GetStorageName())
		if err != nil {
			return nil, fmt.Errorf("getting storage path: %w", err)
		}

		poolRelativePath, err := filepath.Rel(storagePath, poolRepoPath)
		if err != nil {
			return nil, fmt.Errorf("computing pool relative path: %w", err)
		}

		poolRepo := s.localRepoFactory.Build(&gitalypb.Repository{
			StorageName:  request.GetRepository().GetStorageName(),
			RelativePath: poolRelativePath,
		})

		poolPackfiles, err := stats.PackfilesInfoForRepository(ctx, poolRepo)
		if err != nil {
			return nil, fmt.Errorf("deriving pool packfiles info: %w", err)
		}

		repoInfo.Packfiles.Bitmap = poolPackfiles.Bitmap
		repoInfo.Packfiles.MultiPackIndex = poolPackfiles.MultiPackIndex
		repoInfo.Packfiles.MultiPackIndexBitmap = poolPackfiles.MultiPackIndexBitmap
	}

	return convertRepositoryInfo(uint64(repoSize), repoInfo)
}

func convertRepositoryInfo(repoSize uint64, repoInfo stats.RepositoryInfo) (*gitalypb.RepositoryInfoResponse, error) {
	// The loose objects size includes objects which are older than the grace period and thus
	// stale, so we need to subtract the size of stale objects from the overall size.
	recentLooseObjectsSize := repoInfo.LooseObjects.Size - repoInfo.LooseObjects.StaleSize
	// The packfiles size includes the size of cruft packs that contain unreachable objects, so
	// we need to subtract the size of cruft packs from the overall size.
	recentPackfilesSize := repoInfo.Packfiles.Size - repoInfo.Packfiles.CruftSize

	var referenceBackend gitalypb.ReferenceBackend
	switch repoInfo.References.ReferenceBackendName {
	case git.ReferenceBackendReftables.Name:
		referenceBackend = gitalypb.ReferenceBackend_REFERENCE_BACKEND_REFTABLE
	case git.ReferenceBackendFiles.Name:
		referenceBackend = gitalypb.ReferenceBackend_REFERENCE_BACKEND_FILES
	case git.ReferenceBackendMVCC.Name:
		referenceBackend = gitalypb.ReferenceBackend_REFERENCE_BACKEND_MVCC
	default:
		return nil, fmt.Errorf("invalid reference backend")
	}

	response := &gitalypb.RepositoryInfoResponse{
		Size: repoSize,
		References: &gitalypb.RepositoryInfoResponse_ReferencesInfo{
			LooseCount:       repoInfo.References.LooseReferencesCount,
			PackedSize:       repoInfo.References.PackedReferencesSize,
			ReferenceBackend: referenceBackend,
		},
		Objects: &gitalypb.RepositoryInfoResponse_ObjectsInfo{
			Size:                     repoInfo.LooseObjects.Size + repoInfo.Packfiles.Size,
			RecentSize:               recentLooseObjectsSize + recentPackfilesSize,
			StaleSize:                repoInfo.LooseObjects.StaleSize + repoInfo.Packfiles.CruftSize,
			KeepSize:                 repoInfo.Packfiles.KeepSize,
			PackfileCount:            repoInfo.Packfiles.Count,
			ReverseIndexCount:        repoInfo.Packfiles.ReverseIndexCount,
			CruftCount:               repoInfo.Packfiles.CruftCount,
			KeepCount:                repoInfo.Packfiles.KeepCount,
			LooseObjectsCount:        repoInfo.LooseObjects.Count,
			StaleLooseObjectsCount:   repoInfo.LooseObjects.StaleCount,
			LooseObjectsGarbageCount: repoInfo.LooseObjects.GarbageCount,
		},
	}

	// Only set CommitGraph if it exists
	if repoInfo.CommitGraph.Exists {
		response.CommitGraph = &gitalypb.RepositoryInfoResponse_CommitGraphInfo{
			CommitGraphChainLength:    repoInfo.CommitGraph.CommitGraphChainLength,
			HasBloomFilters:           repoInfo.CommitGraph.HasBloomFilters,
			HasGenerationData:         repoInfo.CommitGraph.HasGenerationData,
			HasGenerationDataOverflow: repoInfo.CommitGraph.HasGenerationDataOverflow,
		}
	}

	// Only set Bitmap if it exists
	if repoInfo.Packfiles.Bitmap.Exists {
		response.Bitmap = &gitalypb.RepositoryInfoResponse_BitmapInfo{
			HasHashCache:   repoInfo.Packfiles.Bitmap.HasHashCache,
			HasLookupTable: repoInfo.Packfiles.Bitmap.HasLookupTable,
			Version:        uint64(repoInfo.Packfiles.Bitmap.Version),
		}
	}

	// Only set MultiPackIndex if it exists
	if repoInfo.Packfiles.MultiPackIndex.Exists {
		response.MultiPackIndex = &gitalypb.RepositoryInfoResponse_MultiPackIndexInfo{
			PackfileCount:     repoInfo.Packfiles.MultiPackIndex.PackfileCount,
			Version:           uint64(repoInfo.Packfiles.MultiPackIndex.Version),
			HasLargeOffsets:   repoInfo.Packfiles.MultiPackIndex.HasLargeOffsets,
			HasBitmappedPacks: repoInfo.Packfiles.MultiPackIndex.HasBitmappedPacks,
			HasReverseIndex:   repoInfo.Packfiles.MultiPackIndex.HasReverseIndex,
		}
	}

	// Only set MultiPackIndexBitmap if it exists
	if repoInfo.Packfiles.MultiPackIndexBitmap.Exists {
		response.MultiPackIndexBitmap = &gitalypb.RepositoryInfoResponse_BitmapInfo{
			HasHashCache:   repoInfo.Packfiles.MultiPackIndexBitmap.HasHashCache,
			HasLookupTable: repoInfo.Packfiles.MultiPackIndexBitmap.HasLookupTable,
			Version:        uint64(repoInfo.Packfiles.MultiPackIndexBitmap.Version),
		}
	}

	// Only set Alternates if the file exists, consistent with other fields
	if repoInfo.Alternates.Exists {
		response.Alternates = &gitalypb.RepositoryInfoResponse_AlternatesInfo{
			ObjectDirectories: repoInfo.Alternates.ObjectDirectories,
			LastModified:      &timestamppb.Timestamp{Seconds: repoInfo.Alternates.LastModified.Unix()},
		}
	}

	response.IsObjectPool = repoInfo.IsObjectPool
	if !repoInfo.Packfiles.LastFullRepack.IsZero() {
		response.LastFullRepack = &timestamppb.Timestamp{Seconds: repoInfo.Packfiles.LastFullRepack.Unix()}
	}

	return response, nil
}
